"""Template and diagnostic histogram production from the stage-1 skim."""

import time
from optparse import OptionParser
from typing import Union

import ROOT
from TIMBER import Analyzer

from analysis_utils import get_n_weighted, is_data
import cuts

# N_BINS, MIN, MAX
M_JJ_BINS = (66, 700, 4000)
M_JY_BINS = (112, 40, 600)
JET_PT_BINS = (100, 300, 2000)
JET_ETA_BINS = (50, -2.5, 2.5)
JET_PHI_BINS = (32, -3.2, 3.2)
JET_MASS_BINS = (80, 0, 300)
SCORE_BINS = (50, 0, 1)
TEMPLATE_VARIATIONS = ("nom", "JES__up", "JES__down", "JER__up", "JER__down")


def define_common_columns(analyzer: Analyzer, data_flag: bool, year: str) -> None:
    """Define columns used by templates, diagnostics, and region selections."""
    if data_flag:
        analyzer.Define("event_weight", "1.0")
    else:
        analyzer.Define("event_weight", "genWeight")

    analyzer.Define("h_cand_xbb", "FatJet_globalParT3_Xbb[h_cand_idx]")
    analyzer.Define("y_cand_antiqcd", "1.0 - FatJet_globalParT3_QCD[y_cand_idx]")
    analyzer.Define("trigger_pass", analyzer.GetTriggerString(cuts.triggers[year]))
    analyzer.Define("lead_jet_pt", "FatJet_pt[0]")
    analyzer.Define("sublead_jet_pt", "FatJet_pt[1]")
    analyzer.Define("lead_jet_eta", "FatJet_eta[0]")
    analyzer.Define("sublead_jet_eta", "FatJet_eta[1]")
    analyzer.Define("lead_jet_phi", "FatJet_phi[0]")
    analyzer.Define("sublead_jet_phi", "FatJet_phi[1]")
    analyzer.Define("lead_jet_mreg", "FatJet_regressed_mass[0]")
    analyzer.Define("sublead_jet_mreg", "FatJet_regressed_mass[1]")
    analyzer.Define("lead_jet_mass", "FatJet_mass[0]")
    analyzer.Define("sublead_jet_mass", "FatJet_mass[1]")

def apply_y_mass_cut(analyzer: Analyzer, year: str, jec_variation: str, cut_name_prefix: str) -> None:
    """Apply the Y-candidate mass cut."""
    selection = cuts.TEMPLATE_SELECTION[year]
    analyzer.Cut(
        f"{cut_name_prefix}_y_mass_cut",
        f"y_cand_msd_{jec_variation} >= {selection['y_cand_mass_min']}",
    )

def apply_h_mass_cut(analyzer: Analyzer, year: str, jec_variation: str, cut_name_prefix: str) -> None:
    """Apply the H-candidate mass cut."""
    selection = cuts.TEMPLATE_SELECTION[year]
    analyzer.Cut(
        f"{cut_name_prefix}_h_mass_cut_{jec_variation}",
        f"h_cand_msd_{jec_variation} >= {selection['h_cand_mass_min']} && h_cand_msd_{jec_variation} <= {selection['h_cand_mass_max']}",
    )

def apply_selection_for_variation(analyzer: Analyzer, year: str, jec_variation: str, cut_name_prefix: str) -> None:
    """Apply pT cuts for a specific JEC variation."""
    apply_h_pt_cut(analyzer, year, jec_variation, cut_name_prefix)
    apply_y_pt_cut(analyzer, year, jec_variation, cut_name_prefix)
    apply_h_mass_cut(analyzer, year, jec_variation, cut_name_prefix)
    apply_y_mass_cut(analyzer, year, jec_variation, cut_name_prefix)


def apply_h_pt_cut(analyzer: Analyzer, year: str, jec_variation: str, cut_name_prefix: str) -> None:
    """Apply the H-candidate pT cut for a specific JEC variation."""
    selection = cuts.TEMPLATE_SELECTION[year]
    analyzer.Cut(f"{cut_name_prefix}_h_pt_cut_{jec_variation}", f"h_cand_pt_{jec_variation} > {selection['h_cand_pt_min']}")


def apply_y_pt_cut(analyzer: Analyzer, year: str, jec_variation: str, cut_name_prefix: str) -> None:
    """Apply the Y-candidate pT cut for a specific JEC variation."""
    selection = cuts.TEMPLATE_SELECTION[year]
    analyzer.Cut(f"{cut_name_prefix}_y_pt_cut_{jec_variation}", f"y_cand_pt_{jec_variation} > {selection['y_cand_pt_min']}")


def load_selection_cutflow_histogram(input_file_path: str, data_flag: bool) -> ROOT.TH1:
    """Load and sum the stage-1 cutflow histogram across one or more input files.

    If input_file_path is a .txt filelist, each line is treated as a separate
    ROOT file and the cutflow histograms are summed. Otherwise a single ROOT
    file is opened directly.
    """
    histogram_name = "h_cutflow" if data_flag else "h_cutflow_weighted"

    if input_file_path.endswith(".txt"):
        with open(input_file_path) as f:
            root_files = [line.strip() for line in f if line.strip()]
    else:
        root_files = [input_file_path]

    total_histogram = None
    for path in root_files:
        input_file = ROOT.TFile.Open(path, "READ")
        if not input_file or input_file.IsZombie():
            raise OSError(f"Could not open input file '{path}' to read the cutflow histogram")
        histogram = input_file.Get(histogram_name)
        if histogram is None:
            input_file.Close()
            raise KeyError(f"Histogram '{histogram_name}' was not found in '{path}'")
        histogram_copy = histogram.Clone()
        histogram_copy.SetDirectory(0)
        input_file.Close()
        if total_histogram is None:
            total_histogram = histogram_copy
        else:
            total_histogram.Add(histogram_copy)

    return total_histogram


def make_extended_cutflow_histogram(source_histogram: ROOT.TH1, extra_bins: list[tuple[str, Union[int, float]]]) -> ROOT.TH1F:
    """Extend the stage-1 cutflow histogram with stage-2 yields and region counts."""
    total_bins = source_histogram.GetNbinsX() + len(extra_bins)
    histogram = ROOT.TH1F("template_cutflow", "Template cutflow; Cut; Weighted events", total_bins, 0.5, total_bins + 0.5)

    for bin_index in range(1, source_histogram.GetNbinsX() + 1):
        histogram.GetXaxis().SetBinLabel(bin_index, source_histogram.GetXaxis().GetBinLabel(bin_index))
        histogram.SetBinContent(bin_index, source_histogram.GetBinContent(bin_index))
        histogram.SetBinError(bin_index, source_histogram.GetBinError(bin_index))

    for offset, (label, value) in enumerate(extra_bins, start=1):
        target_bin = source_histogram.GetNbinsX() + offset
        histogram.GetXaxis().SetBinLabel(target_bin, label)
        histogram.SetBinContent(target_bin, value)

    return histogram


def book_diagnostics(analyzer: Analyzer, prefix: str) -> list:
    """Book shared diagnostic histograms with a configurable name prefix."""
    return [
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_pt", ";leading jet p_{T} [GeV];Events", *JET_PT_BINS), "lead_jet_pt", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_pt", ";subleading jet p_{T} [GeV];Events", *JET_PT_BINS), "sublead_jet_pt", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_eta", ";leading jet #eta;Events", *JET_ETA_BINS), "lead_jet_eta", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_eta", ";subleading jet #eta;Events", *JET_ETA_BINS), "sublead_jet_eta", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_phi", ";leading jet #phi;Events", *JET_PHI_BINS), "lead_jet_phi", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_phi", ";subleading jet #phi;Events", *JET_PHI_BINS), "sublead_jet_phi", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_mreg", ";leading jet m_{reg} [GeV];Events", *JET_MASS_BINS), "lead_jet_mreg", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_mreg", ";subleading jet m_{reg} [GeV];Events", *JET_MASS_BINS), "sublead_jet_mreg", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_mass", ";leading jet mass [GeV];Events", *JET_MASS_BINS), "lead_jet_mass", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_mass", ";subleading jet mass [GeV];Events", *JET_MASS_BINS), "sublead_jet_mass", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}m_jj", ";m_{jj} [GeV];Events", *M_JJ_BINS), "m_jj_nom", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}m_jy", ";m_{jy} [GeV];Events", *M_JY_BINS), "y_cand_reg_mass", "event_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}m_jh", ";m_{jh} [GeV];Events", *JET_MASS_BINS), "h_cand_reg_mass", "event_weight"),
    ]


def book_inclusive_diagnostics(analyzer: Analyzer) -> list:
    """Book nominal diagnostics after the common selection and before region splits."""
    histograms = book_diagnostics(analyzer, "inclusive_")
    histograms.extend(
        [
            analyzer.DataFrame.Histo1D(("inclusive_h_cand_xbb", ";h candidate Xbb;Events", *SCORE_BINS), "h_cand_xbb", "event_weight"),
            analyzer.DataFrame.Histo1D(("inclusive_y_cand_antiqcd", ";y candidate anti-QCD;Events", *SCORE_BINS), "y_cand_antiqcd", "event_weight"),
        ]
    )
    return histograms


def book_region_diagnostics(analyzer: Analyzer, region_name: str) -> list:
    """Book nominal diagnostics for a specific region."""
    return book_diagnostics(analyzer, f"{region_name}_")


def book_template(analyzer: Analyzer, region_name: str, variation: str) -> object:
    """Book a single 2D template histogram for a region and variation."""
    return analyzer.DataFrame.Histo2D(
        (f"template_{region_name}_{variation}", ";m_{jj} [GeV];m_{jy} [GeV]", *M_JJ_BINS, *M_JY_BINS),
        f"m_jj_{variation}",
        "y_cand_reg_mass",
        "event_weight",
    )


def region_expression(h_region: str, y_region: str, year: str) -> str:
    """Return the region cut expression for the H and Y tag categories."""
    working_points = cuts.TEMPLATE_TAGGING_WPS[year]
    h_cut = f"h_cand_xbb > {working_points['h_xbb_wp']}" if h_region == "Pass" else f"h_cand_xbb <= {working_points['h_xbb_wp']}"
    y_cut = f"y_cand_antiqcd > {working_points['y_antiqcd_wp']}" if y_region == "Signal" else f"y_cand_antiqcd <= {working_points['y_antiqcd_wp']}"
    return f"({h_cut}) && ({y_cut})"


def write_histograms(output_file: ROOT.TFile, histograms: list) -> None:
    """Write booked ROOT RDataFrame histograms to the output file."""
    output_file.cd()
    for histogram in histograms:
        histogram.GetValue().Write()


def fill_templates_and_diagnostics(input_file_path: str, output_file_path: str, year: str) -> None:
    """Read the preselection skim and write templates plus diagnostics."""
    start_time = time.time()
    analyzer = Analyzer.analyzer(input_file_path)
    data_flag = is_data(analyzer)
    selection_cutflow = load_selection_cutflow_histogram(input_file_path, data_flag)
    print(f"Running on {'data' if data_flag else 'MC'}")

    define_common_columns(analyzer, data_flag, year)
    selection_root = analyzer.GetActiveNode()

    analyzer.SetActiveNode(selection_root)
    analyzer.Cut("template_cutflow_trigger", "trigger_pass")
    post_trigger = get_n_weighted(analyzer, data_flag, "event_weight")

    common_no_pt_node = analyzer.GetActiveNode() # Node without pT/mass cuts that are JEC-variation dependent

    #Cuts for diagnostics histograms that are calculated only for nominal JEC variation
    #pT
    apply_h_pt_cut(analyzer, year, "nom", "template_nom")
    post_h_pt = get_n_weighted(analyzer, data_flag, "event_weight")
    apply_y_pt_cut(analyzer, year, "nom", "template_nom")
    post_y_pt = get_n_weighted(analyzer, data_flag, "event_weight")

    #mass
    apply_h_mass_cut(analyzer, year, "nom", "template_nom")
    post_h_mass = get_n_weighted(analyzer, data_flag, "event_weight")
    apply_y_mass_cut(analyzer, year, "nom", "template_nom")
    post_y_mass = get_n_weighted(analyzer, data_flag, "event_weight")

    nominal_common_node = analyzer.GetActiveNode()

    inclusive_histograms = book_inclusive_diagnostics(analyzer) # Inclusive == before region split
    region_histograms = []
    region_yields = {}

    analyzer.SetActiveNode(nominal_common_node)
    nominal_region_root = analyzer.GetActiveNode()
    for h_region, y_region in cuts.TEMPLATE_REGIONS[year]:
        region_name = f"{h_region[0]}{y_region[0]}"
        analyzer.SetActiveNode(nominal_region_root)
        analyzer.Cut(f"region_{region_name}_nom", region_expression(h_region, y_region, year))
        region_yields[region_name] = get_n_weighted(analyzer, data_flag, "event_weight")
        region_histograms.append(book_template(analyzer, region_name, "nom"))
        region_histograms.extend(book_region_diagnostics(analyzer, region_name))

    if not data_flag:
        for variation in TEMPLATE_VARIATIONS[1:]:
            analyzer.SetActiveNode(common_no_pt_node)
            apply_selection_for_variation(analyzer, year, variation, variation.lower().replace("__", "_"))
            varied_region_root = analyzer.GetActiveNode()
            for h_region, y_region in cuts.TEMPLATE_REGIONS[year]:
                region_name = f"{h_region[0]}{y_region[0]}"
                analyzer.SetActiveNode(varied_region_root)
                analyzer.Cut(f"region_{region_name}_{variation}", region_expression(h_region, y_region, year))
                region_histograms.append(book_template(analyzer, region_name, variation))

    extra_cutflow_bins = [
        ("Post trigger", post_trigger),
        ("Post H pT", post_h_pt),
        ("Post Y pT", post_y_pt),
        ("Post H mass", post_h_mass),
        ("Post Y mass", post_y_mass),
    ]
    extra_cutflow_bins.extend((f"Region {region_name}", region_yields[region_name]) for region_name in region_yields)
    template_cutflow = make_extended_cutflow_histogram(selection_cutflow, extra_cutflow_bins)

    output_file = ROOT.TFile(output_file_path, "RECREATE")
    write_histograms(output_file, inclusive_histograms)
    write_histograms(output_file, region_histograms)
    template_cutflow.Write()
    output_file.Close()

    print(f"Saved template histograms to {output_file_path}")
    print(f"Total time: {(time.time() - start_time) / 60.0:.2f} min")
    print(f"Region yields: {region_yields}")


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option(
        '-i', '--input',
        metavar='IFILE',
        type='string',
        action='store',
        default='',
        dest='input',
        help='Stage-1 skim ROOT file or .txt with list of ROOT files to process'
    )
    parser.add_option(
        '-o', '--output',
        metavar='OFILE',
        type='string',
        action='store',
        default='templates.root',
        dest='output',
        help='Output file name for template histograms'
    )
    parser.add_option(
        '-y', '--year',
        metavar='YEAR',
        type='string',
        action='store',
        default='2024',
        dest='year',
        help='Data-taking year'
    )

    (options, args) = parser.parse_args()

    if not options.input:
        parser.print_help()
        raise ValueError("Input file (-i/--input) is required")

    fill_templates_and_diagnostics(options.input, options.output, options.year)
