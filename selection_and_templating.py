"""Template and diagnostic histogram production from the stage-1 skim."""

import time
from optparse import OptionParser
from typing import Union
from collections import OrderedDict
import re

import ROOT
from TIMBER import Analyzer
from TIMBER.Analyzer import Correction

from analysis_utils import get_n_weighted, is_data, get_pdf_errtype
import cuts

import correctionlib._core as core
from corrections import corrections_paths

# N_BINS, MIN, MAX
M_JJ_BINS = (66, 700, 4000)
M_JY_BINS = (112, 40, 600)
JET_PT_BINS = (100, 300, 2000)
JET_ETA_BINS = (50, -2.5, 2.5)
JET_PHI_BINS = (32, -3.2, 3.2)
JET_MASS_BINS = (80, 0, 300)
SCORE_BINS = (1000, 0, 1)
WEIGHT_BINS = (50, 0, 2)


def _segmented_bin_edges(segments: list, ndigits: int = 6) -> list:
    """Build variable bin edges from (start, stop, step) segments.

    Each segment covers [start, stop] at its own step; segments must chain
    (each start equal to the previous stop). Edges are rounded to collapse
    floating-point drift from repeated addition (e.g. 19*0.05 ==
    0.9500000000000001, not 0.95) which would otherwise land TAxis::FindBin
    queries for a round number like 0.95 one bin off from the intended edge.
    """
    edges = [round(segments[0][0], ndigits)]
    for start, stop, step in segments:
        n_steps = round((stop - start) / step)
        edges.extend(round(start + (i + 1) * step, ndigits) for i in range(n_steps))
    return edges


# Joint (m_jj, m_jY, Xbb, anti-QCD) histogram booked in book_inclusive_diagnostics,
# used by tagger_studies/ to scan WP + mass-window choices from a single
# already-produced histogram instead of re-running the analyzer per point.
# Variable bin edges: coarse where we don't expect optimal value to live
# (Xbb ~0.99) -- keeps the dense THnD small while still resolving real cuts.
TAGGER_SCAN_M_JJ_EDGES = _segmented_bin_edges([(700, 1400, 100), (1400, 2000, 200), (2000, 4000, 500)])
TAGGER_SCAN_M_JY_EDGES = _segmented_bin_edges([(40, 200, 20), (200, 400, 50), (400, 600, 100)])
TAGGER_SCAN_XBB_EDGES = _segmented_bin_edges([(0.0, 0.95, 0.05), (0.95, 1.0, 0.01)])
TAGGER_SCAN_ANTIQCD_EDGES = _segmented_bin_edges([(0.0, 1.0, 0.05)])
TEMPLATE_VARIATIONS = ("nom", "JES__up", "JES__down", "JER__up", "JER__down")

# Weight-based systematics: (Correction name registered via AddCorrection, template syst name).
# These reweight the nominal-JEC (m_jj, m_jY) selection rather than re-deriving kinematics,
# since they only affect the event weight, not jet momenta.
WEIGHT_SYSTEMATICS = (
    ("PileUp_Corr", "PileUp"),
    ("Pdfweight", "PDF"),
    ("ISRunc", "ISR"),
    ("FSRunc", "FSR"),
    ("QCDscale_uncert", "QCDscale"),
)


def define_common_columns(analyzer: Analyzer, data_flag: bool, year: str) -> None:
    """Define columns used by templates, diagnostics, and region selections."""
    if data_flag:
        analyzer.Define("event_weight", "1.0")
    else:
        analyzer.Define("event_weight", "genWeight")

    analyzer.Define("h_cand_xbb", "FatJet_globalParT3_Xbb[h_cand_idx]/(FatJet_globalParT3_Xbb[h_cand_idx]+FatJet_globalParT3_QCD[h_cand_idx])")
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

def book_systematic_weights(analyzer: Analyzer) -> list:
    """Book histograms for the systematic uncertainty event weights."""
    return [
        analyzer.DataFrame.Histo1D((f"SYST_pileup_weight_up", ";Pileup weight up;Events", *WEIGHT_BINS), "PileUp_Corr__up"),
        analyzer.DataFrame.Histo1D((f"SYST_pileup_weight_down", ";Pileup weight down;Events", *WEIGHT_BINS), "PileUp_Corr__down"),
        analyzer.DataFrame.Histo1D((f"SYST_pdf_weight_up", ";Pileup weight up;Events", *WEIGHT_BINS), "Pdfweight__up"),
        analyzer.DataFrame.Histo1D((f"SYST_pdf_weight_down", ";Pileup weight down;Events", *WEIGHT_BINS), "Pdfweight__down"),
        analyzer.DataFrame.Histo1D((f"SYST_ISR_weight_up", ";ISR weight up;Events", *WEIGHT_BINS), "ISRunc__up"),
        analyzer.DataFrame.Histo1D((f"SYST_ISR_weight_down", ";ISR weight down;Events", *WEIGHT_BINS), "ISRunc__down"),
        analyzer.DataFrame.Histo1D((f"SYST_FSR_weight_up", ";FSR weight up;Events", *WEIGHT_BINS), "FSRunc__up"),
        analyzer.DataFrame.Histo1D((f"SYST_FSR_weight_down", ";FSR weight down;Events", *WEIGHT_BINS), "FSRunc__down"),
        analyzer.DataFrame.Histo1D((f"SYST_QCDscale_weight_up", ";QCD scale weight up;Events", *WEIGHT_BINS), "QCDscale_uncert__up"),
        analyzer.DataFrame.Histo1D((f"SYST_QCDscale_weight_down", ";QCD scale weight down;Events", *WEIGHT_BINS), "QCDscale_uncert__down"),
    ]

def book_diagnostics(analyzer: Analyzer, prefix: str) -> list:
    """Book shared diagnostic histograms with a configurable name prefix."""
    return [
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_pt", ";leading jet p_{T} [GeV];Events", *JET_PT_BINS), "lead_jet_pt", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_pt", ";subleading jet p_{T} [GeV];Events", *JET_PT_BINS), "sublead_jet_pt", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_eta", ";leading jet #eta;Events", *JET_ETA_BINS), "lead_jet_eta", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_eta", ";subleading jet #eta;Events", *JET_ETA_BINS), "sublead_jet_eta", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_phi", ";leading jet #phi;Events", *JET_PHI_BINS), "lead_jet_phi", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_phi", ";subleading jet #phi;Events", *JET_PHI_BINS), "sublead_jet_phi", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_mreg", ";leading jet m_{reg} [GeV];Events", *JET_MASS_BINS), "lead_jet_mreg", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_mreg", ";subleading jet m_{reg} [GeV];Events", *JET_MASS_BINS), "sublead_jet_mreg", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}lead_jet_mass", ";leading jet mass [GeV];Events", *JET_MASS_BINS), "lead_jet_mass", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}sublead_jet_mass", ";subleading jet mass [GeV];Events", *JET_MASS_BINS), "sublead_jet_mass", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}m_jj", ";m_{jj} [GeV];Events", *M_JJ_BINS), "m_jj_nom", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}m_jy", ";m_{jy} [GeV];Events", *M_JY_BINS), "y_cand_msd_nom", "nominal_weight"),
        analyzer.DataFrame.Histo1D((f"{prefix}m_jh", ";m_{jh} [GeV];Events", *JET_MASS_BINS), "h_cand_msd_nom", "nominal_weight"),
    ]

def book_tagger_scan_histogram(analyzer: Analyzer):
    """Book the joint (m_jj, m_jY, Xbb, anti-QCD) histogram for WP/mass-window studies.

    See tagger_studies/thn_utils.py: any WP cut combined with any mass
    window can be read off this single histogram by integrating bin ranges
    offline, so WP scans don't need to re-run the analyzer per point.
    """
    edge_lists = [
        TAGGER_SCAN_M_JJ_EDGES,
        TAGGER_SCAN_M_JY_EDGES,
        TAGGER_SCAN_XBB_EDGES,
        TAGGER_SCAN_ANTIQCD_EDGES,
    ]
    nbins = ROOT.std.vector("int")([len(edges) - 1 for edges in edge_lists])
    xbins = ROOT.std.vector("vector<double>")(
        [ROOT.std.vector("double")(edges) for edges in edge_lists]
    )

    model = ROOT.RDF.THnDModel(
        "inclusive_h_xbb_vs_y_antiqcd",
        ";m_{jj} [GeV];m_{jY} [GeV];H candidate Xbb;Y candidate anti-QCD",
        4, nbins, xbins,
    )
    columns = ROOT.std.vector("string")(
        ["m_jj_nom", "y_cand_msd_nom", "h_cand_xbb", "y_cand_antiqcd", "nominal_weight"]
    )
    return analyzer.DataFrame.HistoND(model, columns)


def book_inclusive_diagnostics(analyzer: Analyzer) -> list:
    """Book nominal diagnostics after the common selection and before region splits."""
    histograms = book_diagnostics(analyzer, "inclusive_")
    histograms.extend(
        [
            analyzer.DataFrame.Histo1D(("inclusive_h_cand_xbb", ";h candidate Xbb;Events", *SCORE_BINS), "h_cand_xbb", "nominal_weight"),
            analyzer.DataFrame.Histo1D(("inclusive_y_cand_antiqcd", ";y candidate anti-QCD;Events", *SCORE_BINS), "y_cand_antiqcd", "nominal_weight"),
            book_tagger_scan_histogram(analyzer),
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
        f"y_cand_msd_{variation}",
        "nominal_weight",
    )


def book_weight_template(analyzer: Analyzer, region_name: str, syst_name: str, direction: str, weight_column: str) -> object:
    """Book a single 2D template histogram for a region and weight-based systematic variation."""
    variation = f"{syst_name}__{direction}"
    return analyzer.DataFrame.Histo2D(
        (f"template_{region_name}_{variation}", ";m_{jj} [GeV];m_{jy} [GeV]", *M_JJ_BINS, *M_JY_BINS),
        "m_jj_nom",
        "y_cand_msd_nom",
        weight_column,
    )


def region_expression(h_region: str, y_region: str, year: str) -> str:
    """Return the region cut expression for the H and Y tag categories."""
    boundaries = cuts.TEMPLATE_REGION_BOUNDARIES[year]
    h_lower, h_upper = boundaries[h_region]
    y_lower, y_upper = boundaries[y_region]
    
    # Build H candidate cut
    h_cut_parts = []
    if h_lower is not None:
        h_cut_parts.append(f"h_cand_xbb > {h_lower}")
    if h_upper is not None:
        h_cut_parts.append(f"h_cand_xbb <= {h_upper}")
    h_cut = " && ".join(h_cut_parts)
    
    # Build Y candidate cut
    y_cut_parts = []
    if y_lower is not None:
        y_cut_parts.append(f"y_cand_antiqcd > {y_lower}")
    if y_upper is not None:
        y_cut_parts.append(f"y_cand_antiqcd <= {y_upper}")
    y_cut = " && ".join(y_cut_parts)
    
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

    ###############################################################################################################
    #                                                   Corrections                                               #
    ###############################################################################################################
    # Pileup (MC only). Applied here, upstream of the JEC-variation branch point, so the resulting
    # "nominal_weight" (event_weight * PileUp_Corr__nom) is available to the nom, JES, and JER
    # templates alike, as well as to the yield/diagnostic histograms below.
    if not data_flag:
        pileup_file = corrections_paths.corrections[year]["Pileup"]
        cset_pileup = core.CorrectionSet.from_file(pileup_file)
        collisions  = list(cset_pileup); assert(len(collisions) == 1); collisions = collisions[0] # Should only ever be one key (collision goldenJSON)
        pu = Correction("PileUp_Corr", "TIMBER/Framework/src/PileUp_correctionlib_weight.cc", [pileup_file, collisions, analyzer.isData], corrtype="weight")
        evalargs = {"Pileup_nTrueInt": "Pileup_nTrueInt"}
        analyzer.AddCorrection(pu, evalargs)
        analyzer.Define("nominal_weight", "event_weight * PileUp_Corr__nom")
    else:
        analyzer.Define("nominal_weight", "event_weight")

    common_no_pt_node = analyzer.GetActiveNode() # Node without pT/mass cuts that are JEC-variation dependent

    #Cuts for diagnostics histograms that are calculated only for nominal JEC variation
    #pT
    apply_h_pt_cut(analyzer, year, "nom", "template_nom")
    post_h_pt = get_n_weighted(analyzer, data_flag, "nominal_weight")
    apply_y_pt_cut(analyzer, year, "nom", "template_nom")
    post_y_pt = get_n_weighted(analyzer, data_flag, "nominal_weight")

    #mass
    apply_h_mass_cut(analyzer, year, "nom", "template_nom")
    post_h_mass = get_n_weighted(analyzer, data_flag, "nominal_weight")
    apply_y_mass_cut(analyzer, year, "nom", "template_nom")
    post_y_mass = get_n_weighted(analyzer, data_flag, "nominal_weight")

    nominal_common_node = analyzer.GetActiveNode()

    if not data_flag:
        # PDF
        errtype = get_pdf_errtype(analyzer.lhaid)
        analyzer.AddCorrection(
            Correction('Pdfweight','./TIMBER_modules/PDFWeight.cc',[errtype],corrtype='uncert')
        )

        # Parton shower weights 
        #	- https://twiki.cern.ch/twiki/bin/viewauth/CMS/TopSystematics#Parton_shower_uncertainties
        #	- "Default" variation: https://twiki.cern.ch/twiki/bin/view/CMS/HowToPDF#Which_set_of_weights_to_use
        #	- https://github.com/mroguljic/Hgamma/blob/409622121e8ab28bc1072c6d8981162baf46aebc/templateMaker.py#L210
        analyzer.Define("ISR__up","PSWeight[2]")
        analyzer.Define("ISR__down","PSWeight[0]")
        analyzer.Define("FSR__up","PSWeight[3]")
        analyzer.Define("FSR__down","PSWeight[1]")
        ISRcorr = Correction("ISRunc", "TIMBER/Framework/TopPhi_modules/BranchCorrection.cc", corrtype='uncert', mainFunc='evalUncert')
        analyzer.AddCorrection(ISRcorr, evalArgs={'valUp':'ISR__up','valDown':'ISR__down'})
        FSRcorr = ISRcorr.Clone("FSRunc")
        analyzer.AddCorrection(FSRcorr, evalArgs={'valUp':'FSR__up','valDown':'FSR__down'})

        # QCD renormalization and factorization scales, following recommendation from B2G GEN contact report
        # See: https://indico.cern.ch/event/938672/contributions/3943718/attachments/2073936/3482265/MC_ContactReport_v3.pdf (slide 27)
        QCDScaleUncert = Correction('QCDscale_uncert', './TIMBER_modules/LHEScaleWeights.cc', corrtype='uncert', mainFunc='evalUncert')
        analyzer.AddCorrection(QCDScaleUncert, evalArgs={'LHEScaleWeights':'LHEScaleWeight'})

        # Have TIMBER automatically create the weight columns based on the registered Corrections.
        # Pass in column "event_weight" == genWeight (if MC, else 1.0) as an extra multiplicative factor to the corrections.
        analyzer.MakeWeightCols(extraNominal='event_weight')
        weighted_nominal_node = analyzer.GetActiveNode()  # nominal_common_node (pT/mass cuts) + weight__* columns

        # Book histograms of the weights
        systematic_weight_histograms = book_systematic_weights(analyzer)

    inclusive_histograms = book_inclusive_diagnostics(analyzer) # Inclusive == before region split
    region_histograms = []
    region_yields = {}

    analyzer.SetActiveNode(nominal_common_node)
    nominal_region_root = analyzer.GetActiveNode()
    for h_region, y_region in cuts.TEMPLATE_REGIONS[year]:
        region_name = f"{h_region[0]}{y_region[0]}"
        analyzer.SetActiveNode(nominal_region_root)
        analyzer.Cut(f"region_{region_name}_nom", region_expression(h_region, y_region, year))
        region_yields[region_name] = get_n_weighted(analyzer, data_flag, "nominal_weight")
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

        for corr_name, syst_name in WEIGHT_SYSTEMATICS:
            for direction in ("up", "down"):
                analyzer.SetActiveNode(weighted_nominal_node)
                weight_column = analyzer.GetWeightName(corr_name, direction)
                for h_region, y_region in cuts.TEMPLATE_REGIONS[year]:
                    region_name = f"{h_region[0]}{y_region[0]}"
                    analyzer.SetActiveNode(weighted_nominal_node)
                    analyzer.Cut(f"region_{region_name}_{syst_name}_{direction}", region_expression(h_region, y_region, year))
                    region_histograms.append(book_weight_template(analyzer, region_name, syst_name, direction, weight_column))

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
    if not data_flag:
        write_histograms(output_file, systematic_weight_histograms)
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
