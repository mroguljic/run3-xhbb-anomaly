import ROOT
import time
from optparse import OptionParser
from TIMBER.Tools import Common
from TIMBER import Analyzer
from typing import Union
import cuts

def get_n_weighted(analyzer: Analyzer, is_data: bool) -> Union[int, float]:
    """
    Get the weighted number of events.

    Args:
        analyzer (Analyzer): The Analyzer instance.
        is_data (bool): Whether the input is data or MC.

    Returns:
        Union[int, float]: The weighted number of events.
    """
    if not is_data:
        n_weighted = analyzer.DataFrame.Sum("genWeight").GetValue()
    else:
        n_weighted = analyzer.DataFrame.Count().GetValue()
    return n_weighted


def get_n_events(analyzer: Analyzer) -> int:
    """
    Get the total number of events.

    Args:
        analyzer (Analyzer): The Analyzer instance.

    Returns:
        int: The total number of events.
    """
    n_events = analyzer.DataFrame.Count().GetValue()
    return n_events


def is_data(analyzer: Analyzer) -> bool:
    """
    Determine if the input is data or MC based on the run number.

    Args:
        analyzer (Analyzer): The Analyzer instance.

    Returns:
        bool: True if the input is data, False otherwise.
    """
    run_number = analyzer.DataFrame.Range(1).AsNumpy(["run"])  # Check run number to see if data
    if (run_number["run"][0] > 10000):
        is_data = True
        analyzer.Define("genWeight", "1")
        print("Running on data")
    else:
        is_data = False
        print("Running on MC")
    return is_data


def event_selection(options: OptionParser) -> None:
    """
    Perform event selection

    Args:
        options (OptionParser): Command-line options.
    """
    start_time = time.time()
    a = Analyzer.analyzer(options.input)
    data_flag = is_data(a)  # Renamed local variable to avoid conflict
    n_total = get_n_events(a)

    Common.ApplyMETFilters(a)
    n_met = get_n_events(a)

    trigger_list = cuts.triggers[options.year]
    trigger_string = a.GetTriggerString(trigger_list)
    a.Cut("Triggers", trigger_string)
    n_trig = get_n_events(a)

    a.Cut("nFatJet","nFatJet > 0")
    a.Cut("FatJet_pt","FatJet_pt[1] > {}".format(cuts.boosted["pt"]))

    Common.CompileCpp("TIMBER_modules/JetSelection.cc")
    a.Define("fatjet_indices", "SelectJets(FatJet_pt, FatJet_eta, FatJet_msoftdrop, {}, {}, {})".format(cuts.boosted["pt"], cuts.boosted["eta"], cuts.boosted["m_sd"]))
    a.Cut("nPassingJets","fatjet_indices.size() > 1")
    n_jet = get_n_events(a)

    # Ensure h_cand_idx is defined before using it
    a.Define("h_cand_idx", "FatJet_globalParT3_Xbb[fatjet_indices[0]] > FatJet_globalParT3_Xbb[fatjet_indices[1]] ? fatjet_indices[0] : fatjet_indices[1]")
    a.Define("y_cand_idx", "fatjet_indices[0] == h_cand_idx ? fatjet_indices[1] : fatjet_indices[0]")

    a.Define("FatJet_msoftdrop_h", "FatJet_msoftdrop[h_cand_idx]")
    a.Define("FatJet_msoftdrop_y", "FatJet_msoftdrop[y_cand_idx]")

    a.Define("FatJet_pt_h", "FatJet_pt[h_cand_idx]")
    a.Define("FatJet_pt_y", "FatJet_pt[y_cand_idx]")

    a.Define("h_cand_vec", "hardware::TLvector(FatJet_pt[h_cand_idx], FatJet_eta[h_cand_idx], FatJet_phi[h_cand_idx], FatJet_msoftdrop[h_cand_idx])")
    a.Define("y_cand_vec", "hardware::TLvector(FatJet_pt[y_cand_idx], FatJet_eta[y_cand_idx], FatJet_phi[y_cand_idx], FatJet_msoftdrop[y_cand_idx])")
    a.Define("m_jj", "hardware::InvariantMass({h_cand_vec, y_cand_vec})")
    a.Cut("m_jj", "m_jj > {}".format(cuts.boosted["m_jj"]))
    n_mjj = get_n_events(a)

    histos = []

    h_msoftdrop_H = a.DataFrame.Histo1D(("h_msoftdrop_H", ";H-candidate softdrop mass [GeV]; Events", 60, 0, 600), "FatJet_msoftdrop_h")
    h_msoftdrop_Y = a.DataFrame.Histo1D(("h_msoftdrop_Y", ";Y-candidate softdrop mass [GeV]; Events", 60, 0, 600), "FatJet_msoftdrop_y")
    h_mjj = a.DataFrame.Histo1D(("h_mjj", ";m_{jj} [GeV]; Events", 60, 0, 3000), "m_jj")
    h_pt_H = a.DataFrame.Histo1D(("h_pt_H", ";Higgs p_{T} [GeV]; Events", 100, 0, 1000), "FatJet_pt_h")
    h_pt_Y = a.DataFrame.Histo1D(("h_pt_Y", ";Y p_{T} [GeV]; Events", 100, 0, 1000), "FatJet_pt_y")
    # Cutflow histogram
    cutflow_labels = ["Total", "MET Filters", "Triggers", "Jet selection", "Invariant mass"]
    h_cutflow = ROOT.TH1F("h_cutflow", "Cutflow; Cut; Events", len(cutflow_labels), 0.5, len(cutflow_labels) + 0.5)
    for i, label in enumerate(cutflow_labels):
        h_cutflow.GetXaxis().SetBinLabel(i + 1, label)
    h_cutflow.SetBinContent(1, n_total)
    h_cutflow.SetBinContent(2, n_met)
    h_cutflow.SetBinContent(3, n_trig)
    h_cutflow.SetBinContent(4, n_jet)
    h_cutflow.SetBinContent(5, n_mjj)

    histos.extend([h_msoftdrop_H, h_msoftdrop_Y, h_mjj, h_pt_H, h_pt_Y, h_cutflow])

    output_file = ROOT.TFile(options.output, "RECREATE")
    output_file.cd()
    for hist in histos:
        hist.Write()
    output_file.Close()
    print(f"Saved histograms to {options.output}")
    print("Total time: {:.2f} min".format((time.time() - start_time) / 60.))


parser = OptionParser()

parser.add_option('-i', '--input', metavar='IFILE', type='string', action='store',
                default='',
                dest='input',
                help='A root file or text file with multiple root file locations to analyze')
parser.add_option('-y', '--year', metavar='YEAR', type='string', action='store', default='2024')
parser.add_option('-o', '--output', metavar='OFILE', type='string', action='store', default='output.root')
(options, args) = parser.parse_args()

event_selection(options)