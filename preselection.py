import ROOT
import time
from optparse import OptionParser
from TIMBER.Tools import Common
from TIMBER import Analyzer
from typing import Union
import cuts
import TIMBER.Tools.AutoJetID as AutoJetID
import TIMBER.Tools.AutoJetVetoMap as AutoJetVetoMap
import TIMBER.Tools.AutoJME_correctionlib as AutoJME
from corrections import corrections_paths


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

def detect_era(input_path: str) -> str:
    """
    Detect the data-taking era from the input file path.

    Args:
        input_path (str): The input file path.
    
    Returns:
        str: Era letter extracted as the third character after "Run20".

    Raises:
        ValueError: If "Run20" is not found or the path is too short to contain an era.
    """
    run20_index = input_path.find("Run20")
    if run20_index == -1:
        print("Warning: could not find matching era for data file ", input_path)
        print("Returning default era 'D'")
        return "D"

    era_index = run20_index + len("Run20") + 2
    if era_index >= len(input_path):
        print("Warning: input path does not contain an era character at the expected position: ", input_path)
        print("Returning default era 'D'")
        return "D"

    return input_path[era_index]

def event_selection(options: OptionParser) -> None:
    """
    Perform event selection

    Args:
        options (OptionParser): Command-line options.
    """
    start_time = time.time()
    a = Analyzer.analyzer(options.input)
    data_flag = is_data(a)
    if data_flag:
        era = detect_era(options.input)
    else:
        era = ""
    n_total = get_n_events(a)

    Common.ApplyMETFilters(a)
    n_met = get_n_events(a)
    year = options.year

    trigger_list = cuts.triggers[year]
    trigger_string = a.GetTriggerString(trigger_list)
    a.Cut("Triggers", trigger_string)
    n_trig = get_n_events(a)

    a.Cut("nFatJet","nFatJet > 0")
    a.Cut("FatJet_pt","FatJet_pt[1] > {}".format(cuts.boosted["pt"]))

    Common.CompileCpp("TIMBER_modules/JetSelection.cc")
    # Regressed mass should be defined before we undo jet corrections and apply new ones because the rawFactor in NanoAOD matches the JECs used in NanoAOD creation. Otherwise, we would need to fetch rawFactor from the JECs we apply which is more complicated
    a.Define("FatJet_regressed_mass", "FatJet_globalParT3_massCorrGeneric * FatJet_mass * (1.0 - FatJet_rawFactor)")
    a.Define("fatjet_indices", "SelectJets(FatJet_pt, FatJet_eta, FatJet_regressed_mass, {}, {}, {})".format(cuts.boosted["pt"], cuts.boosted["eta"], cuts.boosted["m_reg"]))
    a.Cut("nPassingJets","fatjet_indices.size() > 1")
    n_jet = get_n_events(a)

    #import correctionlib
    #correctionlib.register_pyroot_binding() 
    jetid_file = corrections_paths.corrections[year]["JetID"]
    jetveto_file = corrections_paths.corrections[year]["JetVetoMap"]
    AutoJetID.AutoJetID(a, correction_file = jetid_file, jet_types=["Jet","FatJet"])
    AutoJME.AutoJME(a, ["Jet", "FatJet"], jec_paths = [corrections_paths.corrections[year]["JEC_AK4"],corrections_paths.corrections[year]["JEC_AK8"]],dataEra=era, verbose = False)
    AutoJetVetoMap.AutoJetVetoMap(a, campaign = "", map_path = jetveto_file, pt_branch = "Jet_pt_nom", id_branch = "Jet_jetId") 

    #AutoJetID.AutoJetID(a, correction_file = "jetidtest.json.gz")

    a.Define("h_cand_idx", "FatJet_globalParT3_Xbb[fatjet_indices[0]] > FatJet_globalParT3_Xbb[fatjet_indices[1]] ? fatjet_indices[0] : fatjet_indices[1]")
    a.Define("y_cand_idx", "fatjet_indices[0] == h_cand_idx ? fatjet_indices[1] : fatjet_indices[0]")
    a.Define("FatJet_mass_h", "FatJet_mass_nom[h_cand_idx]")
    a.Define("FatJet_mass_y", "FatJet_mass_nom[y_cand_idx]")
    a.Define("FatJet_pt_h", "FatJet_pt_nom[h_cand_idx]")
    a.Define("FatJet_pt_y", "FatJet_pt_nom[y_cand_idx]")
    a.Define("regressed_mass_h", "FatJet_regressed_mass[h_cand_idx]")
    a.Define("regressed_mass_y", "FatJet_regressed_mass[y_cand_idx]")

    a.Define("h_cand_vec", "hardware::TLvector(FatJet_pt_nom[h_cand_idx], FatJet_eta[h_cand_idx], FatJet_phi[h_cand_idx], FatJet_mass_nom[h_cand_idx])") # Check with contacts if this needs to be done with regressed or kinematic mass
    a.Define("y_cand_vec", "hardware::TLvector(FatJet_pt_nom[y_cand_idx], FatJet_eta[y_cand_idx], FatJet_phi[y_cand_idx], FatJet_mass_nom[y_cand_idx])")
    a.Define("m_jj", "hardware::InvariantMass({h_cand_vec, y_cand_vec})")
    a.Cut("m_jj", "m_jj > {}".format(cuts.boosted["m_jj"]))
    n_mjj = get_n_events(a)

    snapshot_columns = [
        "run",
        "luminosityBlock",
        "event",
        "nFatJet",
        "FatJet_pt",
        "FatJet_pt_nom",
        "FatJet_pt_jes_up",
        "FatJet_pt_jes_down",
        "FatJet_pt_jer_up",
        "FatJet_pt_jer_down",
        "FatJet_eta",
        "FatJet_mass",
        "FatJet_mass_nom",
        "FatJet_mass_jes_up",
        "FatJet_mass_jes_down",
        "FatJet_mass_jer_up",
        "FatJet_mass_jer_down",
        "FatJet_jetId",
        "fatjet_indices",
        "h_cand_idx",
        "y_cand_idx",
        "m_jj",
    ]

    # print("Debug preview before Snapshot (first 5 rows):")
    # debug_columns = ["event", "nFatJet", "FatJet_jetId"]
    # try:
    #     a.DataFrame.Display(debug_columns, 5).Print()
    # except Exception as error:
    #     print(f"[FAIL] Display({debug_columns}, 5): {error}")
    #     raise

    a.Snapshot(snapshot_columns, options.output, "Events", lazy=False, openOption="RECREATE", saveRunChain=True)

    histos = []

    h_mass_H = a.DataFrame.Histo1D(("h_mass_H", ";H-candidate mass [GeV]; Events", 60, 0, 600), "FatJet_mass_h")
    h_mass_Y = a.DataFrame.Histo1D(("h_mass_Y", ";Y-candidate mass [GeV]; Events", 60, 0, 600), "FatJet_mass_y")
    h_mjj = a.DataFrame.Histo1D(("h_mjj", ";m_{jj} [GeV]; Events", 60, 0, 3000), "m_jj")
    h_pt_H = a.DataFrame.Histo1D(("h_pt_H", ";Higgs p_{T} [GeV]; Events", 100, 0, 1000), "FatJet_pt_h")
    h_pt_Y = a.DataFrame.Histo1D(("h_pt_Y", ";Y p_{T} [GeV]; Events", 100, 0, 1000), "FatJet_pt_y")
    h_regressed_mass_H = a.DataFrame.Histo1D(("h_regressed_mass_H", ";H-candidate regressed mass [GeV]; Events", 60, 0, 600), "regressed_mass_h")
    h_regressed_mass_Y = a.DataFrame.Histo1D(("h_regressed_mass_Y", ";Y-candidate regressed mass [GeV]; Events", 60, 0, 600), "regressed_mass_y")
    
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

    histos.extend([h_mass_H, h_mass_Y, h_mjj, h_pt_H, h_pt_Y, h_regressed_mass_H, h_regressed_mass_Y, h_cutflow])

    output_file = ROOT.TFile(options.output, "UPDATE")
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