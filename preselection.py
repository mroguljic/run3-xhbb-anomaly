
import ROOT
import time
from optparse import OptionParser
from TIMBER.Tools import Common
from TIMBER import Analyzer
import cuts
import TIMBER.Tools.AutoJetID as AutoJetID
import TIMBER.Tools.AutoJetVetoMap as AutoJetVetoMap
import TIMBER.Tools.AutoJME_correctionlib as AutoJME
from analysis_utils import get_n_events, get_n_weighted, is_data
from corrections import corrections_paths
from preselection_branches import get_preselection_snapshot_columns
from typing import Union


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
        print("Warning: could not find matching era for data file", input_path)
        print("Returning default era 'D'")
        return "D"

    era_index = run20_index + len("Run20") + 2
    if era_index >= len(input_path):
        print("Warning: input path does not contain an era character at the expected position:", input_path)
        print("Returning default era 'D'")
        return "D"

    return input_path[era_index]


def apply_mc_year_split(analyzer: Analyzer, year: str, data_flag: bool) -> None:
    """
    Split the shared Summer24 MC between the analysis years by event number.

    2024 and 2025 use the same MC sample, so each year takes a disjoint slice of it
    (cuts.MC_YEAR_SPLIT) to keep the per-year MC statistics independent.

    Must be applied before the cutflow counting starts, so that bin 1 of
    h_cutflow_weighted is the sum of genWeight over *this year's slice only*. That
    bin is what condor/merge_templates.py divides by when applying the
    xsec * int_lumi / sum(genWeight) normalisation, so the split propagates into the
    scaling automatically with no further bookkeeping.

    Args:
        analyzer (Analyzer): The Analyzer instance.
        year (str): Data-taking year (e.g. "2024").
        data_flag (bool): Whether the input is data.
    """
    if data_flag:
        return

    if year not in cuts.MC_YEAR_SPLIT:
        raise ValueError(
            f"No MC year split configured for '{year}'. Available years: {sorted(cuts.MC_YEAR_SPLIT)}"
        )

    lo, hi = cuts.MC_YEAR_SPLIT[year]
    modulus = cuts.MC_YEAR_SPLIT_MODULUS
    print(f"Applying {year} MC year split: (event % {modulus}) in [{lo}, {hi})")
    analyzer.Cut(
        "mc_year_split",
        f"(event % {modulus}) >= {lo} && (event % {modulus}) < {hi}",
    )


def apply_data_lumi_mask(analyzer: Analyzer, year: str, data_flag: bool) -> None:
    """
    Apply the golden JSON lumi mask for data using TIMBER's LumiFilter.

    Compiles LumiFilter.cc (which is part of libtimber), instantiates a global
    LumiFilter for the given year, and filters events via run/luminosityBlock.
    This is a no-op for MC.

    Args:
        analyzer (Analyzer): The Analyzer instance.
        year (str): Data-taking year (e.g. "2024"). LumiFilter resolves the
            bundled golden JSON automatically.
        data_flag (bool): Whether the input is data.
    """
    if not data_flag:
        return
    ROOT.gInterpreter.Declare(f"LumiFilter lumi_filter({int(year)});") # No need to Compile the module since it is part of libtimber.so
    analyzer.Define("lumi_pass", "lumi_filter.eval(run, luminosityBlock)")
    analyzer.Cut("lumi_mask_cut", "lumi_pass==1")
    print(f"Applied lumi mask for year {year}")


def build_cutflow_histogram(labels: list[str], counts: list[Union[int, float]], histogram_name: str, title: str) -> ROOT.TH1F:
    """Build and fill a cutflow histogram with labeled bins."""
    histogram = ROOT.TH1F(histogram_name, title, len(labels), 0.5, len(labels) + 0.5)
    for bin_index, label in enumerate(labels, start=1):
        histogram.GetXaxis().SetBinLabel(bin_index, label)
        histogram.SetBinContent(bin_index, counts[bin_index - 1])
    return histogram


def define_lund_gen_parts(analyzer: Analyzer) -> None:
    """Define the hard-process decay tree that seeds the Lund plane prongs (MC only).

    Selection: outgoing hard-process particles (includes quarks), plus the last copies of 
    hard-process pdgId>5 (no quarks except tops). Decay products (quarks) point 
    at the last copy of their parent, which is not always flagged with isHardProcess, 
    The purpose of second condition is to keep that last copy (mother of quarks)

    Args:
        analyzer (Analyzer): TIMBER analyzer, MC only (crashes on data, no GenPart).
    """
    analyzer.Define(
        "sel_gen_mask",
        "((GenPart_statusFlags & (1 << 7)) != 0 && GenPart_status != 21)" # isHardProcess minus beam OR
        " || ((GenPart_statusFlags & (1 << 8)) != 0 && (GenPart_statusFlags & (1 << 13)) != 0" 
        " && abs(GenPart_pdgId) > 5 && GenPart_pdgId != 21)", # Last copy of **fromHardProcess** with pdgId>5
    )
    analyzer.Define(
        "lund_gen_idx",
        "ROOT::VecOps::RVec<int> indices;"
        " for (size_t i = 0; i < sel_gen_mask.size(); ++i) if (sel_gen_mask[i]) indices.push_back((int)i);"
        " return indices;",
    )
    analyzer.Define(
        "lund_gen_mothIdx",
        "ROOT::VecOps::RVec<int> mothers;"
        " for (auto mother_idx : GenPart_genPartIdxMother[sel_gen_mask]) mothers.push_back((int)mother_idx);"
        " return mothers;",
    )
    # Never found mothIdx=-1 in testing, but still guarding against it
    analyzer.Define(
        "lund_gen_mothPdgId",
        "ROOT::VecOps::RVec<int> mother_ids;"
        " for (auto mother_idx : lund_gen_mothIdx)"
        " mother_ids.push_back(mother_idx >= 0 ? GenPart_pdgId[mother_idx] : 0);"
        " return mother_ids;",
    )
    for gen_variable in ["pt", "eta", "phi", "pdgId"]:
        analyzer.Define(f"lund_gen_{gen_variable}", f"GenPart_{gen_variable}[sel_gen_mask]")


def event_preselection(options: OptionParser) -> None:
    """
    Perform event selection

    Args:
        options (OptionParser): Command-line options.
    """
    start_time = time.time()
    analyzer = Analyzer.analyzer(options.input)
    data_flag = is_data(analyzer)
    if data_flag:
        print("Running on data")
    else:
        print("Running on MC")
    year = options.year
    era = detect_era(options.input) if data_flag else ""
    preselection_cuts = cuts.PRESELECTION_CUTS[year]
    print("Compiling JetSelection.cc")
    Common.CompileCpp("TIMBER_modules/JetSelection.cc")  # also loads libtimber.so

    apply_mc_year_split(analyzer, year, data_flag)

    n_total = get_n_events(analyzer)
    n_total_weighted = get_n_weighted(analyzer, data_flag)

    apply_data_lumi_mask(analyzer, year, data_flag)
    n_lumi = get_n_events(analyzer)
    n_lumi_weighted = get_n_weighted(analyzer, data_flag)

    Common.ApplyMETFilters(analyzer)
    n_met = get_n_events(analyzer)
    n_met_weighted = get_n_weighted(analyzer, data_flag)

    analyzer.Cut("n_FatJet_cut","nFatJet > 1") #Avoids crashing on events with <2 fatjets
    n_one_fatjet = get_n_events(analyzer)
    n_one_fatjet_weighted = get_n_weighted(analyzer, data_flag)

    analyzer.Define("FatJet_regressed_mass", "FatJet_globalParT3_massCorrGeneric * FatJet_mass * (1.0 - FatJet_rawFactor)")

    jetid_file = corrections_paths.get_correction_path(year, "JetID")
    jetveto_file = corrections_paths.get_correction_path(year, "JetVetoMap")
    AutoJetID.AutoJetID(analyzer, correction_file=jetid_file, jet_types=["Jet", "FatJet"])
    AutoJME.AutoJME(analyzer, ["Jet", "FatJet"], jec_paths=[corrections_paths.get_correction_path(year, "JEC_AK4"), corrections_paths.get_correction_path(year, "JEC_AK8")], dataEra=era, verbose=False)
    AutoJME.AutoJME_mSD(analyzer, jec_path=corrections_paths.get_correction_path(year, "JEC_AK4"), dataEra=era, verbose=False)
    AutoJetVetoMap.AutoJetVetoMap(analyzer, map_path=jetveto_file, pt_branch="Jet_pt_nom", id_branch="Jet_jetId")
    # Required in Run 3 (JME): drop the event if any loose AK4 jet (pT > 15, tight ID,
    # EM fraction < 0.9) lies in a vetoed region. Same map for data and MC.
    analyzer.Cut("jet_veto_map", "jetmap_vetoed_events == 0")

    # We use JES__up in MC because it has the highest pT for the fatjets and thus gives a conservative selection of valid fatjets that will pass the pT cut under any JEC variation. Assumes that the ordering of fatjets by pT does not change under JEC variations, which is reasonable.
    if data_flag:
        pt_branch_for_selection = "FatJet_pt_nom"
        mass_branch_for_selection = "FatJet_msoftdrop_nom"
        jec_variations = ["nom"]
    else:        
        pt_branch_for_selection = "FatJet_pt_JES__up"
        mass_branch_for_selection = "FatJet_msoftdrop_JES__up"  
        jec_variations = ["nom", "JES__up", "JES__down", "JER__up", "JER__down"]

    def select_jets_expression(jet_id_min):
        return "SelectJets({0}, FatJet_eta, {1}, FatJet_jetId, {2}, {3}, {4}, {5})".format(
            pt_branch_for_selection,
            mass_branch_for_selection,
            preselection_cuts["valid_fatjet_pt_min"],
            preselection_cuts["valid_fatjet_abs_eta_max"],
            preselection_cuts["valid_fatjet_mass_min"],
            jet_id_min,
        )

    # Book the jet-veto count and the "two valid fatjets before jet ID" count together,
    # so they cost one event loop. The latter is cutflow information only.
    no_id_frame = analyzer.DataFrame.Filter(f"{select_jets_expression(0)}.size() >= 2")
    counts = [analyzer.DataFrame.Count(), no_id_frame.Count()]
    if not data_flag:
        counts += [analyzer.DataFrame.Sum("genWeight"), no_id_frame.Sum("genWeight")]
    n_veto, n_two_valid_no_id = counts[0].GetValue(), counts[1].GetValue()
    if data_flag:
        n_veto_weighted, n_two_valid_no_id_weighted = n_veto, n_two_valid_no_id
    else:
        n_veto_weighted, n_two_valid_no_id_weighted = counts[2].GetValue(), counts[3].GetValue()

    analyzer.Define(
        "valid_fatjet_indices",
        select_jets_expression(preselection_cuts["valid_fatjet_jet_id_min"]),
    )
    analyzer.Define("n_valid_fatjets", "valid_fatjet_indices.size()")
    analyzer.Cut("two_valid_fatjets", "n_valid_fatjets >= 2")
    n_two_valid_fatjets = get_n_events(analyzer)
    n_two_valid_fatjets_weighted = get_n_weighted(analyzer, data_flag)

    # FatJet index of the valid fatjet with higher globalParT XbbVsQCD score (H candidate)
    analyzer.Define("leading_xbb_vs_qcd","FatJet_globalParT3_Xbb[valid_fatjet_indices[0]]/(FatJet_globalParT3_Xbb[valid_fatjet_indices[0]]+FatJet_globalParT3_QCD[valid_fatjet_indices[0]])")
    analyzer.Define("subleading_xbb_vs_qcd","FatJet_globalParT3_Xbb[valid_fatjet_indices[1]]/(FatJet_globalParT3_Xbb[valid_fatjet_indices[1]]+FatJet_globalParT3_QCD[valid_fatjet_indices[1]])")
    analyzer.Define(
        "h_cand_idx",
        "leading_xbb_vs_qcd > subleading_xbb_vs_qcd ? valid_fatjet_indices[0] : valid_fatjet_indices[1]",
    )
    analyzer.Define(
        "y_cand_idx",
        "valid_fatjet_indices[0] == h_cand_idx ? valid_fatjet_indices[1] : valid_fatjet_indices[0]",
    )

    # Constituents of the Y candidatefor Lund plane reweighting
    analyzer.Define("sel_pfcand_link", "FatJetPFCand_jetIdx == y_cand_idx")
    analyzer.Define("sel_pfcand_idx", "FatJetPFCand_pfCandIdx[sel_pfcand_link]")
    for pfcand_variable in ["pt", "eta", "phi", "mass", "pdgId"]:
        analyzer.Define(f"y_cand_pfcand_{pfcand_variable}", f"ROOT::VecOps::Take(PFCand_{pfcand_variable}, sel_pfcand_idx)")

    if not data_flag:
        define_lund_gen_parts(analyzer)

    analyzer.Define("h_cand_reg_mass", "FatJet_regressed_mass[h_cand_idx]")
    analyzer.Define("y_cand_reg_mass", "FatJet_regressed_mass[y_cand_idx]")
    analyzer.Define("h_cand_kin_mass", "FatJet_mass_nom[h_cand_idx]")
    analyzer.Define("y_cand_kin_mass", "FatJet_mass_nom[y_cand_idx]")

    for jec_variation in jec_variations:
        analyzer.Define(f"h_cand_pt_{jec_variation}", f"FatJet_pt_{jec_variation}[h_cand_idx]")
        analyzer.Define(f"h_cand_msd_{jec_variation}", f"FatJet_msoftdrop_{jec_variation}[h_cand_idx]")
        analyzer.Define(f"y_cand_pt_{jec_variation}", f"FatJet_pt_{jec_variation}[y_cand_idx]")
        analyzer.Define(f"y_cand_msd_{jec_variation}", f"FatJet_msoftdrop_{jec_variation}[y_cand_idx]")
        analyzer.Define(
            "h_cand_vec_{0}".format(jec_variation),
            "hardware::TLvector(h_cand_pt_{0}, FatJet_eta[h_cand_idx], FatJet_phi[h_cand_idx], h_cand_msd_{0})".format(jec_variation),
        )
        analyzer.Define(
            "y_cand_vec_{0}".format(jec_variation),
            "hardware::TLvector(y_cand_pt_{0}, FatJet_eta[y_cand_idx], FatJet_phi[y_cand_idx], y_cand_msd_{0})".format(jec_variation),
        )
        analyzer.Define(f"m_jj_{jec_variation}", f"hardware::InvariantMass({{h_cand_vec_{jec_variation}, y_cand_vec_{jec_variation}}})")

    m_jj_skim_expression = " || ".join(
        f"m_jj_{jec_variation} > {preselection_cuts['m_jj_skim_min']}" for jec_variation in jec_variations
    )
    analyzer.Cut("m_jj_skim", m_jj_skim_expression)
    n_mjj_skim = get_n_events(analyzer)
    n_mjj_skim_weighted = get_n_weighted(analyzer, data_flag)

    snapshot_columns = get_preselection_snapshot_columns(year)

    analyzer.Snapshot(snapshot_columns, options.output, "Events", lazy=False, openOption="RECREATE", saveRunChain=True)

    cutflow_labels = ["Total", "Lumi mask", "MET Filters", ">1 fatjets", "Jet veto map", "Two valid fatjets (no ID)", "Two valid fatjets (+ jet ID)", f"m_jj > {preselection_cuts['m_jj_skim_min']}"]
    h_cutflow = build_cutflow_histogram(cutflow_labels, [n_total, n_lumi, n_met, n_one_fatjet, n_veto, n_two_valid_no_id, n_two_valid_fatjets, n_mjj_skim], "h_cutflow", "Cutflow; Cut; Events")
    h_cutflow_weighted = build_cutflow_histogram(
        cutflow_labels,
        [n_total_weighted, n_lumi_weighted, n_met_weighted, n_one_fatjet_weighted, n_veto_weighted, n_two_valid_no_id_weighted, n_two_valid_fatjets_weighted, n_mjj_skim_weighted],
        "h_cutflow_weighted",
        "Weighted cutflow; Cut; Weighted events",
    )

    output_file = ROOT.TFile(options.output, "UPDATE")
    output_file.cd()
    h_cutflow.Write()
    h_cutflow_weighted.Write()
    output_file.Close()
    print(f"Saved preselection outputs and cutflow histograms to {options.output}")
    print("Total time: {:.2f} min".format((time.time() - start_time) / 60.0))


parser = OptionParser()

parser.add_option('-i', '--input', metavar='IFILE', type='string', action='store',
                default='',
                dest='input',
                help='A root file or text file with multiple root file locations to analyze')
parser.add_option('-y', '--year', metavar='YEAR', type='string', action='store', default='2024')
parser.add_option('-o', '--output', metavar='OFILE', type='string', action='store', default='output.root')
(options, args) = parser.parse_args()

event_preselection(options)