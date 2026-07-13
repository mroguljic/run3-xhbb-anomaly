"""Working point scan for TIMBER-based selection.
Produces mjj–mjy templates for Xbb / anti-QCD working point optimization.
"""

import time
import numpy as np
from optparse import OptionParser

import ROOT
from TIMBER import Analyzer

from analysis_utils import get_n_weighted, is_data
import cuts
from selection_and_templating import load_selection_cutflow_histogram, make_extended_cutflow_histogram
from tagger_studies.config import M_JJ_BINS, M_JY_BINS, SCAN_CONFIG

# ----------------------------
# Common definitions
# ----------------------------

def define_common_columns(analyzer: Analyzer, data_flag: bool, year: str) -> None:
    """Trimmed version of define_common_columns in selection_and_templating.py."""

    if data_flag:
        analyzer.Define("event_weight", "1.0")
    else:
        analyzer.Define("event_weight", "genWeight")

    analyzer.Define("h_cand_xbb", "FatJet_globalParT3_Xbb[h_cand_idx]/(FatJet_globalParT3_Xbb[h_cand_idx]+FatJet_globalParT3_QCD[h_cand_idx])")
    analyzer.Define("y_cand_antiqcd", "1.0 - FatJet_globalParT3_QCD[y_cand_idx]")
    analyzer.Define("trigger_pass", analyzer.GetTriggerString(cuts.triggers[year]))

def apply_preselection(analyzer: Analyzer, year: str) -> None:
    """Apply full preselection chain identical to templating stage."""

    analyzer.Cut("trigger", "trigger_pass")

    sel = cuts.TEMPLATE_SELECTION[year]

    analyzer.Cut("h_pt", f"h_cand_pt_nom > {sel['h_cand_pt_min']}")
    analyzer.Cut("y_pt", f"y_cand_pt_nom > {sel['y_cand_pt_min']}")

    analyzer.Cut(
        "h_mass",
        f"h_cand_msd_nom >= {sel['h_cand_mass_min']} && h_cand_msd_nom <= {sel['h_cand_mass_max']}"
    )

    analyzer.Cut(
        "y_mass",
        f"y_cand_msd_nom >= {sel['y_cand_mass_min']}"
    )

def apply_working_point(analyzer: Analyzer, xbb_wp: float, antiqcd_wp: float) -> None:
    """Independent WP cut (IMPORTANT: no chaining across scan points)."""

    analyzer.Cut(
        f"wp_cut_xbb{xbb_wp:.4f}_aqcd{antiqcd_wp:.4f}",
        f"(h_cand_xbb > {xbb_wp}) && (y_cand_antiqcd > {antiqcd_wp})"
    )

def book_mjj_mjy(analyzer: Analyzer, tag: str):
    """Only output we care about."""

    return analyzer.DataFrame.Histo2D(
        (f"mjj_mjy_{tag}", ";m_{jj} [GeV];m_{jy} [GeV]",
         *M_JJ_BINS, *M_JY_BINS),
        "m_jj_nom",
        "y_cand_msd_nom",
        "event_weight",
    )

def make_scan_points(cfg):
    """Generate combined WP scan points with deduplication."""

    seen = set()
    points = []

    # --- scan anti-QCD while fixing Xbb ---
    xbb = cfg["xbb_fixed"]
    antiqcd_vals = cfg["antiqcd_scan_points"]

    for v in antiqcd_vals:
        p = (xbb, float(v))
        if p not in seen:
            seen.add(p)
            points.append(p)

    # --- scan Xbb while fixing anti-QCD ---
    antiqcd = cfg["antiqcd_fixed"]
    xbb_vals = cfg["xbb_scan_points"]

    for v in xbb_vals:
        p = (float(v), antiqcd)
        if p not in seen:
            seen.add(p)
            points.append(p)

    return points


# ----------------------------
# Main execution
# ----------------------------

def run_working_point_scan(input_file, output_file, year):

    start = time.time()

    analyzer = Analyzer.analyzer(input_file)
    data_flag = is_data(analyzer)

    print(f"[INFO] Running WP scan on {'DATA' if data_flag else 'MC'}")

    define_common_columns(analyzer, data_flag, year)
    base_node = analyzer.GetActiveNode()
    apply_preselection(analyzer, year)
    preselection_node = analyzer.GetActiveNode()
    selection_cutflow = load_selection_cutflow_histogram(input_file, data_flag)

    # ------------------------
    # Scan
    # ------------------------
    scan_points = make_scan_points(SCAN_CONFIG)

    histograms = []
    wp_yields = []

    print("Scan points", scan_points)
    for xbb_wp, antiqcd_wp in scan_points:

        analyzer.SetActiveNode(preselection_node)
        apply_working_point(analyzer, xbb_wp, antiqcd_wp)
        tag = f"xbb{xbb_wp:.4f}_aqcd{antiqcd_wp:.4f}"
        histograms.append(book_mjj_mjy(analyzer, tag))
        wp_yields.append(
            (tag, get_n_weighted(analyzer, data_flag, "event_weight"))
        )

    extra_bins = [(name, val) for name, val in wp_yields]
    extended_cutflow = make_extended_cutflow_histogram(
        selection_cutflow,
        extra_bins
    )

    out = ROOT.TFile(output_file, "RECREATE")

    for h in histograms:
        h.GetValue().Write()

    extended_cutflow.Write()
    out.Close()

    print(f"[DONE] Saved to {output_file}")
    print(f"[TIME] {(time.time() - start)/60:.2f} min")
