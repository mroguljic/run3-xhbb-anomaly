#!/usr/bin/env python3
"""Caveman scan for the Fail/Control lower WP bounds in
cuts.TEMPLATE_REGION_BOUNDARIES.

Reports yields in 

Usage:
    python3 tagger_studies/region_boundary_scan.py
    python3 tagger_studies/region_boundary_scan.py --xbb-pass-wp 0.99 --antiqcd-signal-wp 0.9
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from condor.config import LOCAL_MERGED_TEMPLATES_DIR
from tagger_studies import config
from tagger_studies.thn_utils import ANTIQCD_AXIS, MJJ_AXIS, MJY_AXIS, XBB_AXIS, open_thn

DEFAULT_MERGED_DIR = Path(LOCAL_MERGED_TEMPLATES_DIR)


def region_yield(
    thn,
    xbb_range: Tuple[Optional[float], Optional[float]],
    antiqcd_range: Tuple[Optional[float], Optional[float]],
) -> float:
    """Sum the THnD over its full m_jj/m_jY range, for an (xbb, anti-QCD) band.

    xbb_range/antiqcd_range are (lo, hi) bin-boundary tuples; None means
    unbounded on that side (axis min/max).
    """
    ax_mjj = thn.GetAxis(MJJ_AXIS)
    ax_mjy = thn.GetAxis(MJY_AXIS)
    ax_xbb = thn.GetAxis(XBB_AXIS)
    ax_antiqcd = thn.GetAxis(ANTIQCD_AXIS)

    xbb_lo = ax_xbb.FindBin(xbb_range[0]) if xbb_range[0] is not None else 1
    xbb_hi = ax_xbb.FindBin(xbb_range[1] - 1e-6) if xbb_range[1] is not None else ax_xbb.GetNbins()
    aqcd_lo = ax_antiqcd.FindBin(antiqcd_range[0]) if antiqcd_range[0] is not None else 1
    aqcd_hi = ax_antiqcd.FindBin(antiqcd_range[1] - 1e-6) if antiqcd_range[1] is not None else ax_antiqcd.GetNbins()

    total = 0.0
    idx = np.zeros(4, dtype=np.int32)
    for i in range(1, ax_mjj.GetNbins() + 1):
        idx[MJJ_AXIS] = i
        for j in range(1, ax_mjy.GetNbins() + 1):
            idx[MJY_AXIS] = j
            for k in range(xbb_lo, xbb_hi + 1):
                idx[XBB_AXIS] = k
                for l in range(aqcd_lo, aqcd_hi + 1):
                    idx[ANTIQCD_AXIS] = l
                    total += thn.GetBinContent(idx)
    return total


def total_background_yield(
    bkg_thns: Dict[str, object],
    xbb_range: Tuple[Optional[float], Optional[float]],
    antiqcd_range: Tuple[Optional[float], Optional[float]],
) -> float:
    return sum(region_yield(thn, xbb_range, antiqcd_range) for thn in bkg_thns.values())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--merged-dir", type=Path, default=DEFAULT_MERGED_DIR)
    parser.add_argument("--xbb-pass-wp", type=float, default=0.99, help="Fixed Pass threshold for h_cand_xbb")
    parser.add_argument("--antiqcd-signal-wp", type=float, default=0.9, help="Fixed Signal threshold for y_cand_antiqcd")
    parser.add_argument("--fail-target-ratio", type=float, default=100.0, help="Target Fail-Signal / Pass-Signal ratio")
    args = parser.parse_args()

    xbb_pass_wp = args.xbb_pass_wp
    antiqcd_signal_wp = args.antiqcd_signal_wp

    bkg_thns = {
        proc: open_thn(str(args.merged_dir / f"templates_{proc}.root"), config.THN_HIST_NAME)
        for proc in config.BACKGROUND_PROCESSES
    }

    ax_xbb = next(iter(bkg_thns.values())).GetAxis(XBB_AXIS)
    ax_antiqcd = next(iter(bkg_thns.values())).GetAxis(ANTIQCD_AXIS)

    pass_signal = total_background_yield(bkg_thns, (xbb_pass_wp, None), (antiqcd_signal_wp, None))
    print(f"Fixed Pass/Signal WPs: h_cand_xbb > {xbb_pass_wp}, y_cand_antiqcd > {antiqcd_signal_wp}")
    print(f"Pass-Signal background yield: {pass_signal:.3f}\n")

    # --- Scan Control lower bound: Pass-Control ~= Pass-Signal ---
    antiqcd_candidates = [ax_antiqcd.GetBinLowEdge(b) for b in range(1, ax_antiqcd.GetNbins() + 1)
                          if ax_antiqcd.GetBinLowEdge(b) < antiqcd_signal_wp]

    print(f"{'antiqcd_control_lo':>20} | {'Pass-Control yield':>19} | {'ratio to Pass-Signal':>21}")
    best_control_lo, best_control_diff = None, np.inf
    for lo in antiqcd_candidates:
        y = total_background_yield(bkg_thns, (xbb_pass_wp, None), (lo, antiqcd_signal_wp))
        ratio = y / pass_signal if pass_signal else float("nan")
        marker = ""
        if abs(ratio - 1.0) < best_control_diff:
            best_control_diff, best_control_lo = abs(ratio - 1.0), lo
        print(f"{lo:20.2f} | {y:19.3f} | {ratio:21.3f}")
    print(f"--> closest Control lower bound: antiqcd > {best_control_lo:g} "
          f"(Pass-Control/Pass-Signal = {1 + best_control_diff if best_control_lo is not None else float('nan'):.3f} at worst)\n")

    # --- Scan Fail lower bound: Fail-Signal ~= fail_target_ratio x Pass-Signal ---
    xbb_candidates = [ax_xbb.GetBinLowEdge(b) for b in range(1, ax_xbb.GetNbins() + 1)
                      if ax_xbb.GetBinLowEdge(b) < xbb_pass_wp]

    fail_target = args.fail_target_ratio * pass_signal
    print(f"{'xbb_fail_lo':>20} | {'Fail-Signal yield':>18} | {'ratio to ' + str(args.fail_target_ratio) + 'x Pass-Signal':>28}")
    best_fail_lo, best_fail_diff = None, np.inf
    for lo in xbb_candidates:
        y = total_background_yield(bkg_thns, (lo, xbb_pass_wp), (antiqcd_signal_wp, None))
        ratio = y / fail_target if fail_target else float("nan")
        if abs(ratio - 1.0) < best_fail_diff:
            best_fail_diff, best_fail_lo = abs(ratio - 1.0), lo
        print(f"{lo:20.2f} | {y:18.3f} | {ratio:28.3f}")
    print(f"--> closest Fail lower bound: xbb > {best_fail_lo:g}\n")

    # --- Consistency check on Fail-Control with both chosen bounds ---
    pass_control = total_background_yield(bkg_thns, (xbb_pass_wp, None), (best_control_lo, antiqcd_signal_wp))
    fail_control = total_background_yield(bkg_thns, (best_fail_lo, xbb_pass_wp), (best_control_lo, antiqcd_signal_wp))
    fail_signal = total_background_yield(bkg_thns, (best_fail_lo, xbb_pass_wp), (antiqcd_signal_wp, None))
    print("=" * 70)
    print("Summary with chosen bounds:")
    print(f"  Pass  = h_cand_xbb > {xbb_pass_wp}")
    print(f"  Fail  = {best_fail_lo:g} < h_cand_xbb <= {xbb_pass_wp}")
    print(f"  Signal = y_cand_antiqcd > {antiqcd_signal_wp}")
    print(f"  Control = {best_control_lo:g} < y_cand_antiqcd <= {antiqcd_signal_wp}\n")
    print(f"  Pass-Signal:  {pass_signal:9.3f}")
    print(f"  Pass-Control: {pass_control:9.3f}  (ratio to Pass-Signal: {pass_control/pass_signal:.3f})")
    print(f"  Fail-Signal:  {fail_signal:9.3f}  (ratio to Pass-Signal: {fail_signal/pass_signal:.3f})")
    print(f"  Fail-Control: {fail_control:9.3f}  (ratio to Pass-Control: {fail_control/pass_control:.3f}, "
          f"target was {args.fail_target_ratio:g})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
