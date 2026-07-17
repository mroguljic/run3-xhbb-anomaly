#!/usr/bin/env python3
"""Full (Xbb WP x anti-QCD WP) significance scan per signal point.

Reads the joint (m_jj, m_jY, Xbb, anti-QCD) THnD that
selection_and_templating.py books at the inclusive level
(inclusive_h_xbb_vs_y_antiqcd) directly from the merged per-process template
files, so no per-WP re-templating is needed: every (mass window, Xbb WP,
anti-QCD WP) combination is just a bin-range sum over one histogram.

The WP scan isn't limited to a curated set of points -- since the THnD's own
Xbb/anti-QCD binning already resolves every WP the histogram can distinguish,
tagger_studies.thn_utils.window_yield_grid + suffix_sum_2d compute the yield
for *all* bin-boundary combinations in one vectorized pass per signal/
background, rather than re-summing the THnD once per scan point.

For each signal in config.SIGNALS (edit that list to control which signals
get scanned):
  1. Derive its own (m_jj, m_jY) integration window from its kinematic peak
     (tagger_studies/window_finder.py's fixed-fraction contour method).
  2. Skip it if that window doesn't actually contain the signal's own (MX, MY)
     -- this happens for MY/MX ratios above ~0.2-0.3, where the Y->bb decay
     products are too wide-angle to merge into a single AK8 jet, so neither
     axis reconstructs the true resonance mass and any WP scan built on that
     window would not be physically meaningful.
  3. Compute the Asimov significance Z = sqrt(2*((S+B)*ln(1+S/B) - S)) over
     every (Xbb bin, anti-QCD bin) combination, with B summed (not combined in
     quadrature) over config.BACKGROUND_PROCESSES.
  4. Plot a significance heatmap and log the best-WP point.

Usage:
    python3 tagger_studies/significance_scan.py
    python3 tagger_studies/significance_scan.py --signals MX600_MY400 MX1800_MY100
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from condor.config import LOCAL_MERGED_TEMPLATES_DIR
from plotting.config import CMS_COLORS, PROCESSES as PLOT_PROCESSES
from tagger_studies import config
from tagger_studies.thn_utils import MJJ_AXIS, MJY_AXIS, open_thn, project_1d, suffix_sum_2d, window_yield_grid
from tagger_studies.window_finder import find_signal_window

SIGNAL_NAME_RE = re.compile(r"templates_(MX(\d+)_MY(\d+))\.root")

DEFAULT_MERGED_DIR = Path(LOCAL_MERGED_TEMPLATES_DIR)
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "scans"

# Sequential blue ramp, light->dark (tagger_studies/../dataviz skill palette).
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#5598e7", "#256abf", "#104281", "#0d366b"]


def discover_signals(merged_dir: Path) -> dict:
    """Map signal name -> (mx, my, path) for every templates_MX<mx>_MY<my>.root."""
    signals = {}
    for f in sorted(merged_dir.glob("templates_MX*_MY*.root")):
        m = SIGNAL_NAME_RE.match(f.name)
        if not m:
            continue
        name, mx, my = m.group(1), int(m.group(2)), int(m.group(3))
        signals[name] = (mx, my, f)
    return signals


def window_is_sane(window: Tuple[float, float, float, float], mx: int, my: int) -> bool:
    """A window is physically meaningful only if it actually contains the
    signal's own generated (MX, MY) -- otherwise the reconstructed m_jj/m_jY
    peak isn't tracking the true resonance masses at all (see module docstring)."""
    mjj_lo, mjj_hi, mjy_lo, mjy_hi = window
    return mjj_lo <= mx <= mjj_hi and mjy_lo <= my <= mjy_hi


def asimov_significance(s: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Asimov significance Z = sqrt(2*((s+b)*ln(1+s/b) - s)), 
    more appropriate than S/sqrt(B) in the low-B regime

    Returns 0 wherever b <= 0 (a low-MC-stats artifact at very tight WPs
    here, not a real zero-background regime -- Z formally diverges there).
    """
    s = np.clip(s, 0, None)
    b = np.clip(b, 0, None)
    ratio = np.divide(s, b, out=np.zeros_like(s), where=b > 0)
    z_arg = np.clip(2 * ((s + b) * np.log1p(ratio) - s), 0, None)
    return np.where(b > 0, np.sqrt(z_arg), 0.0)


def compute_significance_grid(
    signal_thn,
    bkg_thns: List,
    window: Tuple[float, float, float, float],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (significance, xbb_edges, antiqcd_edges) for every (Xbb, anti-QCD)
    bin-boundary combination the THnD's binning resolves."""
    mjj_range = (window[0], window[1])
    mjy_range = (window[2], window[3])

    sig_raw, xbb_edges, antiqcd_edges = window_yield_grid(signal_thn, mjj_range, mjy_range)
    s_pass = suffix_sum_2d(sig_raw)

    b_pass = np.zeros_like(s_pass)
    for bthn in bkg_thns:
        b_raw, _, _ = window_yield_grid(bthn, mjj_range, mjy_range)
        b_pass += suffix_sum_2d(b_raw)

    significance = asimov_significance(s_pass, b_pass)
    return significance, xbb_edges, antiqcd_edges


def plot_significance_heatmap(
    grid: np.ndarray,
    xbb_edges: np.ndarray,
    antiqcd_edges: np.ndarray,
    signal_name: str,
    window: Tuple[float, float, float, float],
    output_dir: Path,
) -> None:
    import matplotlib.colors as mcolors
    import matplotlib.pyplot as plt

    cmap = mcolors.LinearSegmentedColormap.from_list("sequential_blue", BLUE_RAMP)

    best_i, best_j = np.unravel_index(np.argmax(grid), grid.shape)
    best_xbb, best_antiqcd, best_sig = xbb_edges[best_i], antiqcd_edges[best_j], grid[best_i, best_j]

    fig, ax = plt.subplots(figsize=(8, 6.5))
    # grid is (n_xbb, n_antiqcd); pcolormesh wants (rows=y, cols=x), so transpose
    # to put anti-QCD on x and Xbb on y, matching axis labels below.
    mesh = ax.pcolormesh(antiqcd_edges, xbb_edges, grid, cmap=cmap, vmin=0, shading="auto")

    ax.scatter([best_antiqcd], [best_xbb], marker="*", s=220, color="#f5a623",
               edgecolor="#1a1a1a", linewidth=1.0, zorder=5,
               label=f"Best: Xbb>{best_xbb:.2f}, anti-QCD>{best_antiqcd:.2f} (Z={best_sig:.2f})")

    ax.set_xlabel("Y-candidate anti-QCD WP (score >)")
    ax.set_ylabel("H-candidate Xbb WP (score >)")
    ax.legend(loc="lower left", framealpha=0.9)

    cbar = fig.colorbar(mesh, ax=ax)
    cbar.set_label("Asimov significance Z")

    mjj_lo, mjj_hi, mjy_lo, mjy_hi = window
    ax.set_title(
        f"{signal_name}\n"
        f"window: $m_{{jj}}\\in[{mjj_lo:.0f},{mjj_hi:.0f}]$ GeV, "
        f"$m_{{jY}}\\in[{mjy_lo:.0f},{mjy_hi:.0f}]$ GeV"
    )

    fig.tight_layout()
    output_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_dir / "significance.png", dpi=200)
    fig.savefig(output_dir / "significance.pdf")
    plt.close(fig)


def plot_mass_distributions(
    signal_name: str,
    mx: int,
    my: int,
    signal_thn,
    bkg_thns: Dict[str, object],
    window: Tuple[float, float, float, float],
    best_xbb: float,
    best_antiqcd: float,
    output_dir: Path,
) -> None:
    """Plot the m_jj and m_jY distributions (stacked backgrounds + signal
    overlay) at the scan's best WP. For each mass axis, writes two versions:

    - "<axis>_dist" -- restricted to the *other* axis' window (i.e. exactly
      the selection that fed the significance calculation), with that cut
      stated in the legend title and the plotted axis' own window shaded for
      reference (shading only, not applied as a cut here).
    - "<axis>_dist_full" -- the full projection with no cut on the other
      axis, only the WP cut.
    """
    import matplotlib.pyplot as plt
    import mplhep as hep

    from plotting.config import MATPLOTLIB_RCPARAMS

    plt.style.use(hep.style.CMS)
    plt.rcParams.update(MATPLOTLIB_RCPARAMS)

    mjj_lo, mjj_hi, mjy_lo, mjy_hi = window
    # (axis_index, base filename, axis symbol, own window (shaded), other
    # axis' window (applied as a cut in the windowed variant), other axis symbol)
    axes_specs = [
        (MJJ_AXIS, "mjj_dist", r"$m_{jj}$", (mjj_lo, mjj_hi), (mjy_lo, mjy_hi), r"$m_{jY}$"),
        (MJY_AXIS, "mjy_dist", r"$m_{jY}$", (mjy_lo, mjy_hi), (mjj_lo, mjj_hi), r"$m_{jj}$"),
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    for axis_index, base_filename, xlabel_sym, own_window, other_window, other_sym in axes_specs:
        for suffix, other_mass_range, cut_note in [
            ("", other_window, f"{other_sym} cut: [{other_window[0]:.0f}, {other_window[1]:.0f}] GeV"),
            ("_full", None, f"{other_sym}: full range (no cut)"),
        ]:
            bkg_values, bkg_labels, bkg_colors, edges = [], [], [], None
            for proc, bthn in bkg_thns.items():
                values, edges = project_1d(bthn, axis_index, best_xbb, best_antiqcd, other_mass_range=other_mass_range)
                proc_cfg = PLOT_PROCESSES.get(proc, {})
                bkg_values.append(values)
                bkg_labels.append(proc_cfg.get("label", proc))
                bkg_colors.append(proc_cfg.get("color", "#cccccc"))

            sig_values, sig_edges = project_1d(signal_thn, axis_index, best_xbb, best_antiqcd, other_mass_range=other_mass_range)

            fig, ax = plt.subplots(figsize=(8, 7))
            if bkg_values:
                ax.hist([edges[:-1]] * len(bkg_values), bins=edges, weights=bkg_values, stacked=True,
                        label=bkg_labels, color=bkg_colors, edgecolor="none")
            ax.hist(sig_edges[:-1], bins=sig_edges, weights=sig_values, histtype="step",
                    linewidth=2.5, color=CMS_COLORS["red"], label=f"{signal_name} (MX={mx}, MY={my})", zorder=5)
            ax.axvspan(own_window[0], own_window[1], color="black", alpha=0.08,
                       label=f"{xlabel_sym} window: [{own_window[0]:.0f}, {own_window[1]:.0f}] GeV", zorder=0)

            ax.set_xlabel(f"{xlabel_sym} [GeV]")
            ax.set_ylabel("Events")
            ax.set_yscale("log")
            ax.legend(fontsize=12, title=cut_note, title_fontsize=12)
            ax.set_title(f"{signal_name}: Xbb>{best_xbb:.2f}, anti-QCD>{best_antiqcd:.2f}", fontsize=16)

            fig.tight_layout()
            fig.savefig(output_dir / f"{base_filename}{suffix}.png", dpi=200)
            fig.savefig(output_dir / f"{base_filename}{suffix}.pdf")
            plt.close(fig)


def run_scan(merged_dir: Path, output_dir: Path, signal_names: List[str]) -> None:
    available = discover_signals(merged_dir)

    bkg_thns = {
        proc: open_thn(str(merged_dir / f"templates_{proc}.root"), config.THN_HIST_NAME)
        for proc in config.BACKGROUND_PROCESSES
    }

    summary_rows = []
    n_skipped = 0

    for name in signal_names:
        if name not in available:
            print(f"[skip] {name}: no merged template found in {merged_dir}")
            n_skipped += 1
            continue
        mx, my, path = available[name]

        thn = open_thn(str(path), config.THN_HIST_NAME)
        window = find_signal_window(thn, target_fraction=config.WINDOW_TARGET_FRACTION)

        if not window_is_sane(window, mx, my):
            print(f"[skip] {name}: window {window} does not contain (MX={mx}, MY={my}); "
                  f"Y candidate likely not merged into a single AK8 jet at this mass ratio")
            n_skipped += 1
            continue

        grid, xbb_edges, antiqcd_edges = compute_significance_grid(thn, list(bkg_thns.values()), window)
        best_i, best_j = np.unravel_index(np.argmax(grid), grid.shape)
        best_xbb, best_antiqcd, best_sig = xbb_edges[best_i], antiqcd_edges[best_j], grid[best_i, best_j]

        plot_significance_heatmap(grid, xbb_edges, antiqcd_edges, name, window, output_dir / name)
        plot_mass_distributions(name, mx, my, thn, bkg_thns, window, best_xbb, best_antiqcd, output_dir / name)

        print(f"[ok] {name}: window mjj=[{window[0]:.0f},{window[1]:.0f}] "
              f"mjy=[{window[2]:.0f},{window[3]:.0f}], best WP xbb>{best_xbb:g} antiqcd>{best_antiqcd:g}, "
              f"significance={best_sig:.3f} (scanned {grid.size} bin combinations)")

        summary_rows.append({
            "signal": name, "mx": mx, "my": my,
            "mjj_lo": window[0], "mjj_hi": window[1], "mjy_lo": window[2], "mjy_hi": window[3],
            "best_xbb_wp": best_xbb, "best_antiqcd_wp": best_antiqcd, "best_significance": best_sig,
        })

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / "summary.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()) if summary_rows else [
            "signal", "mx", "my", "mjj_lo", "mjj_hi", "mjy_lo", "mjy_hi",
            "best_xbb_wp", "best_antiqcd_wp", "best_significance",
        ])
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"\n{len(summary_rows)} signal(s) scanned, {n_skipped} skipped.")
    print(f"Summary written to {output_dir / 'summary.csv'}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--merged-dir", type=Path, default=DEFAULT_MERGED_DIR,
                         help=f"Directory with merged templates_<process>.root files (default: {DEFAULT_MERGED_DIR})")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                         help=f"Directory to save per-signal scan plots/summary (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--signals", nargs="+", default=None,
                         help="Restrict to these signal names (e.g. MX1800_MY100); default: config.SIGNALS")
    args = parser.parse_args()

    run_scan(args.merged_dir, args.output_dir, args.signals if args.signals is not None else config.SIGNALS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
