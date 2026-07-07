#!/usr/bin/env python3
"""
ROC curves for the Xbb (H-candidate) and anti-QCD (Y-candidate) taggers.

Reads the `inclusive_h_cand_xbb` / `inclusive_y_cand_antiqcd` diagnostic
histograms that `selection_and_templating.py` books for every process (filled
after the trigger + pT/mass preselection but before the Pass/Fail x
Signal/Control tag-region split), from the merged, cross-section-scaled
per-process template files produced by `condor/merge_templates.py`.

Efficiency at a given score threshold is the fraction of the (weighted)
histogram integral in bins at or above that threshold, so this reflects this
analysis' own H/Y candidate assignment (the higher-Xbb valid fat jet is always
the H candidate) rather than a gen-matched, assignment-free tagger validation
curve.

Usage:
    python3 tagger_studies/roc.py
    python3 tagger_studies/roc.py --input-dir condor/output/templates/merged --output-dir tagger_studies/roc
"""

import argparse
import sys
from pathlib import Path
from typing import Tuple

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

import cuts
from condor.config import LOCAL_MERGED_TEMPLATES_DIR
from plotting.config import PROCESSES
from plotting.utils import read_histogram_from_root

TAGGERS = {
    "xbb": {
        "histogram": "inclusive_h_cand_xbb",
        "title": "H-candidate Xbb tagger",
        "wp_key": "h_xbb_wp",
    },
    "antiqcd": {
        "histogram": "inclusive_y_cand_antiqcd",
        "title": "Y-candidate anti-QCD tagger",
        "wp_key": "y_antiqcd_wp",
    },
}

DEFAULT_INPUT_DIR = Path(LOCAL_MERGED_TEMPLATES_DIR)


def efficiency_curve(bin_contents: np.ndarray, bin_edges: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Return (thresholds, efficiency) for cutting on 'score >= threshold'.

    thresholds[i] is the lower edge of bin i; efficiency[i] is the (weighted)
    fraction of the histogram's total integral at or above that edge.
    """
    total = bin_contents.sum()
    if total <= 0:
        raise ValueError("Histogram has zero or negative total weight; cannot compute efficiency")
    cumulative_from_right = np.cumsum(bin_contents[::-1])[::-1]
    return bin_edges[:-1], cumulative_from_right / total


def load_efficiency(input_dir: Path, filename: str, histogram_name: str) -> Tuple[np.ndarray, np.ndarray]:
    """Load a process' score histogram and convert it to an efficiency curve."""
    file_path = input_dir / filename
    if not file_path.exists():
        raise FileNotFoundError(f"Merged template not found: {file_path}")
    _, bin_contents, _, bin_edges = read_histogram_from_root(str(file_path), histogram_name)
    return efficiency_curve(bin_contents, bin_edges)


def efficiency_at_threshold(thresholds: np.ndarray, efficiency: np.ndarray, threshold_value: float) -> float:
    """Interpolate a (thresholds, efficiency) curve at one score threshold."""
    return float(np.interp(threshold_value, thresholds, efficiency))


def plot_tagger_roc(tagger_key: str, input_dir: Path, output_dir: Path, year: str) -> None:
    """Plot signal-vs-background ROC curves for one tagger, one line per background process."""
    import matplotlib.pyplot as plt

    tagger = TAGGERS[tagger_key]
    working_point = cuts.TEMPLATE_TAGGING_WPS[year][tagger["wp_key"]]
    signal_processes = {name: cfg for name, cfg in PROCESSES.items() if cfg["type"] == "signal"}
    background_processes = {name: cfg for name, cfg in PROCESSES.items() if cfg["type"] == "bkg"}

    if not signal_processes:
        raise RuntimeError("No 'signal' type process found in plotting.config.PROCESSES")
    if not background_processes:
        raise RuntimeError("No 'bkg' type process found in plotting.config.PROCESSES")

    output_dir.mkdir(parents=True, exist_ok=True)

    for signal_name, signal_cfg in signal_processes.items():
        signal_thresholds, signal_efficiency = load_efficiency(input_dir, signal_cfg["file"], tagger["histogram"])
        signal_eff_at_wp = efficiency_at_threshold(signal_thresholds, signal_efficiency, working_point)

        plt.figure(figsize=(7, 7))
        for bkg_name, bkg_cfg in background_processes.items():
            bkg_thresholds, bkg_efficiency = load_efficiency(input_dir, bkg_cfg["file"], tagger["histogram"])
            plt.plot(signal_efficiency, bkg_efficiency, label=bkg_name, color=bkg_cfg.get("color"), linewidth=2.0)

            bkg_eff_at_wp = efficiency_at_threshold(bkg_thresholds, bkg_efficiency, working_point)
            plt.scatter(
                [signal_eff_at_wp], [bkg_eff_at_wp],
                marker="o", s=90, facecolor=bkg_cfg.get("color"), edgecolor="black", linewidth=1.2, zorder=5,
            )

        # single legend entry explaining the WP markers (color already conveys which background)
        plt.scatter([], [], marker="o", s=90, facecolor="white", edgecolor="black", linewidth=1.2, label=f"Selected WP (score > {working_point:g})")

        plt.xlabel(f"Signal efficiency ({signal_cfg['label']})")
        plt.ylabel("Background efficiency")
        plt.yscale("log")
        plt.xlim(0, 1)
        plt.title(f"{tagger['title']} ROC — {signal_cfg['label']}")
        plt.legend()
        plt.grid(True, which="both", alpha=0.3)

        output_path = output_dir / f"roc_{tagger_key}_{signal_name}.png"
        plt.savefig(output_path, dpi=200)
        plt.savefig(output_path.with_suffix(".pdf"))
        plt.close()
        print(f"Saved {output_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Directory with merged templates_<process>.root files (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "roc",
        help="Directory to save ROC plots",
    )
    parser.add_argument(
        "--taggers",
        nargs="+",
        choices=list(TAGGERS),
        default=list(TAGGERS),
        help="Which taggers to plot (default: all)",
    )
    parser.add_argument(
        "--year",
        default="2024",
        choices=list(cuts.TEMPLATE_TAGGING_WPS),
        help="Year whose cuts.TEMPLATE_TAGGING_WPS working point gets marked on the curves (default: 2024)",
    )
    args = parser.parse_args()

    for tagger_key in args.taggers:
        plot_tagger_roc(tagger_key, args.input_dir, args.output_dir, args.year)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
