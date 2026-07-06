"""
Plotting module for template histograms.

This module provides tools for reading ROOT histogram files and creating
publication-quality plots using matplotlib and mplhep CMS styling.

Main entry point: plotting/cli.py
"""

from plotting.config import (
    HISTOGRAMS_TO_PLOT,
    PROCESSES,
    PROCESS_TYPE_DEFAULTS,
    OUTPUT_PLOTS_DIR,
)
from plotting.utils import read_histogram_from_root, read_histograms_from_files

# plot_histogram/plot_all_histograms are intentionally not re-exported here: they live in
# plotting.template_plotter, which requires mplhep. Import from that module directly
# (as plotting/cli.py does) so that consumers of just PROCESSES/read_histogram_from_root
# (e.g. tagger_studies/roc.py) don't pick up an mplhep dependency they don't need.

__all__ = [
    "HISTOGRAMS_TO_PLOT",
    "PROCESSES",
    "PROCESS_TYPE_DEFAULTS",
    "OUTPUT_PLOTS_DIR",
    "read_histogram_from_root",
    "read_histograms_from_files",
]
