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
from plotting.template_plotter import plot_histogram, plot_all_histograms

__all__ = [
    "HISTOGRAMS_TO_PLOT",
    "PROCESSES",
    "PROCESS_TYPE_DEFAULTS",
    "OUTPUT_PLOTS_DIR",
    "read_histogram_from_root",
    "read_histograms_from_files",
    "plot_histogram",
    "plot_all_histograms",
]
