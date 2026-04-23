"""
Plotting configuration for template histograms.
"""

from pathlib import Path

# Input and output directories
DEFAULT_TEMPLATE_INPUT_DIR = Path("condor/output/templates")
OUTPUT_PLOTS_DIR = Path("output/plots")
OUTPUT_PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# Histogram names to plot and their metadata
# Each histogram can define:
#   - label: LaTeX label for x-axis
#   - blind: Whether to hide data for this histogram
#   - log_y: Whether to generate log y-scale version (generates both linear and log)
#   - x_range: [xmin, xmax] for x-axis limits, or None for auto
#   - y_range: [ymin, ymax] for linear y-axis limits, or None for auto
#   - y_log_range: [ymin, ymax] for log y-axis limits, or None for auto
#   - rebin: Rebinning factor (1 = no rebinning, 2 = 2x coarser, etc.)
HISTOGRAMS_TO_PLOT = {
    "inclusive_m_jj": {
        "label": r"$m_{jj}$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [600,4000],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "inclusive_m_jy": {
        "label": r"$m_{jY}$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [40,400],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "inclusive_lead_jet_pt": {
        "label": r"Leading jet $p_T$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [260,1500],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "inclusive_lead_jet_eta": {
        "label": r"Leading jet $\eta$",
        "blind": False,
        "log_y": None,
        "x_range": [-2.5,2.5],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "inclusive_lead_jet_phi": {
        "label": r"Leading jet $\phi$",
        "blind": False,
        "log_y": None,
        "x_range": [-3.2,3.2],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "PC_m_jj": {
        "label": r"$m_{jj}$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [400,3000],
        "y_range": None,
        "y_log_range": None,
        "rebin": 2,
    },
    "PC_m_jy": {
        "label": r"$m_{jY}$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [30,400],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "FC_m_jj": {
        "label": r"$m_{jj}$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [400,3000],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "FC_m_jy": {
        "label": r"$m_{jY}$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [30,400],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "PS_m_jj": {
        "label": r"$m_{jj}$ (GeV)",
        "blind": True,
        "log_y": True,
        "x_range": [400,3000],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "PS_m_jy": {
        "label": r"$m_{jY}$ (GeV)",
        "blind": True,
        "log_y": True,
        "x_range": [30,400],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
}

# https://cms-analysis.docs.cern.ch/guidelines/plotting/colors/#categorical-data-eg-1d-stackplots
CMS_COLORS = {
"blue" : "#5790fc",
"orange" : "#f89c20",
"red" : "#e42536",
"purple" : "#964a8b",
"gray" : "#9c9ca1",
"dark_purple" : "#7a21dd"
}

# Process definitions with types and file mappings
# Each process explicitly defines its type (data, bkg, signal), file, and plotting options
PROCESSES = {
    "data": {
        "type": "data",
        "file": "templates_Run2024.root",
        "label": "Data",
        "color": "black"
    },
    "QCD": {
        "type": "bkg",
        "file": "templates_QCD.root",
        "label": "QCD",
        "color": CMS_COLORS["blue"]
    },
    "TT": {
        "type": "bkg",
        "file": "templates_TT.root",
        "label": "ttbar",
        "color": CMS_COLORS["orange"]
    },
    "MX1800_MY100": {
        "type": "signal",
        "file": "templates_MX1800_MY100.root",
        "label": "MX=1800 MY=100",
        "color": CMS_COLORS["red"]
    },
}

# Default styles per process type
PROCESS_TYPE_DEFAULTS = {
    "data": {
        "color": "black",
        "marker": "o",
        "markersize": 5,
    },
    "bkg": {
        "color": "#cccccc",
        "alpha": 0.7,
    },
    "signal": {
        "color": "red",
        "linestyle": "-",
        "linewidth": 2.5,
    },
}

# Plot appearance
FIGURE_SIZE = (8, 8)
DPI = 300
LINE_WIDTH = 2.0
MARKER_SIZE = 6.0

# Legend settings
LEGEND_LOC = "upper right"
LEGEND_FONTSIZE = 20

# Axis settings
XLABEL_FONTSIZE = 22
YLABEL_FONTSIZE = 22
TITLE_AXIS_FONTSIZE = 20
TICK_LABEL_FONTSIZE = 18

# Global matplotlib style overrides
MATPLOTLIB_RCPARAMS = {
    "figure.figsize": FIGURE_SIZE,
    "figure.dpi": DPI,
    "lines.linewidth": LINE_WIDTH,
    "lines.markersize": MARKER_SIZE,
    "axes.labelsize": XLABEL_FONTSIZE,
    "axes.titlesize": TITLE_AXIS_FONTSIZE,
    "xtick.labelsize": TICK_LABEL_FONTSIZE,
    "ytick.labelsize": TICK_LABEL_FONTSIZE,
    "legend.fontsize": LEGEND_FONTSIZE,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
}

# Signal line style
SIGNAL_LINE_STYLE = "-"  # Solid line
SIGNAL_LINE_WIDTH = 2.5

# Data marker style
DATA_MARKER = "o"
DATA_MARKERSIZE = 5
DATA_LINE_WIDTH = 1.5

# CMS style from mplhep
CMS_STYLE = "WiP"  # or "CMS" for preliminary
