"""
Plotting configuration for template histograms.
"""

from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent

# Input and output directories
DEFAULT_TEMPLATE_INPUT_DIR = REPO_ROOT / "condor" / "output" / "templates" / "merged"
OUTPUT_PLOTS_DIR = REPO_ROOT / "output" / "plots"
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
#   - ratio: Whether to add a data/MC ratio panel (default: RATIO_PANEL below).
#            Ignored when there is no unblinded data to divide by.
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
    "inclusive_sublead_jet_pt": {
        "label": r"Subleading jet $p_T$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [260,1500],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "inclusive_sublead_jet_eta": {
        "label": r"Subleading jet $\eta$",
        "blind": False,
        "log_y": None,
        "x_range": [-2.5,2.5],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "inclusive_sublead_jet_phi": {
        "label": r"Subleading jet $\phi$",
        "blind": False,
        "log_y": None,
        "x_range": [-3.2,3.2],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "inclusive_abs_delta_eta": {
        "label": r"$|\Delta\eta_{jj}|$",
        "blind": False,
        "log_y": None,
        "x_range": [0,4.8],
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
    "FS_m_jj": {
        "label": r"$m_{jj}$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [400,3000],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "FS_m_jy": {
        "label": r"$m_{jY}$ (GeV)",
        "blind": False,
        "log_y": True,
        "x_range": [30,400],
        "y_range": None,
        "y_log_range": None,
        "rebin": 1,
    },
    "inclusive_h_cand_xbb": {
        "label": r"H candidate Xbb score",
        "blind": False,
        "log_y": True,
        "x_range": [0,1],
        "y_range": None,
        "y_log_range": None,
        "rebin": 20,
    },
    "inclusive_y_cand_antiqcd": {
        "label": r"Y candidate anti-QCD score",
        "blind": False,
        "log_y": True,
        "x_range": [0,1],
        "y_range": None,
        "y_log_range": None,
        "rebin": 20,
    },
}

# https://cms-analysis.docs.cern.ch/guidelines/plotting/colors/#categorical-data-eg-1d-stackplots
CMS_COLORS = {
# 6-color scheme
"blue" : "#5790fc",
"orange" : "#f89c20",
"red" : "#e42536",
"purple" : "#964a8b",
"gray" : "#9c9ca1",
"dark_purple" : "#7a21dd",
# Extras from 10-color scheme
"olive": "#b9ac70",
"teal": "#92dadd"
}

# Process definitions with types and file mappings
# Each process explicitly defines its type (data, bkg, signal), file, and plotting options
PROCESSES = {
    "data": {
        "type": "data",
        "file": "templates_JetMET2024.root",
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
    "WJets": {
        "type": "bkg",
        "file": "templates_WJets.root",
        "label": "W+jets",
        "color": CMS_COLORS["olive"]
    },
    "ZJets": {
        "type": "bkg",
        "file": "templates_ZJets.root",
        "label": "Z+jets",
        "color": CMS_COLORS["teal"]
    },
    "MX2000_MY200": {
        "type": "signal",
        "file": "templates_MX2000_MY200.root",
        "label": "MX=2000 MY=200",
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

# Data/MC ratio panel (bottom panel). Only drawn when unblinded data is present.
RATIO_PANEL = True
RATIO_FIGURE_SIZE = (8, 9.5)
RATIO_HEIGHT_RATIOS = (3, 1)
RATIO_Y_RANGE = (0.0, 2.0)
RATIO_YLABEL = "Data / MC"
RATIO_MC_BAND_COLOR = "#9c9ca1"
RATIO_MC_BAND_ALPHA = 0.5
RATIO_MC_BAND_LABEL = "MC stat. unc."
LINE_WIDTH = 2.0
MARKER_SIZE = 6.0

# Legend settings
LEGEND_LOC = "upper right"
LEGEND_FONTSIZE = 17
LEGEND_NCOL = 2

# Automatic y-axis headroom, so the legend and region box do not sit on top of
# the histograms. Used only when the histogram has no explicit "y_range" /
# "y_log_range" in HISTOGRAMS_TO_PLOT; the peak is measured inside the plotted x_range
Y_AUTO_RANGE = True
# The tallest drawn point is placed at this fraction of the panel height, leaving
# the rest free for the legend and region box.
Y_PEAK_FRACTION = 0.72
# Log scale: never add more than this many decades above the peak
Y_HEADROOM_LOG_MAX_DECADES = 4.0
# Log scale bottom: this far below the smallest positive value drawn, but never
# more than Y_LOG_MAX_DECADES below the peak
Y_LOG_BOTTOM_PAD_DECADES = 0.5
Y_LOG_MAX_DECADES = 7.0

# Region text box: an in-plot annotation naming the (H tag, Y tag) region a
# histogram belongs to, derived from its name prefix (e.g. "PC_m_jj" -> Pass/Control).
# Per-histogram override: set "region_text" in HISTOGRAMS_TO_PLOT (a string, or
# None/"" to suppress the box for that histogram).
REGION_TEXT_BOX = True
REGION_TEXT_FONTSIZE = 17
REGION_TEXT_POSITION = (0.02, 0.96)  # axes fraction, top-left corner of the box
REGION_TEXT_BBOX = {
    "boxstyle": "round",
    "facecolor": "white",
    "edgecolor": CMS_COLORS["gray"],
    "alpha": 0.8,
}

# Histogram-name prefix -> label shown in the region box. The prefix is the
# (H tag, Y tag) region encoding written by selection_and_templating.py
REGION_PREFIX_TITLES = {
    "PS": "Pass / Signal",
    "PC": "Pass / Control",
    "FS": "Fail / Signal",
    "FC": "Fail / Control",
    "inclusive": "Inclusive",
}


def get_region_text(histogram_name: str, year: Optional[str] = None) -> Optional[str]:
    """
    Text for the in-plot region box of a histogram, or None if it has no region.

    The region is taken from the histogram name prefix (the part before the first
    underscore). `year` is accepted for interface stability but unused: only the
    region name is shown, not the tagger working point values.
    """
    prefix = histogram_name.split("_")[0]
    return REGION_PREFIX_TITLES.get(prefix)

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


CMS_STYLE = "WiP"

# Relative font scales for the mplhep CMS label: (exp, text, lumi, supp).
# mplhep defaults are (1.3, 1.0, 0.77, 0.77); the lumi line is shrunk so
# "<lumi> fb^-1, <year> (13.6 TeV)" fits next to "CMS Simulation".
CMS_LABEL_FONTSCALES = (1.3, 1.0, 0.68, 0.68)
