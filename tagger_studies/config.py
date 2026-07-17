YEAR = "2024"
THN_HIST_NAME = "inclusive_h_xbb_vs_y_antiqcd"
BACKGROUND_PROCESSES = ["QCD", "TT", "WJets", "ZJets"]

# Fraction of a signal's own (m_jj, m_jY) yield the fixed-fraction-contour
# window (tagger_studies/window_finder.py) is grown to capture.
WINDOW_TARGET_FRACTION = 0.7

# Signals significance_scan.py runs over
SIGNALS = [
    "MX1800_MY100",
    "MX1800_MY300",
    "MX2000_MY200",
    "MX2500_MY400",
    "MX3000_MY200",
    "MX3000_MY300",
    "MX3500_MY200",
    "MX3500_MY300",
    "MX3500_MY400",
]
