from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

MANIFEST_PATH = str(REPO_ROOT / "condor" / "template_manifest_2024.json")
PROCESS_MAPS = {"MX1800_MY100": ["MX1800_MY100"], "QCD": ["QCD-4Jets_HT-600to800","QCD-4Jets_HT-800to1000","QCD-4Jets_HT-1000to1200","QCD-4Jets_HT-1200to1500", "QCD-4Jets_HT-1500to2000", "QCD-4Jets_HT-2000"], "TT": ["TTto4Q", "TTtoLNu2Q"]}
TEMPLATE_STORE_DIR = str(REPO_ROOT / "tagger_studies" / "templates")
YEAR = "2024"
SIGNAL = "MX1800_MY100"


M_JJ_BINS = (66, 700, 4000)
M_JY_BINS = (112, 40, 600)


SCAN_CONFIG = {
    "xbb_fixed": 0.99,
    "antiqcd_fixed": 0.5,
}

import numpy as np
antiqcd_vals = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
xbb_vals = [0.975, 0.99] # These have SFs ready https://indico.cern.ch/event/1602300/contributions/6751293/attachments/3157105/5609849/Run-3%20GloParT%20TXbb%20Calibration%20with%20Zbb%20(Updated).pdf

SCAN_CONFIG["xbb_scan_points"] = xbb_vals
SCAN_CONFIG["antiqcd_scan_points"] = antiqcd_vals

# Hand picked bins for MX1800_MY100 bsed on the following bins
#M_JJ_BINS = (66, 700, 4000)
#M_JY_BINS = (112, 40, 600)
INTEGRAL_X_MIN_BIN = 19
INTEGRAL_X_MAX_BIN = 24
INTEGRAL_Y_MIN_BIN = 10
INTEGRAL_Y_MAX_BIN = 19