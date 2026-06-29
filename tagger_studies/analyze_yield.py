import ROOT
from filelists.xsecs import get_int_lumi, get_xsec
from tagger_studies.config import PROCESS_MAPS, INTEGRAL_X_MAX_BIN, INTEGRAL_X_MIN_BIN, INTEGRAL_Y_MAX_BIN, INTEGRAL_Y_MIN_BIN, SIGNAL, TEMPLATE_STORE_DIR, SCAN_CONFIG, YEAR

def get_weighted_yield(filename, hname, process, year):

    def get_from_histo(filename, hname):
        # Get the localized yield from a histogram in a ROOT file, using the integral over specified bins. Needs to be scaled with cross section weight
        f = ROOT.TFile.Open(filename)
        histo = f.Get(hname)
        return histo.Integral(INTEGRAL_X_MIN_BIN, INTEGRAL_X_MAX_BIN, INTEGRAL_Y_MIN_BIN, INTEGRAL_Y_MAX_BIN)

    def get_genw_sum(filename):
        f = ROOT.TFile.Open(filename)
        histo = f.Get("template_cutflow")
        return histo.GetBinContent(1)

    int_lumi = get_int_lumi(year)
    total_yield = 0.0
    for subprocesses in PROCESS_MAPS[process]:
        xsec = get_xsec(subprocesses)
        genw_sum = get_genw_sum(filename)
        localized_yield = get_from_histo(filename, hname)
        weighted_yield = localized_yield * xsec * int_lumi / genw_sum
        print(f"{subprocesses} yield: {weighted_yield:.2f}")
        total_yield += weighted_yield
    
    print(f"Total yield for {process}: {total_yield:.2f}")
    return total_yield

def scan_xbb(cfg, year):
    xbb_vals = cfg["xbb_scan_points"]
    aqcd_val = cfg["antiqcd_fixed"]

    bkg_yields = []
    sig_yields = []
    for xbb_val in xbb_vals:
        sig_yield = 0.
        bkg_yield = 0.
        h = f"mjj_mjy_xbb{xbb_val:.4f}_aqcd{aqcd_val:.4f}"
        for process, subprocesses in PROCESS_MAPS.items():
            process_yield = 0.
        
            for subprocess in subprocesses:
                filename = f"{TEMPLATE_STORE_DIR}/{subprocess}_templates.root"
                subprocess_yield = get_weighted_yield(filename, h, process, YEAR)
                process_yield += subprocess_yield
        
            if process == SIGNAL:
                sig_yield += process_yield
            else:
                bkg_yield += process_yield
        
        sig_yields.append(sig_yield)
        bkg_yields.append(bkg_yield)
    return sig_yields, bkg_yields, xbb_vals

def scan_aqcd(cfg, year):
    xbb_val = cfg["xbb_fixed"]
    aqcd_vals = cfg["antiqcd_scan_points"]

    bkg_yields = []
    sig_yields = []
    for aqcd_val in aqcd_vals:
        sig_yield = 0.
        bkg_yield = 0.
        h = f"mjj_mjy_xbb{xbb_val:.4f}_aqcd{aqcd_val:.4f}"
        for process, subprocesses in PROCESS_MAPS.items():
            process_yield = 0.
        
            for subprocess in subprocesses:
                filename = f"{TEMPLATE_STORE_DIR}/{subprocess}_templates.root"
                subprocess_yield = get_weighted_yield(filename, h, process, YEAR)
                process_yield += subprocess_yield
        
            if process == SIGNAL:
                sig_yield += process_yield
            else:
                bkg_yield += process_yield
        
        sig_yields.append(sig_yield)
        bkg_yields.append(bkg_yield)
    return sig_yields, bkg_yields, aqcd_vals

def plot_scan(scan_points, significances, output_file, label):
    import matplotlib.pyplot as plt
    plt.figure(figsize=(10, 6))
    plt.plot(scan_points, significances, marker='o',label=label)
    plt.title(f'Significance Scan for {label}')
    plt.xlabel(f'{label} Working Point')
    plt.ylabel('Significance (S/sqrt(B))')
    max_significance = max(significances)
    best_scan_point = scan_points[significances.index(max_significance)]
    plt.scatter([best_scan_point], [max_significance], color='red', label=f'Max Significance: at {best_scan_point:.4f}')
    plt.savefig(output_file)
    plt.savefig(output_file.replace(".png", ".pdf"))

def optimize_tagger(tagger):   
    if tagger == "xbb":
        sig_yields, bkg_yields, scan_points = scan_xbb(SCAN_CONFIG, YEAR)
        label = "Significance vs Xbb WP"
        output_plot = f"{TEMPLATE_STORE_DIR}/Xbb_scan.png"

    elif tagger == "antiqcd":
        sig_yields, bkg_yields, scan_points = scan_aqcd(SCAN_CONFIG, YEAR)
        label = "Significance vs AntiQCD WP"
        output_plot = f"{TEMPLATE_STORE_DIR}/AntiQCD_scan.png"
    else:
        raise ValueError(f"Unknown tagger: {tagger}. Choices are 'xbb' or 'antiqcd'.")
    
    significances = []
    for sig, bkg in zip(sig_yields, bkg_yields):
        if bkg > 0:
            significance = sig / (bkg ** 0.5)
        else:
            significance = 0
        significances.append(significance)
    
    plot_scan(scan_points, significances, output_plot, label)