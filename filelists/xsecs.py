# Cross sections in pb
# From xsdb unless otherwise specified

# Inclusive: https://twiki.cern.ch/twiki/bin/view/LHCPhysics/TtbarNNLO#Updated_reference_cross_sections
# W BR: https://doi.org/10.1103/PhysRevD.105.072008 is very close to 2/3 hadronic, 1/3 leptonic
inclusive_ttbar_xsec = 923.6
br_w_qq = 67.3 / 100.
br_w_lnu = 1. - br_w_qq
br_ttbar_4q = br_w_qq**2
br_ttbar_lnu2q = 2*br_w_qq*br_w_lnu
br_ttbar_2l2nu = br_w_lnu**2


xsecs = {
    "TTto4Q": inclusive_ttbar_xsec * br_ttbar_4q,
    "TTtoLNu2Q": inclusive_ttbar_xsec * br_ttbar_lnu2q,
    "TTto2L2Nu": inclusive_ttbar_xsec * br_ttbar_2l2nu,
    "QCD-4Jets_HT-800to1000": 3010.,
    "QCD-4Jets_HT-1000to1200": 890.3,
    "QCD-4Jets_HT-1200to1500": 384.8,
    "QCD-4Jets_HT-1500to2000": 127.3.,
    "QCD-4Jets_HT-2000": 26.26,
}

int_lumi = {
    # brilcalc lumi --normtag /cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json -u /fb -i <golde_json>
    "2024": 109987.998903 
}

def get_xsec(process: str) -> float:
    """
    Get cross section for a given process.
    
    Args:
        process (str): Process name, e.g. "TTTo4Q"
    
    Returns:
        float: Cross section in pb
    """
    if process not in xsecs:
        if(process.startswith("MX")):
            return 0.001 # 1fb for signal samples
        raise ValueError(f"{process} is not listed in xsecs.py. Available processes: {list(xsecs.keys())}. Signal processes should start with 'MX'.")
    return xsecs[process]

def get_int_lumi(year: str) -> float:
    """
    Get integrated luminosity for a given year.
    
    Args:
        year (str): Year, e.g. "2022"
    
    Returns:
        float: Integrated luminosity in /pb
    """
    if year not in int_lumi:
        raise ValueError(f"Int. lumi for {year} is not listed in xsecs.py. Available years: {list(int_lumi.keys())}")
    return int_lumi[year]