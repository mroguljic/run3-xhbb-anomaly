PRESELECTION_CUTS = {
    "2024": {
        "valid_fatjet_pt_min": 300,
        "valid_fatjet_abs_eta_max": 2.4,
        "valid_fatjet_mass_min": 40,
    }
}


REGION_CUTS = {
    "2024": {
        "m_jj_min": 900,
    }
}

TEMPLATE_SELECTION = {
    "2024": {
        "h_cand_mass_min": 100,
        "h_cand_mass_max": 140,
        "y_cand_mass_min": 40,
        "h_cand_pt_min": 300,
        "y_cand_pt_min": 300,
    }
}

TEMPLATE_TAGGING_WPS = {
    "2024": {
        "h_xbb_wp": 0.99,
        "y_antiqcd_wp": 0.6,
    }
}

# Region boundaries as (lower, upper) tuples. None means unbounded.
TEMPLATE_REGION_BOUNDARIES = {
    "2024": {
        "Pass": (0.99, None),        # h_cand_xbb > 0.99
        "Fail": (0.5, 0.99),         # 0.5 < h_cand_xbb <= 0.99
        "Signal": (0.6, None),       # y_cand_antiqcd > 0.6
        "Control": (0.2, 0.6),       # 0.2 < y_cand_antiqcd <= 0.6
    }
}

TEMPLATE_REGIONS = {
    "2024": [
        ("Pass", "Signal"),
        ("Pass", "Control"),
        ("Fail", "Signal"),
        ("Fail", "Control"),
    ]
}

triggers = {
    "2024": [
        "HLT_AK8DiPFJet250_250_SoftDropMass40",
        "HLT_AK8PFJet250_SoftDropMass40_PNetBB0p06",
    ]
}