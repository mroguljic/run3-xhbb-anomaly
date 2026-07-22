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
 #Lower boundaries set so kinematics are more similar to pass / signal regions
    "2024": {
        "h_xbb_wp": 0.99,
        "y_antiqcd_wp": 0.90, 
        "h_xbb_wp_lo": 0.50,
        "y_antiqcd_wp_lo": 0.60, 
    }
}

# Region boundaries as (lower, upper) tuples. None means unbounded.
TEMPLATE_REGION_BOUNDARIES = {
    "2024": {
        "Pass": (TEMPLATE_TAGGING_WPS["2024"]["h_xbb_wp"], None),
        "Fail": (TEMPLATE_TAGGING_WPS["2024"]["h_xbb_wp_lo"], TEMPLATE_TAGGING_WPS["2024"]["h_xbb_wp"]),
        "Signal": (TEMPLATE_TAGGING_WPS["2024"]["y_antiqcd_wp"], None),
        "Control": (TEMPLATE_TAGGING_WPS["2024"]["y_antiqcd_wp_lo"], TEMPLATE_TAGGING_WPS["2024"]["y_antiqcd_wp"]), 
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

REFERENCE_TRIGGER = {
    "2024": "HLT_Mu50",
}