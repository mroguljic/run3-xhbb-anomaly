import copy

PRESELECTION_CUTS = {
    "2024": {
        "valid_fatjet_pt_min": 300,
        "valid_fatjet_abs_eta_max": 2.4,
        "valid_fatjet_mass_min": 40,
        "m_jj_skim_min": 800, # Looser than final cut, to allow studying if a lower cut could work
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

#Apply same selection in 2025 as in 2024
for config in [PRESELECTION_CUTS, REGION_CUTS, TEMPLATE_SELECTION, TEMPLATE_TAGGING_WPS, TEMPLATE_REGIONS, triggers, REFERENCE_TRIGGER]:
    config["2025"] = copy.deepcopy(config["2024"])


TEMPLATE_REGION_BOUNDARIES = {
    year: {
        "Pass":    (wps["h_xbb_wp"], None),
        "Fail":    (wps["h_xbb_wp_lo"], wps["h_xbb_wp"]),
        "Signal":  (wps["y_antiqcd_wp"], None),
        "Control": (wps["y_antiqcd_wp_lo"], wps["y_antiqcd_wp"]),
    }
    for year, wps in TEMPLATE_TAGGING_WPS.items()
}