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
        "trigger_or": True,
        "h_cand_pt_min": 300,
        "y_cand_pt_min": 300,
    }
}

TEMPLATE_TAGGING_WPS = {
    "2024": {
        "h_xbb_wp": 0.98,
        "y_antiqcd_wp": 0.95,
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
        "HLT_AK8DiPFJet250_250_MassSD30",
    ]
}