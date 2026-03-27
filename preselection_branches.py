from typing import Dict, List


COMMON_SNAPSHOT_COLUMNS: list[str] = [
    "run",
    "luminosityBlock",
    "event",
    "genWeight",
    "nFatJet",
    "valid_fatjet_indices",
    "m_jj*",
    "n_valid_fatjets",
    "nFatJet",
    "FatJet_eta",
    "FatJet_phi",
    "FatJet_globalParT3_QCD",
    "FatJet_globalParT3_XWW4q",
    "FatJet_globalParT3_Xbb",
    "FatJet_regressed_mass",
    "FatJet_mass",
    "FatJet_msoftdrop",
    "FatJet_phi",
    "FatJet_pt",
    "h_cand*",
    "y_cand*",
    "HLT_AK8DiPFJet250_250_SoftDropMass40",
    "HLT_AK8PFJet250_SoftDropMass40_PNetBB0p06",
    "HLT_AK8DiPFJet250_250_MassSD30"
]


YEAR_EXTRA_COLUMNS: Dict[str, List[str]] = {
    "2024": [],
}


def get_preselection_snapshot_columns(year: str, data_flag: bool = False) -> list[str]:
    """Return the flat preselection snapshot branch list for a given year.

    Args:
        year (str): Data-taking year string, for example ``"2024"``.
        data_flag (bool, optional): Whether the sample is data. If True,
            branches unavailable in data (for example ``genWeight``) are removed.

    Returns:
        list[str]: Ordered flat list of branch names to snapshot.

    Raises:
        ValueError: If ``year`` is not supported.
    """
    if year not in YEAR_EXTRA_COLUMNS:
        supported_years = ", ".join(sorted(YEAR_EXTRA_COLUMNS.keys()))
        raise ValueError(f"Unsupported year '{year}'. Supported years: {supported_years}")

    columns = COMMON_SNAPSHOT_COLUMNS + YEAR_EXTRA_COLUMNS[year]
    if data_flag:
        columns = [column for column in columns if column != "genWeight"]
    return columns