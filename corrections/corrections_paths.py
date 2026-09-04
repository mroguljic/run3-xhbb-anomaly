import os


corrections = {
    "2024" : {
        "JEC_AK8":    "/cvmfs/cms-griddata.cern.ch/cat/metadata/JME/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/2026-07-16/fatJet_jerc.json.gz",
        "JEC_AK4":    "/cvmfs/cms-griddata.cern.ch/cat/metadata/JME/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/2026-07-16/jet_jerc.json.gz",
        "JetID":      "/cvmfs/cms-griddata.cern.ch/cat/metadata/JME/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/2026-07-16/jetid.json.gz",
        "JetVetoMap": "/cvmfs/cms-griddata.cern.ch/cat/metadata/JME/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/2026-07-16/jetvetomaps.json.gz",
        "Pileup":     "/cvmfs/cms-griddata.cern.ch/cat/metadata/LUM/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/2026-04-15/puWeights_BCDEFGHI.json.gz"
    },
    "2025" : {
        "JEC_AK8":    "/cvmfs/cms-griddata.cern.ch/cat/metadata/JME/Run3-25Prompt-Summer24-NanoAODv15/2026-07-16/fatJet_jerc.json.gz",
        "JEC_AK4":    "/cvmfs/cms-griddata.cern.ch/cat/metadata/JME/Run3-25Prompt-Summer24-NanoAODv15/2026-07-16/jet_jerc.json.gz",
        "JetID":      "/cvmfs/cms-griddata.cern.ch/cat/metadata/JME/Run3-25Prompt-Summer24-NanoAODv15/2026-07-16/jetid.json.gz",
        "JetVetoMap": "/cvmfs/cms-griddata.cern.ch/cat/metadata/JME/Run3-25Prompt-Summer24-NanoAODv15/2026-07-16/jetvetomaps.json.gz",
        "Pileup":     "/cvmfs/cms-griddata.cern.ch/cat/metadata/LUM/Run3-25Prompt-Summer24-NanoAODv15/2026-06-05/puWeights_2025pp_Golden_Summer24_25ns_69200ub.json.gz"
    }
}


def get_correction_path(year: str, correction: str) -> str:
    if year not in corrections:
        raise ValueError(f"No corrections configured for year '{year}'. Available years: {sorted(corrections)}")
    if correction not in corrections[year]:
        raise ValueError(f"No '{correction}' correction configured for {year}. Available: {sorted(corrections[year])}")

    path = corrections[year][correction]
    if not os.path.exists(path):
        raise FileNotFoundError(f"{correction} correction file for {year} not found: {path}")
    return path
