#2022 and 2023 need to be reprocessed by CMS so only few MC datasets are available. Until that is done, we will focus on 2024 only
mc_bkg = {
    "2022": {
        "TTbar": {
            "TTto4Q": "/TTto4Q_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer22NanoAODv15-150X_mcRun3_2022_realistic_v1-v2/NANOAODSIM",
            "TTtoLNu2Q": "/TTtoLNu2Q_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer22NanoAODv15-150X_mcRun3_2022_realistic_v1-v2/NANOAODSIM",
            #"TTto2L2Nu": "/TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer22NanoAODv15-150X_mcRun3_2022_realistic_v1-v2/NANOAODSIM"  # Possibly process never
        },
    },
    "2023": {
        "TTbar": {
            "TTto4Q": "/TTto4Q_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer23NanoAODv15-150X_mcRun3_2023_realistic_v1-v2/NANOAODSIM",
            "TTtoLNu2Q": "/TTtoLNu2Q_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer23NanoAODv15-150X_mcRun3_2023_realistic_v1-v2/NANOAODSIM",
            #"TTto2L2Nu": "/TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer23NanoAODv15-150X_mcRun3_2023_realistic_v1-v2/NANOAODSIM"
        },
    },
    "2024": {
        "TTbar": {
            "TTto4Q": "/TTto4Q_TuneCP5_13p6TeV_powheg-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "TTtoLNu2Q": "/TTtoLNu2Q_TuneCP5_13p6TeV_powheg-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            #"TTto2L2Nu": "/TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v3/NANOAODSIM"
        },
        "QCD": {
            #"QCD-4Jets_HT-100to200": "/QCD-4Jets_Bin-HT-100to200_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            #"QCD-4Jets_HT-200to400": "/QCD-4Jets_Bin-HT-200to400_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            #"QCD-4Jets_HT-400to600": "/QCD-4Jets_Bin-HT-400to600_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            #"QCD-4Jets_HT-600to800": "/QCD-4Jets_Bin-HT-600to800_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM", #These HT and lower do not contribute (check)
            "QCD-4Jets_HT-800to1000": "/QCD-4Jets_Bin-HT-800to1000_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "QCD-4Jets_HT-1000to1200": "/QCD-4Jets_Bin-HT-1000to1200_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "QCD-4Jets_HT-1200to1500": "/QCD-4Jets_Bin-HT-1200to1500_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "QCD-4Jets_HT-1500to2000": "/QCD-4Jets_Bin-HT-1500to2000_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "QCD-4Jets_HT-2000": "/QCD-4Jets_Bin-HT-2000_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM"
        }
    }
}

mc_sig = {
    "2022": {},
    "2023": {},
    "2024": {
        "XtoYHto4b" :{
            "MX1800_MY100": "/NMSSM-XtoYHto4B_Par-MX-1800-MY-100_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM"
        }
    }
}

jetmet = {
    "2022": {
        "JetMET": {
            "Run2022C": "/JetMET/Run2022C-NanoAODv15-v1/NANOAOD",
            "Run2022D": "/JetMET/Run2022D-NanoAODv15-v1/NANOAOD",
            "Run2022E": "/JetMET/Run2022E-NanoAODv15-v1/NANOAOD",
            "Run2022F": "/JetMET/Run2022F-NanoAODv15-v1/NANOAOD",
            "Run2022G": "/JetMET/Run2022G-NanoAODv15-v1/NANOAOD"
        }
    },
    "2023": {
        "JetMET": {
            "Run2023C": [
                "/JetMET0/Run2023C-NanoAODv15-v1/NANOAOD",
                "/JetMET0/Run2023C-NanoAODv15_v2-v1/NANOAOD",
                "/JetMET0/Run2023C-NanoAODv15_v3-v1/NANOAOD",
                "/JetMET0/Run2023C-NanoAODv15_v4-v1/NANOAOD",
                "/JetMET1/Run2023C-NanoAODv15-v1/NANOAOD",
                "/JetMET1/Run2023C-NanoAODv15_v2-v1/NANOAOD",
                "/JetMET1/Run2023C-NanoAODv15_v3-v1/NANOAOD",
                "/JetMET1/Run2023C-NanoAODv15_v4-v1/NANOAOD"
            ],
            "Run2023D": [
                "/JetMET0/Run2023D-NanoAODv15-v1/NANOAOD",
                "/JetMET0/Run2023D-NanoAODv15_v2-v1/NANOAOD",
                "/JetMET1/Run2023D-NanoAODv15-v1/NANOAOD",
                "/JetMET1/Run2023D-NanoAODv15_v2-v1/NANOAOD"
            ]
        }
    },
    "2024": {
        "JetMET": {
            "Run2024C": [
                "/JetMET0/Run2024C-MINIv6NANOv15-v1/NANOAOD",
                "/JetMET1/Run2024C-MINIv6NANOv15-v1/NANOAOD"
            ],
            "Run2024D": [
                "/JetMET0/Run2024D-MINIv6NANOv15-v1/NANOAOD",
                "/JetMET1/Run2024D-MINIv6NANOv15-v1/NANOAOD"
            ],
            "Run2024E": [
                "/JetMET0/Run2024E-MINIv6NANOv15-v1/NANOAOD",
                "/JetMET1/Run2024E-MINIv6NANOv15-v1/NANOAOD"
            ],
            "Run2024F": [
                "/JetMET0/Run2024F-MINIv6NANOv15-v2/NANOAOD",
                "/JetMET1/Run2024F-MINIv6NANOv15-v2/NANOAOD"
            ],
            "Run2024G": [
                "/JetMET0/Run2024G-MINIv6NANOv15-v2/NANOAOD",
                "/JetMET1/Run2024G-MINIv6NANOv15-v2/NANOAOD"
            ],
            "Run2024H": [
                "/JetMET0/Run2024H-MINIv6NANOv15-v2/NANOAOD",
                "/JetMET1/Run2024H-MINIv6NANOv15-v2/NANOAOD"
            ],
            "Run2024I": [
                "/JetMET0/Run2024I-MINIv6NANOv15-v2/NANOAOD",
                "/JetMET0/Run2024I-MINIv6NANOv15_v2-v1/NANOAOD",
                "/JetMET1/Run2024I-MINIv6NANOv15-v1/NANOAOD",
                "/JetMET1/Run2024I-MINIv6NANOv15_v2-v2/NANOAOD"
            ]
        }
    }
}