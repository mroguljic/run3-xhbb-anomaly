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
            "QCD-4Jets_HT-400to600": "/QCD-4Jets_Bin-HT-400to600_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "QCD-4Jets_HT-600to800": "/QCD-4Jets_Bin-HT-600to800_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM", #These HT and lower do not contribute (check)
            "QCD-4Jets_HT-800to1000": "/QCD-4Jets_Bin-HT-800to1000_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "QCD-4Jets_HT-1000to1200": "/QCD-4Jets_Bin-HT-1000to1200_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "QCD-4Jets_HT-1200to1500": "/QCD-4Jets_Bin-HT-1200to1500_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "QCD-4Jets_HT-1500to2000": "/QCD-4Jets_Bin-HT-1500to2000_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "QCD-4Jets_HT-2000": "/QCD-4Jets_Bin-HT-2000_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM"
        },
        "WJets": {
            "Wto2Q-2Jets_HT-100": "/Wto2Q-2Jets_Bin-PTQQ-100_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v3/NANOAODSIM",
            "Wto2Q-2Jets_HT-200": "/Wto2Q-2Jets_Bin-PTQQ-200_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "Wto2Q-2Jets_HT-400": "/Wto2Q-2Jets_Bin-PTQQ-400_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v3/NANOAODSIM",
            "Wto2Q-2Jets_HT-600": "/Wto2Q-2Jets_Bin-PTQQ-600_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
        },
        "ZJets": {
            "Zto2Q-2Jets_HT-100": "/Zto2Q-2Jets_Bin-PTQQ-100_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "Zto2Q-2Jets_HT-200": "/Zto2Q-2Jets_Bin-PTQQ-200_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "Zto2Q-2Jets_HT-400": "/Zto2Q-2Jets_Bin-PTQQ-400_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "Zto2Q-2Jets_HT-600": "/Zto2Q-2Jets_Bin-PTQQ-600_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
        }
    }
}

# Run get_xhy_sigs.py to create entries automatically for existing signals
mc_sig = {
    "2022": {},
    "2023": {},
    "2024": {
        "XtoYHto4b" :{
            "MX1800_MY100": "/NMSSM-XtoYHto4B_Par-MX-1800-MY-100_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM"
        },
        "XtoYHto2W2Bto4Q2B": {
            "MX1200_MY500": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-1200-MY-500_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX1600_MY1000": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-1600-MY-1000_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX1600_MY1400": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-1600-MY-1400_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX1600_MY800": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-1600-MY-800_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX1800_MY1600": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-1800-MY-1600_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX1800_MY300": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-1800-MY-300_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX1800_MY600": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-1800-MY-600_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX2000_MY1600": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-2000-MY-1600_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX2000_MY1800": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-2000-MY-1800_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX2000_MY200": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-2000-MY-200_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX2000_MY500": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-2000-MY-500_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX2500_MY1000": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-2500-MY-1000_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX2500_MY1600": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-2500-MY-1600_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX2500_MY400": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-2500-MY-400_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX2500_MY500": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-2500-MY-500_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX2500_MY600": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-2500-MY-600_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX3000_MY200": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-3000-MY-200_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX3000_MY300": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-3000-MY-300_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX3500_MY2000": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-3500-MY-2000_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX3500_MY200": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-3500-MY-200_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX3500_MY300": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-3500-MY-300_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX3500_MY400": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-3500-MY-400_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX3500_MY800": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-3500-MY-800_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX4000_MY1800": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-4000-MY-1800_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX4000_MY800": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-4000-MY-800_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX4000_MY1400": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-4000-MY-1400_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX4000_MY2000": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-4000-MY-2000_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX550_MY300": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-550-MY-300_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX550_MY400": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-550-MY-400_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX600_MY400": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-600-MY-400_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX650_MY500": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-550-MY-300_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX800_MY400": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-800-MY-400_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX900_MY300": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-900-MY-300_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
            "MX900_MY600": "/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-900-MY-600_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM",
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
            "JetMET0_2023C_v1": "/JetMET0/Run2023C-NanoAODv15-v1/NANOAOD",
            "JetMET0_2023C_v2-v1": "/JetMET0/Run2023C-NanoAODv15_v2-v1/NANOAOD",
            "JetMET0_2023C_v3-v1": "/JetMET0/Run2023C-NanoAODv15_v3-v1/NANOAOD",
            "JetMET0_2023C_v4-v1": "/JetMET0/Run2023C-NanoAODv15_v4-v1/NANOAOD",
            "JetMET1_2023C_v1": "/JetMET1/Run2023C-NanoAODv15-v1/NANOAOD",
            "JetMET1_2023C_v2-v1": "/JetMET1/Run2023C-NanoAODv15_v2-v1/NANOAOD",
            "JetMET1_2023C_v3-v1": "/JetMET1/Run2023C-NanoAODv15_v3-v1/NANOAOD",
            "JetMET1_2023C_v4-v1": "/JetMET1/Run2023C-NanoAODv15_v4-v1/NANOAOD",
            "JetMET0_2023D_v1": "/JetMET0/Run2023D-NanoAODv15-v1/NANOAOD",
            "JetMET0_2023D_v2-v1": "/JetMET0/Run2023D-NanoAODv15_v2-v1/NANOAOD",
            "JetMET1_2023D_v1": "/JetMET1/Run2023D-NanoAODv15-v1/NANOAOD",
            "JetMET1_2023D_v2-v1": "/JetMET1/Run2023D-NanoAODv15_v2-v1/NANOAOD"
        }
    },
    "2024": {
        "JetMET": {
            "JetMET0_2024C_v1": "/JetMET0/Run2024C-MINIv6NANOv15-v1/NANOAOD",
            "JetMET1_2024C_v1": "/JetMET1/Run2024C-MINIv6NANOv15-v1/NANOAOD",
            "JetMET0_2024D_v1": "/JetMET0/Run2024D-MINIv6NANOv15-v1/NANOAOD",
            "JetMET1_2024D_v1": "/JetMET1/Run2024D-MINIv6NANOv15-v1/NANOAOD",
            "JetMET0_2024E_v1": "/JetMET0/Run2024E-MINIv6NANOv15-v1/NANOAOD",
            "JetMET1_2024E_v1": "/JetMET1/Run2024E-MINIv6NANOv15-v1/NANOAOD",
            "JetMET0_2024F_v2": "/JetMET0/Run2024F-MINIv6NANOv15-v2/NANOAOD",
            "JetMET1_2024F_v2": "/JetMET1/Run2024F-MINIv6NANOv15-v2/NANOAOD",
            "JetMET0_2024G_v2": "/JetMET0/Run2024G-MINIv6NANOv15-v2/NANOAOD",
            "JetMET1_2024G_v2": "/JetMET1/Run2024G-MINIv6NANOv15-v2/NANOAOD",
            "JetMET0_2024H_v2": "/JetMET0/Run2024H-MINIv6NANOv15-v2/NANOAOD",
            "JetMET1_2024H_v2": "/JetMET1/Run2024H-MINIv6NANOv15-v2/NANOAOD",
            "JetMET0_2024I_v2": "/JetMET0/Run2024I-MINIv6NANOv15-v2/NANOAOD",
            "JetMET0_2024I_v2-v1": "/JetMET0/Run2024I-MINIv6NANOv15_v2-v1/NANOAOD",
            "JetMET1_2024I_v1": "/JetMET1/Run2024I-MINIv6NANOv15-v1/NANOAOD",
            "JetMET1_2024I_v2-v2": "/JetMET1/Run2024I-MINIv6NANOv15_v2-v2/NANOAOD"
        }
    }
}