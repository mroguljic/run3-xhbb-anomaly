'''
Create the entries for existing MC signals for Nano_v15.py
'''
import subprocess

MX = [300, 400, 500, 550, 600, 650, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000, 3500, 4000]
MY = [60, 70, 80, 90, 95, 100, 125, 150, 200, 300, 400, 500, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2600, 3000, 3500]

# <short name> : [ <dataset template from DAS>, {empty dict} ]
sigs = {
    "XtoYHto2W2Bto4Q2B": ["/NMSSM-XtoYHto2W2Bto4Q2B_Par-MX-%s-MY-%s_TuneCP5_13p6TeV_madgraph-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM", {}]
}

# Get the signals that exist
for sig, d in sigs.items():
    subdict = sigs[sig][-1]
    substr  = sigs[sig][0]
    for mx in MX:
        for my in MY:
            if mx > my + 125:
                result = subprocess.run(['dasgoclient', '-query', f'file dataset={substr%(mx,my)}'], capture_output=True, text=True)
                if result.stdout: 
                    print(f'Found signal {sig}: ({mx}, {my}) ----------------------------------------------------------------')
                    sigs[sig][-1][f'MX{mx}_MY{my}'] = substr%(mx,my)
                else:
                    # Try v2-v3, some of them seem to be this? 
                    ss = substr.replace("v2-v2","v2-v3")
                    result = subprocess.run(['dasgoclient', '-query', f'file dataset={ss%(mx,my)}'], capture_output=True, text=True)
                    if result.stdout: 
                        print(f'Found signal {sig}: ({mx}, {my}) [v2-v3] ----------------------------------------------------------------')
                        sigs[sig][-1][f'MX{mx}_MY{my}'] = substr%(mx,my)
            else:
                continue

    # Write out the file list that you can copy into Nano_v15.py from stdout
    tab = '    '
    preamble = '%s"%s": {'%(tab*2, sig)
    files    = [f'{tab*3}"{mxmy}": "{dataset}",' for mxmy, dataset in sigs[sig][-1].items()]
    closing  = '%s},'%(tab*2)
    print(preamble)
    for f in files: print(f)
    print(closing)
