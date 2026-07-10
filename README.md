# run3-xhbb-anomaly

CMS Run 3 physics analysis for boosted X->H(bb)Y(anomalous jet) anomaly search.
Built on [TIMBER](https://gitlab.cern.ch/jhu-tools/TIMBER).

## Environment

The idea is to run commands inside this repo's singularity container.
One can define `anomaly_exec` command to help with that.
For example, on lpc, add this to .bashrc.
```
anomaly_exec() {
    singularity exec \
        --bind "$(readlink $HOME)" \
        --bind "$(readlink -f ${HOME}/nobackup/)" \
        --bind /uscms_data \
        --bind /etc/grid-security/certificates \
        --bind /cvmfs \
        /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/mrogulji/run3-xhbb-anomaly:master/ \
        python3 "$@"
}
```

Commands are run from the root repo directory.

## 1. Preselection (stage 1: skim)

Runs on raw NanoAOD; applies lumi mask (data only), MET filters, jet
ID/JEC/JER/veto-map corrections, and writes a flat skim + cutflow.

```bash
anomaly_exec preselection.py -i filelists/test_files/XToYHto4b_MX1800_MY100_testfile.root -y 2024 -o output_presel_signal.root
```

## 2. Templating (stage 2: selection + templates)

Runs on a stage-1 skim; applies trigger/pT/mass cuts, computes MC systematic
weights, splits events into Pass/Fail x Signal/Control regions, and writes 2D
template histograms.

```bash
anomaly_exec selection_and_templating.py -i output_presel_signal.root -y 2024 -o templates_signal.root
```

## 3. HTCondor batch production (`condor/`)

`controller.py` orchestrates the full batch pipeline end-to-end (manifest
generation, submission, output validation, template merging) and is
idempotent — it reuses valid artifacts and only regenerates what's missing.

```bash
# Edit campaign, store path, etc. in condor/config.py
# Then run the controller and follow the instructions for input
anomaly_exec condor/controller.py --year 2024 --all-datasets

# If bookkeeping .json files need to be recalculated
anomaly_exec condor/controller.py --year 2024 --all-datasets --force-regen
```

Individual pipeline stages (`generate_batches.py`, `generate_submission.py`,
`check_skim_outputs.py`, `generate_template_manifest.py`,
`check_template_outputs.py`, `merge_templates.py`) can also be run standalone
— see `condor/test_commands.txt` for the full sequence.

Merged, cross-section-scaled templates land in
`condor/output/templates/merged/templates_<process>.root`.

## 4. Plotting (`plotting/`)

Reads the merged template files and produces stacked data/MC/signal
comparison plots for the histograms configured in `plotting/config.py`'s
`HISTOGRAMS_TO_PLOT` (inclusive kinematics plus Pass/Fail x Signal/Control
`m_jj`/`m_jY`, with the Signal-region ones blinded).

```bash
anomaly_exec plotting/cli.py --year 2024 --input-dir condor/output/templates/merged
```

Useful variants:

```bash
# custom output location (default: output/plots)
anomaly_exec plotting/cli.py --year 2024 --input-dir condor/output/templates/merged --output-dir output/plots/2024

# only specific processes
anomaly_exec plotting/cli.py --year 2024 --input-dir condor/output/templates/merged --processes QCD TT data
```

## 5. Tagger studies (`tagger_studies/`)

A standalone workflow for scanning Xbb/anti-QCD working points and computing
significance/yield tradeoffs directly from already-produced template ROOT
files. Config (process-to-dataset mapping, scan grid, signal-region
integration bins) lives in `tagger_studies/config.py`.

```bash
# First edit settings in tagger_studies/config.py
# Run the working-point scan (produces per-process templates in
# tagger_studies/templates/ if they don't already exist, then optimizes
# the Xbb and anti-QCD working points)
anomaly_exec tagger_studies/optimize_taggers.py

# ROC curves for both taggers from the merged templates
anomaly_exec tagger_studies/roc.py --input-dir condor/output/templates/merged --output-dir tagger_studies/roc
```