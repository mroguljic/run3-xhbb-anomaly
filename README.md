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

`generate_submission.py` and `condor_submit` must be run from inside `condor/`
— `controller.py` always does this itself, but if you're running these steps
by hand, note the generated `.sub` file's `executable = template_wrapper.sh`
line is a bare filename, resolved relative to wherever `condor_submit` is
invoked from, not to where the `.sub` file lives. Running it from the repo
root fails with `ERROR: Executable file template_wrapper.sh does not exist`:

```bash
cd condor
anomaly_exec generate_submission.py --manifest template_manifest_2024.json --output template_submission_2024.sub
condor_submit template_submission_2024.sub
```

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
significance directly from the joint (m_jj, m_jY, Xbb, anti-QCD) THnD
(`inclusive_h_xbb_vs_y_antiqcd`) that `selection_and_templating.py` books per
process, read straight out of the already-merged template ROOT files -- no
per-WP re-templating needed. Config (background processes, the list of
signals to scan, window-finding target fraction) lives in
`tagger_studies/config.py`; edit its `SIGNALS` list to control which signal
points get scanned.

For each signal, the scan derives that signal's own (m_jj, m_jY) integration
window from its kinematic peak (fixed-fraction contour around the peak bin),
skips signals whose window doesn't contain their own (MX, MY) -- a sign the Y
candidate isn't merging into a single AK8 jet at that mass ratio -- then
computes the Asimov significance Z = sqrt(2*((S+B)*ln(1+S/B) - S)) over every
(Xbb, anti-QCD) bin-boundary combination the THnD resolves (B summed over
`config.BACKGROUND_PROCESSES`).

```bash
# First edit tagger_studies/config.py's SIGNALS list to pick which signals to scan
anomaly_exec tagger_studies/significance_scan.py

# Or restrict to specific signals ad hoc, without touching config.py
anomaly_exec tagger_studies/significance_scan.py --signals MX1800_MY100 MX3000_MY300

# ROC curves for both taggers from the merged templates
anomaly_exec tagger_studies/roc.py --input-dir condor/output/templates/merged --output-dir tagger_studies/roc
```

Output per signal, under `tagger_studies/scans/<signal>/`:
- `significance.png`/`.pdf` -- Asimov significance heatmap over the full
  (Xbb WP, anti-QCD WP) grid, with the best point marked
- `mjj_dist.png`/`.pdf`, `mjy_dist.png`/`.pdf` -- stacked background + signal
  mass distributions at the best WP, restricted to the *other* axis' window
  (i.e. exactly the selection that fed the significance calculation)
- `mjj_dist_full.png`/`.pdf`, `mjy_dist_full.png`/`.pdf` -- the same
  projections with no cut on the other axis (WP cut only)

`tagger_studies/scans/summary.csv` collects the best WP/window/significance
across all scanned signals.