#!/usr/bin/env python3
"""
Merge template chunk outputs per process.

Reads a template manifest and produces one merged template ROOT file per process:
  - If a process has one valid chunk file, copy it to the merged destination.
  - If a process has multiple valid chunk files, merge them with hadd.

Output destination is derived from condor config:
  OUTPUT["templates_dir"]/<process>/<merged_filename>

Examples:
    python3 merge_templates.py --manifest template_manifest_2024.json

    python3 merge_templates.py --manifest template_manifest_2024.json \
        --processes TTto4Q QCD-4Jets_HT-800to1000

    python3 merge_templates.py --manifest template_manifest_2024.json --dry-run
"""

import argparse
import fnmatch
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.check_template_outputs import stat_eos_file, store_path_from_eos_url
from condor.config import EOS_MKDIR, LOCAL_MERGED_TEMPLATES_DIR, OUTPUT, get_store_eos_path
from filelists.xsecs import get_int_lumi, get_xsec
import ROOT

DEFAULT_GROUP_RULES = [
    "Run2024=Run2024*",
    "QCD=QCD*",
    "TT=TT*",
]


@dataclass
class ProcessMergeResult:
    """Result of merging template chunks for a single process."""

    process: str
    output_path: str
    merge_level: str = "process"
    all_inputs: List[str] = field(default_factory=list)
    valid_inputs: List[str] = field(default_factory=list)
    invalid_inputs: List[str] = field(default_factory=list)
    n_inputs_total: int = 0
    n_inputs_valid: int = 0
    action: str = "none"
    status: str = "pending"
    local_output_path: Optional[str] = None
    scale_factor: Optional[float] = None
    error: Optional[str] = None


@dataclass
class MergeSummary:
    """Aggregated merge results over all processes."""

    manifest_path: str
    total_processes: int = 0
    n_merged: int = 0
    n_copied: int = 0
    n_skipped: int = 0
    n_failed: int = 0
    results: Dict[str, ProcessMergeResult] = field(default_factory=dict)

    def add(self, result: ProcessMergeResult) -> None:
        """Add one process result and update counters."""
        result_key = f"{result.merge_level}:{result.process}"
        self.results[result_key] = result
        self.total_processes += 1

        if result.status == "ok" and result.action == "merge":
            self.n_merged += 1
        elif result.status == "ok" and result.action == "copy":
            self.n_copied += 1
        elif result.status.startswith("skipped"):
            self.n_skipped += 1
        elif result.status == "failed":
            self.n_failed += 1


def eos_file_exists(eos_path: str) -> bool:
    """Return True if eos_path exists according to xrdfs stat."""
    store_path = store_path_from_eos_url(eos_path)
    exists, _, _ = stat_eos_file(store_path)
    return exists


def ensure_eos_directory(output_eos_path: str) -> None:
    """Create EOS parent directory for output if missing."""
    store_path = store_path_from_eos_url(output_eos_path)
    parent_dir = str(Path(store_path).parent)
    command = f"{EOS_MKDIR} -p {parent_dir}"

    completed = subprocess.run(command, shell=True, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "Failed to create EOS directory")


def run_xrdcp(source: str, destination: str) -> None:
    """Copy one file with xrdcp -f."""
    command = ["xrdcp", "-f", source, destination]
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "xrdcp failed")


def run_hadd(output_file: str, inputs: List[str]) -> None:
    """Merge ROOT files with hadd."""
    command = ["hadd", "-f", output_file, *inputs]
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "hadd failed")


def is_data_process(process: str) -> bool:
    """Return True for data process names like Run2024C."""
    return process.startswith("Run20")


def get_sum_gen_weight(template_file_path: str) -> float:
    """Read the first bin of template_cutflow from a local ROOT file."""

    root_file = ROOT.TFile.Open(template_file_path, "READ")
    if not root_file or root_file.IsZombie():
        raise RuntimeError(f"Could not open ROOT file '{template_file_path}' to read template_cutflow")

    template_cutflow = root_file.Get("template_cutflow")
    if template_cutflow is None:
        root_file.Close()
        raise RuntimeError(f"Histogram 'template_cutflow' not found in '{template_file_path}'")

    sum_gen_weight = float(template_cutflow.GetBinContent(1))
    root_file.Close()

    if sum_gen_weight == 0.0:
        raise RuntimeError(f"template_cutflow bin 1 is zero in '{template_file_path}'")

    return sum_gen_weight


def scale_histograms_in_file(input_root_file: str, output_root_file: str, scale_factor: float) -> int:
    """Copy objects into a new ROOT file and scale TH1-derived objects (no subdirectories assumed)."""
    scaled_histograms = 0
    input_tfile = ROOT.TFile.Open(input_root_file, "READ")
    key_list = input_tfile.GetListOfKeys()
    histos = []

    for i in range(len(key_list)):
        h = input_tfile.Get(key_list[i].GetName())
        if h.InheritsFrom("TH1"):
            h.Scale(scale_factor)
            h.SetDirectory(0)
            histos.append(h)
            scaled_histograms += 1
        else:
            # As it stands, we should only have histograms in the file, but this allows copying other objects
            histos.append(h)
    input_tfile.Close()

    output_tfile = ROOT.TFile.Open(output_root_file, "RECREATE")
    output_tfile.cd()
    for h in histos:
        h.Write()
    output_tfile.Close()
    return scaled_histograms


def scale_merged_mc_template(template_file_path: str, process: str, year: str) -> float:
    """Scale a merged MC template file via a new temp ROOT file and return the applied factor."""
    sum_gen_weight = get_sum_gen_weight(template_file_path)
    int_lumi = get_int_lumi(year)
    xsec = get_xsec(process)
    scale_factor = int_lumi * xsec / sum_gen_weight

    ROOT.gROOT.SetBatch(True)
    temp_scaled_path: Optional[str] = None

    with tempfile.NamedTemporaryFile(
        prefix=f"scaled_{process}_",
        suffix=".root",
        dir=str(Path(template_file_path).parent),
        delete=False,
    ) as temp_file_handle:
        temp_scaled_path = temp_file_handle.name


    scaled_histograms = scale_histograms_in_file(template_file_path, temp_scaled_path, scale_factor)


    if scaled_histograms == 0:
        if temp_scaled_path and os.path.exists(temp_scaled_path):
            os.remove(temp_scaled_path)
        raise RuntimeError(f"No histograms found to scale in '{template_file_path}'")

    os.replace(temp_scaled_path, template_file_path)

    return scale_factor


def get_local_output_path(process: str, output_name_template: str) -> str:
    """Return the local merged template destination for a process or group."""
    return str(Path(LOCAL_MERGED_TEMPLATES_DIR) / output_name_template.format(process=process))


def copy_output_to_local(output_eos_path: str, local_output_path: str) -> None:
    """Copy one merged EOS output to the configured local results directory."""
    os.makedirs(str(Path(local_output_path).parent), exist_ok=True)
    run_xrdcp(output_eos_path, local_output_path)


def build_process_inputs(manifest: dict) -> Dict[str, List[str]]:
    """Collect template chunk output paths grouped by process."""
    grouped: Dict[str, List[str]] = {}

    for dataset_key, dataset_info in manifest.get("datasets", {}).items():
        process = dataset_info.get("process", dataset_key)
        for batch in dataset_info.get("batches", {}).values():
            output_path = batch.get("output_path")
            if not output_path:
                continue
            grouped.setdefault(process, []).append(output_path)

    for process, inputs in grouped.items():
        unique_inputs = sorted(set(inputs))
        grouped[process] = unique_inputs

    return grouped


def parse_group_rules(group_rules: Optional[List[str]]) -> Dict[str, List[str]]:
    """Parse group rules like TARGET=PATTERN1,PATTERN2 into a mapping."""
    parsed: Dict[str, List[str]] = {}
    if not group_rules:
        return parsed

    for rule in group_rules:
        if "=" not in rule:
            raise ValueError(f"Invalid group rule '{rule}'. Expected TARGET=PATTERN[,PATTERN...]")
        target, patterns_raw = rule.split("=", 1)
        target = target.strip()
        patterns = [pattern.strip() for pattern in patterns_raw.split(",") if pattern.strip()]

        if not target:
            raise ValueError(f"Invalid group rule '{rule}': empty target")
        if not patterns:
            raise ValueError(f"Invalid group rule '{rule}': no patterns")

        parsed[target] = patterns

    return parsed


def build_group_inputs(
    process_inputs: Dict[str, List[str]],
    group_rule_map: Dict[str, List[str]],
    selected_processes: Optional[List[str]] = None,
) -> Dict[str, List[str]]:
    """Build grouped input file lists according to wildcard rules."""
    available_processes = set(process_inputs.keys())
    scope = set(selected_processes) if selected_processes else available_processes

    grouped: Dict[str, List[str]] = {}
    for target, patterns in group_rule_map.items():
        collected: List[str] = []
        matched = False

        for process in sorted(scope):
            if any(fnmatch.fnmatch(process, pattern) for pattern in patterns):
                matched = True
                collected.extend(process_inputs.get(process, []))

        if matched:
            grouped[target] = sorted(set(collected))

    return grouped


def merge_single_process(
    process: str,
    inputs: List[str],
    output_name_template: str,
    year: Optional[str] = None,
    apply_scale: bool = False,
    copy_local: bool = True,
    dry_run: bool = False,
    overwrite: bool = False,
) -> ProcessMergeResult:
    """Merge or copy template chunks for one process."""
    output_store_path = f"{OUTPUT['templates_dir']}/{process}/{output_name_template.format(process=process)}"
    output_eos_path = get_store_eos_path(output_store_path)
    local_output_path = get_local_output_path(process, output_name_template) if copy_local else None

    result = ProcessMergeResult(
        process=process,
        output_path=output_eos_path,
        merge_level="process",
        all_inputs=inputs,
        local_output_path=local_output_path,
        n_inputs_total=len(inputs),
    )

    for path in inputs:
        store_path = store_path_from_eos_url(path)
        exists, size_bytes, _ = stat_eos_file(store_path)
        if exists and size_bytes > 0:
            result.valid_inputs.append(path)
        else:
            result.invalid_inputs.append(path)

    result.n_inputs_valid = len(result.valid_inputs)

    if result.n_inputs_valid == 0:
        result.status = "failed"
        result.error = "No valid input files found"
        return result

    if eos_file_exists(output_eos_path) and not overwrite:
        if copy_local and local_output_path and not dry_run:
            try:
                copy_output_to_local(output_eos_path, local_output_path)
            except Exception as exc:
                result.status = "failed"
                result.error = f"Existing merged output found, but local copy failed: {exc}"
                return result
        result.status = "skipped-existing"
        result.action = "skip"
        return result

    if dry_run:
        result.status = "skipped-dry-run"
        result.action = "copy" if result.n_inputs_valid == 1 else "merge"
        return result

    try:
        ensure_eos_directory(output_eos_path)

        with tempfile.TemporaryDirectory(prefix=f"merge_{process}_") as temp_dir:
            local_output = str(Path(temp_dir) / output_name_template.format(process=process))

            if result.n_inputs_valid == 1:
                run_xrdcp(result.valid_inputs[0], local_output)
            else:
                run_hadd(local_output, result.valid_inputs)

            if apply_scale:
                if year is None:
                    raise RuntimeError("Year is required when scaling MC templates")
                result.scale_factor = scale_merged_mc_template(local_output, process, year)

            run_xrdcp(local_output, output_eos_path)
            if copy_local and local_output_path:
                copy_output_to_local(output_eos_path, local_output_path)

        result.action = "copy" if result.n_inputs_valid == 1 else "merge"
        result.status = "ok"
        return result

    except Exception as exc:
        result.status = "failed"
        result.error = str(exc)
        return result


def merge_all_processes(
    manifest: dict,
    processes: Optional[List[str]] = None,
    output_name_template: str = "templates_{process}.root",
    merge_groups: bool = False,
    group_rules: Optional[List[str]] = None,
    group_output_name_template: str = "templates_{process}.root",
    dry_run: bool = False,
    overwrite: bool = False,
    verbose: bool = True,
) -> MergeSummary:
    """Merge template chunks for all (or selected) processes and optional groups."""
    grouped_inputs = build_process_inputs(manifest)
    manifest_path = manifest.get("_source_path", "unknown")
    year = str(manifest.get("year", "unknown"))
    summary = MergeSummary(manifest_path=manifest_path)
    merged_process_outputs: Dict[str, List[str]] = {}

    requested = sorted(grouped_inputs.keys())
    if processes:
        unknown = [proc for proc in processes if proc not in grouped_inputs]
        if unknown:
            raise ValueError(f"Unknown process(es): {', '.join(unknown)}")
        requested = processes

    for process in requested:
        inputs = grouped_inputs.get(process, [])
        result = merge_single_process(
            process=process,
            inputs=inputs,
            output_name_template=output_name_template,
            year=year,
            apply_scale=not is_data_process(process),
            copy_local=True,
            dry_run=dry_run,
            overwrite=overwrite,
        )
        summary.add(result)

        if result.status != "failed":
            merged_process_outputs[process] = [result.output_path]

        if verbose:
            if result.status == "ok":
                scale_note = f" | scale={result.scale_factor:.8g}" if result.scale_factor is not None else ""
                local_note = f" | local={result.local_output_path}" if result.local_output_path else ""
                print(f"  [OK] {process}: {result.action} -> {result.output_path}{local_note}{scale_note}")
            elif result.status.startswith("skipped"):
                local_note = f" | local={result.local_output_path}" if result.local_output_path else ""
                print(f"  [SKIP] {process}: {result.status} ({result.n_inputs_valid} valid inputs){local_note}")
            else:
                print(f"  [FAIL] {process}: {result.error}")

    if merge_groups:
        parsed_rules = parse_group_rules(group_rules)
        grouped_targets = build_group_inputs(
            process_inputs=merged_process_outputs,
            group_rule_map=parsed_rules,
            selected_processes=requested,
        )

        if verbose:
            print("\nGrouped merges:")

        for group_name, inputs in grouped_targets.items():
            result = merge_single_process(
                process=group_name,
                inputs=inputs,
                output_name_template=group_output_name_template,
                year=year,
                apply_scale=False,
                copy_local=True,
                dry_run=dry_run,
                overwrite=overwrite,
            )
            result.merge_level = "group"
            summary.add(result)

            if verbose:
                if result.status == "ok":
                    local_note = f" | local={result.local_output_path}" if result.local_output_path else ""
                    print(f"  [OK] {group_name}: {result.action} -> {result.output_path}{local_note}")
                elif result.status.startswith("skipped"):
                    local_note = f" | local={result.local_output_path}" if result.local_output_path else ""
                    print(f"  [SKIP] {group_name}: {result.status} ({result.n_inputs_valid} valid inputs){local_note}")
                else:
                    print(f"  [FAIL] {group_name}: {result.error}")

    return summary


def main() -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Merge per-chunk template outputs into one file per process",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 merge_templates.py --manifest template_manifest_2024.json
  python3 merge_templates.py --manifest template_manifest_2024.json --dry-run
  python3 merge_templates.py --manifest template_manifest_2024.json --processes TTto4Q
    python3 merge_templates.py --manifest template_manifest_2024.json --merge-groups
    python3 merge_templates.py --manifest template_manifest_2024.json --merge-groups --group-rule Run2024=Run2024* --group-rule TT=TT*
        """,
    )
    parser.add_argument("--manifest", required=True, help="Path to template manifest JSON")
    parser.add_argument("--processes", nargs="+", default=None, help="Only merge these processes")
    parser.add_argument(
        "--output-name-template",
        default="templates_{process}.root",
        help="Merged filename template (default: templates_{process}.root)",
    )
    parser.add_argument(
        "--merge-groups",
        action="store_true",
        help="Also produce grouped merges using wildcard rules (defaults: Run2024, QCD, TT)",
    )
    parser.add_argument(
        "--group-rule",
        action="append",
        default=None,
        help="Grouping rule TARGET=PATTERN[,PATTERN...] (repeatable), e.g. Run2024=Run2024*",
    )
    parser.add_argument(
        "--group-output-name-template",
        default="templates_{process}.root",
        help="Grouped merged filename template (default: templates_{process}.root)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing outputs")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite merged outputs if they already exist")
    parser.add_argument("--report", default=None, help="Optional path to write JSON summary report")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-process output")

    args = parser.parse_args()

    if not args.dry_run and shutil.which("xrdcp") is None:
        print("ERROR: xrdcp command not found in PATH")
        return 1

    if not args.dry_run and shutil.which("hadd") is None:
        print("ERROR: hadd command not found in PATH")
        return 1

    try:
        with open(args.manifest) as file_handle:
            manifest = json.load(file_handle)
        manifest["_source_path"] = args.manifest
    except FileNotFoundError:
        print(f"ERROR: manifest not found: {args.manifest}")
        return 1
    except json.JSONDecodeError as exc:
        print(f"ERROR: failed to parse manifest: {exc}")
        return 1

    print("=" * 80)
    print("Template Merge")
    print("=" * 80)
    print(f"Manifest:   {args.manifest}")
    print(f"Campaign:   {manifest.get('campaign', 'unknown')}")
    print(f"Year:       {manifest.get('year', 'unknown')}")
    print(f"Dry run:    {'yes' if args.dry_run else 'no'}")
    print(f"Overwrite:  {'yes' if args.overwrite else 'no'}")
    if args.merge_groups:
        active_group_rules = args.group_rule if args.group_rule else DEFAULT_GROUP_RULES
        print(f"Group merge: yes ({'; '.join(active_group_rules)})")
    else:
        print("Group merge: no")
    print("=" * 80 + "\n")

    try:
        summary = merge_all_processes(
            manifest=manifest,
            processes=args.processes,
            output_name_template=args.output_name_template,
            merge_groups=args.merge_groups,
            group_rules=args.group_rule if args.group_rule else DEFAULT_GROUP_RULES,
            group_output_name_template=args.group_output_name_template,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
            verbose=not args.quiet,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1

    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"  Processes checked: {summary.total_processes}")
    print(f"  Merged:            {summary.n_merged}")
    print(f"  Copied:            {summary.n_copied}")
    print(f"  Skipped:           {summary.n_skipped}")
    print(f"  Failed:            {summary.n_failed}")

    if args.report:
        serialisable = {
            "manifest_path": summary.manifest_path,
            "total_processes": summary.total_processes,
            "n_merged": summary.n_merged,
            "n_copied": summary.n_copied,
            "n_skipped": summary.n_skipped,
            "n_failed": summary.n_failed,
            "results": {result_key: asdict(result) for result_key, result in summary.results.items()},
        }
        with open(args.report, "w") as file_handle:
            json.dump(serialisable, file_handle, indent=2)
        print(f"\nReport written to {args.report}")

    return 0 if summary.n_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
