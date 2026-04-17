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
import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.check_template_outputs import stat_eos_file, store_path_from_eos_url
from condor.config import EOS_MKDIR, OUTPUT, get_store_eos_path


@dataclass
class ProcessMergeResult:
    """Result of merging template chunks for a single process."""

    process: str
    output_path: str
    all_inputs: List[str] = field(default_factory=list)
    valid_inputs: List[str] = field(default_factory=list)
    invalid_inputs: List[str] = field(default_factory=list)
    n_inputs_total: int = 0
    n_inputs_valid: int = 0
    action: str = "none"
    status: str = "pending"
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
        self.results[result.process] = result
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


def merge_single_process(
    process: str,
    inputs: List[str],
    output_name_template: str,
    dry_run: bool = False,
    overwrite: bool = False,
) -> ProcessMergeResult:
    """Merge or copy template chunks for one process."""
    output_store_path = f"{OUTPUT['templates_dir']}/{process}/{output_name_template.format(process=process)}"
    output_eos_path = get_store_eos_path(output_store_path)

    result = ProcessMergeResult(
        process=process,
        output_path=output_eos_path,
        all_inputs=inputs,
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
        result.status = "skipped-existing"
        result.action = "skip"
        return result

    if dry_run:
        result.status = "skipped-dry-run"
        result.action = "copy" if result.n_inputs_valid == 1 else "merge"
        return result

    try:
        ensure_eos_directory(output_eos_path)

        if result.n_inputs_valid == 1:
            run_xrdcp(result.valid_inputs[0], output_eos_path)
            result.action = "copy"
            result.status = "ok"
            return result

        with tempfile.TemporaryDirectory(prefix=f"merge_{process}_") as temp_dir:
            local_output = str(Path(temp_dir) / f"merged_{process}.root")
            run_hadd(local_output, result.valid_inputs)
            run_xrdcp(local_output, output_eos_path)

        result.action = "merge"
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
    dry_run: bool = False,
    overwrite: bool = False,
    verbose: bool = True,
) -> MergeSummary:
    """Merge template chunks for all (or selected) processes in the manifest."""
    grouped_inputs = build_process_inputs(manifest)
    manifest_path = manifest.get("_source_path", "unknown")
    summary = MergeSummary(manifest_path=manifest_path)

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
            dry_run=dry_run,
            overwrite=overwrite,
        )
        summary.add(result)

        if verbose:
            if result.status == "ok":
                print(f"  [OK] {process}: {result.action} -> {result.output_path}")
            elif result.status.startswith("skipped"):
                print(f"  [SKIP] {process}: {result.status} ({result.n_inputs_valid} valid inputs)")
            else:
                print(f"  [FAIL] {process}: {result.error}")

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
        """,
    )
    parser.add_argument("--manifest", required=True, help="Path to template manifest JSON")
    parser.add_argument("--processes", nargs="+", default=None, help="Only merge these processes")
    parser.add_argument(
        "--output-name-template",
        default="templates_{process}.root",
        help="Merged filename template (default: templates_{process}.root)",
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
    print("=" * 80 + "\n")

    try:
        summary = merge_all_processes(
            manifest=manifest,
            processes=args.processes,
            output_name_template=args.output_name_template,
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
            "results": {process: asdict(result) for process, result in summary.results.items()},
        }
        with open(args.report, "w") as file_handle:
            json.dump(serialisable, file_handle, indent=2)
        print(f"\nReport written to {args.report}")

    return 0 if summary.n_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
