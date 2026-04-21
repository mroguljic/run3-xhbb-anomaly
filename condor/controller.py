#!/usr/bin/env python3
"""End-to-end Condor controller.

This orchestrator reuses Condor primitives to coordinate:
  1. Preselection planning (manifest + submission)
  2. Skim completeness checking
  3. Template planning (template manifest + submission)
  4. Template completeness checking
  5. Template merge stage (per-process hadd + local copy)

The controller is idempotent:
  - If an artifact exists and is valid, it is reused.
  - If missing/invalid, it is regenerated.
  - Campaign consistency is enforced; mismatches trigger error or force-regeneration.

By default the controller is non-destructive and does not submit jobs.
Use ``--auto-submit`` and/or ``--resubmit-missing`` to enable submissions.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.check_skim_outputs import check_all_skims
from condor.check_template_outputs import check_all_templates
from condor.config import (
    AUTO_RESUBMIT_MISSING,
    AUTO_SUBMIT,
    CAMPAIGN,
    DEFAULT_YEAR,
    LOCAL_MERGED_TEMPLATES_DIR,
    MERGED_TEMPLATE_FILENAME,
    YEARS_TO_PROCESS,
)


@dataclass
class ControllerPaths:
    """Resolved artifact paths for a given year."""

    bookkeeping_dir: Path
    manifest: Path
    submission: Path
    skim_report: Path
    template_manifest: Path
    template_submission: Path
    template_report: Path
    merge_report: Path
    skim_missing_manifest: Path
    skim_resubmit_submission: Path
    template_missing_manifest: Path
    template_resubmit_submission: Path
    status_json: Path


@dataclass
class StageOutcome:
    """Result metadata for one controller stage."""

    name: str
    ok: bool
    details: Dict[str, Any]


def parse_args() -> argparse.Namespace:
    """Parse controller CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Run preselection/template Condor orchestration stages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 controller.py --year 2024 --all-datasets --dry-run
  python3 controller.py --year 2024 --all-datasets --auto-submit
  python3 controller.py --year 2024 --datasets TTto4Q QCD-4Jets_HT-800to1000 --resubmit-missing
        """,
    )
    parser.add_argument("--year", default=DEFAULT_YEAR, choices=YEARS_TO_PROCESS, help=f"Data year (default: {DEFAULT_YEAR})")

    dataset_group = parser.add_mutually_exclusive_group(required=True)
    dataset_group.add_argument("--all-datasets", action="store_true", help="Process all datasets for the selected year")
    dataset_group.add_argument("--datasets", nargs="+", default=None, help="Explicit dataset keys to process")

    parser.add_argument("--test", action="store_true", help="Use test mode when generating submission files")
    parser.add_argument("--auto-submit", action="store_true", default=AUTO_SUBMIT, help="Submit generated .sub files with condor_submit")
    parser.add_argument(
        "--resubmit-missing",
        action="store_true",
        default=AUTO_RESUBMIT_MISSING,
        help="Generate resubmission files for missing outputs and continue past stage gates",
    )
    parser.add_argument("--skip-merge", action="store_true", help="Skip template merge stage and local copy")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without creating or submitting artifacts")
    parser.add_argument(
        "--status-json",
        default=None,
        help="Optional path for machine-readable status report (default: controller_status_<year>.json)",
    )
    parser.add_argument(
        "--force-regenerate-manifests",
        "--force-regen",
        dest="force_regenerate_manifests",
        action="store_true",
        help="Regenerate manifests/submission files instead of reusing existing artifacts",
    )
    return parser.parse_args()


def run_command(command: List[str], cwd: Path, dry_run: bool) -> int:
    """Run one subprocess command.

    Args:
        command: Full command token list.
        cwd: Working directory.
        dry_run: If True, print command and skip execution.

    Returns:
        Exit code (0 for success).
    """
    rendered = " ".join(command)
    print(f"  $ {rendered}")
    if dry_run:
        return 0

    completed = subprocess.run(command, cwd=str(cwd))
    return completed.returncode


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON from disk.

    Args:
        path: JSON file path.

    Returns:
        Parsed JSON dictionary.

    Raises:
        RuntimeError: If file is missing or malformed.
    """
    try:
        with path.open("r") as file_handle:
            data = json.load(file_handle)
    except FileNotFoundError as exc:
        raise RuntimeError(f"File not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise RuntimeError(f"Expected JSON object in {path}, got {type(data).__name__}")
    return data


def write_json(path: Path, payload: Dict[str, Any], dry_run: bool) -> None:
    """Write JSON with indentation.

    Args:
        path: Output path.
        payload: Serializable dictionary.
        dry_run: If True, no write is performed.
    """
    if dry_run:
        print(f"  DRY-RUN: would write {path}")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as file_handle:
        json.dump(payload, file_handle, indent=2)


def prompt_yes_no(question: str, default: bool = False) -> bool:
    """Prompt user for yes/no input with a default choice."""
    suffix = "[Y/n]" if default else "[y/N]"
    while True:
        try:
            response = input(f"{question} {suffix} ").strip().lower()
        except EOFError:
            return default

        if response == "":
            return default
        if response in {"y", "yes"}:
            return True
        if response in {"n", "no"}:
            return False
        print("  Please answer 'y' or 'n'.")


def choose_step_action(step_label: str, report_path: Path | None = None) -> str:
    """Return action for a stage: run, reuse, or skip."""
    if report_path is not None and report_path.exists():
        timestamp = datetime.fromtimestamp(report_path.stat().st_mtime).isoformat(timespec="seconds")
        print(f"  Found previous report: {report_path} (updated {timestamp})")
        if prompt_yes_no("  Reuse previous report?", default=True):
            return "reuse"

    if prompt_yes_no(f"  Perform {step_label}?", default=False):
        return "run"
    return "skip"


def outcome_from_existing_report(stage_name: str, report_path: Path) -> StageOutcome:
    """Create a stage outcome from a previously written JSON report."""
    report_data = load_json(report_path)

    if stage_name in {"skim-check", "template-check"}:
        missing_batch_ids = report_data.get("missing_batch_ids", [])
        details = {
            "report": str(report_path),
            "total": report_data.get("total"),
            "ok": report_data.get("n_ok"),
            "missing": report_data.get("n_missing"),
            "empty": report_data.get("n_empty"),
            "missing_batch_ids": missing_batch_ids,
            "reused_report": True,
        }
        stage_ok = report_data.get("n_missing", 0) == 0 and report_data.get("n_empty", 0) == 0
        return StageOutcome(name=stage_name, ok=stage_ok, details=details)

    if stage_name == "merge-stage":
        merged_count = report_data.get("n_merged", 0) + report_data.get("n_copied", 0)
        failed_count = report_data.get("n_failed", 0)
        details = {
            "report": str(report_path),
            "merged_count": merged_count,
            "failed_count": failed_count,
            "reused_report": True,
        }
        return StageOutcome(name=stage_name, ok=failed_count == 0, details=details)

    raise RuntimeError(f"Unsupported stage report type: {stage_name}")


def valid_manifest(path: Path) -> bool:
    """Check if a manifest JSON exists and has required keys."""
    if not path.exists():
        return False
    try:
        data = load_json(path)
    except RuntimeError:
        return False
    return isinstance(data.get("datasets"), dict) and "year" in data and "campaign" in data


def get_manifest_campaign(path: Path) -> str:
    """Extract the campaign from a manifest JSON file.

    Args:
        path: Manifest file path.

    Returns:
        Campaign value as a string.

    Raises:
        RuntimeError: If manifest is missing or lacks campaign metadata.
    """
    data = load_json(path)
    campaign = data.get("campaign")
    if not isinstance(campaign, str) or not campaign:
        raise RuntimeError(f"Manifest {path.name} is missing a valid campaign field")
    return campaign


def valid_submission(path: Path) -> bool:
    """Check if a submission file exists and looks non-empty."""
    if not path.exists():
        return False
    try:
        content = path.read_text()
    except OSError:
        return False
    return "queue BATCH_ID" in content and "executable" in content


def ensure_manifest(
    manifest_path: Path,
    condor_dir: Path,
    year: str,
    all_datasets: bool,
    datasets: List[str] | None,
    expected_campaign: str,
    force_regenerate: bool,
    dry_run: bool,
) -> None:
    """Create/reuse preselection manifest."""
    if valid_manifest(manifest_path):
        manifest_campaign = get_manifest_campaign(manifest_path)
        if manifest_campaign != expected_campaign:
            if not force_regenerate:
                raise RuntimeError(
                    f"Campaign mismatch in {manifest_path.name}: {manifest_campaign} != {expected_campaign}. "
                    "Use --force-regen to regenerate artifacts."
                )
            print(
                f"  Regenerating manifest due to campaign mismatch: "
                f"{manifest_campaign} -> {expected_campaign}"
            )
            if not dry_run:
                manifest_path.unlink(missing_ok=True)
        elif force_regenerate:
            print(f"  Force regenerating manifest: {manifest_path.name}")
            if not dry_run:
                manifest_path.unlink(missing_ok=True)
        else:
            print(f"  Reusing existing manifest: {manifest_path.name}")
            return

    command = ["python3", "generate_batches.py", "--year", year, "--output", manifest_path.name]
    if all_datasets:
        command.append("--all-datasets")
    else:
        command.extend(["--datasets", *(datasets or [])])

    print(f"  Generating preselection manifest: {manifest_path.name}")
    exit_code = run_command(command, cwd=condor_dir, dry_run=dry_run)
    if exit_code != 0:
        raise RuntimeError("Failed to generate preselection manifest")


def ensure_template_manifest(
    template_manifest_path: Path,
    skim_manifest_path: Path,
    condor_dir: Path,
    datasets: List[str] | None,
    expected_campaign: str,
    force_regenerate: bool,
    dry_run: bool,
) -> None:
    """Create/reuse template manifest with campaign consistency checks."""
    if valid_manifest(template_manifest_path):
        manifest_campaign = get_manifest_campaign(template_manifest_path)
        if manifest_campaign != expected_campaign:
            if not force_regenerate:
                raise RuntimeError(
                    f"Campaign mismatch in {template_manifest_path.name}: "
                    f"{manifest_campaign} != {expected_campaign}. Use --force-regen to regenerate artifacts."
                )
            print(
                f"  Regenerating template manifest due to campaign mismatch: "
                f"{manifest_campaign} -> {expected_campaign}"
            )
            if not dry_run:
                template_manifest_path.unlink(missing_ok=True)
        elif force_regenerate:
            print(f"  Force regenerating template manifest: {template_manifest_path.name}")
            if not dry_run:
                template_manifest_path.unlink(missing_ok=True)
        else:
            print(f"  Reusing existing template manifest: {template_manifest_path.name}")
            return

    command = [
        "python3",
        "generate_template_manifest.py",
        "--skim-manifest",
        skim_manifest_path.name,
        "--output",
        template_manifest_path.name,
    ]
    if datasets:
        command.extend(["--datasets", *datasets])

    print(f"  Generating template manifest: {template_manifest_path.name}")
    exit_code = run_command(command, cwd=condor_dir, dry_run=dry_run)
    if exit_code != 0:
        raise RuntimeError("Failed to generate template manifest")


def ensure_campaign_consistency(path: Path, expected_campaign: str) -> None:
    """Validate that an existing manifest campaign matches the configured campaign."""
    if not valid_manifest(path):
        raise RuntimeError(f"Cannot validate campaign for invalid manifest: {path}")
    manifest_campaign = get_manifest_campaign(path)
    if manifest_campaign != expected_campaign:
        raise RuntimeError(
            f"Campaign mismatch in {path.name}: {manifest_campaign} != {expected_campaign}. "
            "Use --force-regen to regenerate artifacts."
        )


def ensure_submission(
    submission_path: Path,
    manifest_path: Path,
    condor_dir: Path,
    test_mode: bool,
    force_regenerate: bool,
    dry_run: bool,
) -> None:
    """Create/reuse submission file for a manifest."""
    if valid_submission(submission_path) and not force_regenerate:
        print(f"  Reusing existing submission: {submission_path.name}")
        return

    if valid_submission(submission_path) and force_regenerate:
        print(f"  Force regenerating submission: {submission_path.name}")
        if not dry_run:
            submission_path.unlink(missing_ok=True)

    command = [
        "python3",
        "generate_submission.py",
        "--manifest",
        manifest_path.name,
        "--output",
        submission_path.name,
    ]
    if test_mode:
        command.append("--test")

    print(f"  Generating submission file: {submission_path.name}")
    exit_code = run_command(command, cwd=condor_dir, dry_run=dry_run)
    if exit_code != 0:
        raise RuntimeError(f"Failed to generate submission file {submission_path.name}")


def maybe_submit(submission_path: Path, condor_dir: Path, auto_submit: bool, dry_run: bool) -> None:
    """Optionally submit a Condor submission file."""
    if not auto_submit:
        print(f"  Submission disabled: condor_submit {submission_path.name}")
        return

    print(f"  Submitting: {submission_path.name}")
    exit_code = run_command(["condor_submit", submission_path.name], cwd=condor_dir, dry_run=dry_run)
    if exit_code != 0:
        raise RuntimeError(f"condor_submit failed for {submission_path.name}")


def filter_manifest_to_batches(manifest: Dict[str, Any], selected_batch_ids: List[str]) -> Dict[str, Any]:
    """Return a manifest containing only selected batch IDs."""
    wanted = set(selected_batch_ids)
    filtered = deepcopy(manifest)
    filtered_datasets: Dict[str, Any] = {}

    for dataset_key, dataset_info in manifest.get("datasets", {}).items():
        batches = dataset_info.get("batches", {})
        selected_batches = {batch_id: batch for batch_id, batch in batches.items() if batch_id in wanted}
        if not selected_batches:
            continue

        dataset_copy = deepcopy(dataset_info)
        dataset_copy["batches"] = selected_batches

        if "n_batches" in dataset_copy:
            dataset_copy["n_batches"] = len(selected_batches)
        if "n_chunks" in dataset_copy:
            dataset_copy["n_chunks"] = len(selected_batches)

        filtered_datasets[dataset_key] = dataset_copy

    filtered["datasets"] = filtered_datasets
    return filtered


def run_skim_check(manifest_path: Path, report_path: Path, dry_run: bool) -> StageOutcome:
    """Run skim completeness checks and persist report JSON."""
    stage_name = "skim-check"

    if dry_run:
        details = {
            "report": str(report_path),
            "total": None,
            "ok": None,
            "missing": None,
            "empty": None,
        }
        return StageOutcome(name=stage_name, ok=True, details=details)

    manifest = load_json(manifest_path)
    manifest["_source_path"] = str(manifest_path)

    report = check_all_skims(manifest, do_root_check=False, verbose=True)

    serialisable = {
        "generated_at": datetime.now().isoformat(),
        "manifest_path": report.manifest_path,
        "total": report.total,
        "n_ok": report.n_ok,
        "n_missing": report.n_missing,
        "n_empty": report.n_empty,
        "missing_batch_ids": [result.batch_id for result in report.bad_results()],
        "results": {
            batch_id: {
                "batch_id": result.batch_id,
                "dataset": result.dataset,
                "eos_path": result.eos_path,
                "store_path": result.store_path,
                "exists": result.exists,
                "size_bytes": result.size_bytes,
                "error": result.error,
            }
            for batch_id, result in report.results.items()
        },
    }
    write_json(report_path, serialisable, dry_run=False)

    details = {
        "report": str(report_path),
        "total": report.total,
        "ok": report.n_ok,
        "missing": report.n_missing,
        "empty": report.n_empty,
        "missing_batch_ids": serialisable["missing_batch_ids"],
    }
    stage_ok = report.n_missing == 0 and report.n_empty == 0
    return StageOutcome(name=stage_name, ok=stage_ok, details=details)


def run_template_check(manifest_path: Path, report_path: Path, dry_run: bool) -> StageOutcome:
    """Run template completeness checks and persist report JSON."""
    stage_name = "template-check"

    if dry_run:
        details = {
            "report": str(report_path),
            "total": None,
            "ok": None,
            "missing": None,
            "empty": None,
        }
        return StageOutcome(name=stage_name, ok=True, details=details)

    manifest = load_json(manifest_path)
    manifest["_source_path"] = str(manifest_path)

    report = check_all_templates(manifest, verbose=True)

    serialisable = {
        "generated_at": datetime.now().isoformat(),
        "manifest_path": report.manifest_path,
        "total": report.total,
        "n_ok": report.n_ok,
        "n_missing": report.n_missing,
        "n_empty": report.n_empty,
        "missing_batch_ids": report.missing_batch_ids(),
        "results": {
            batch_id: {
                "batch_id": result.batch_id,
                "dataset": result.dataset,
                "process": result.process,
                "eos_path": result.eos_path,
                "store_path": result.store_path,
                "exists": result.exists,
                "size_bytes": result.size_bytes,
                "error": result.error,
            }
            for batch_id, result in report.results.items()
        },
    }
    write_json(report_path, serialisable, dry_run=False)

    details = {
        "report": str(report_path),
        "total": report.total,
        "ok": report.n_ok,
        "missing": report.n_missing,
        "empty": report.n_empty,
        "missing_batch_ids": serialisable["missing_batch_ids"],
    }
    stage_ok = report.n_missing == 0 and report.n_empty == 0
    return StageOutcome(name=stage_name, ok=stage_ok, details=details)


def resolve_paths(condor_dir: Path, year: str, status_json_override: str | None) -> ControllerPaths:
    """Resolve all controller artifact paths for a run."""
    bookkeeping_dir = condor_dir.parent / "output" / "bookkeeping"
    status_name = status_json_override or f"controller_status_{year}.json"

    return ControllerPaths(
        bookkeeping_dir=bookkeeping_dir,
        manifest=condor_dir / f"manifest_{year}.json",
        submission=condor_dir / f"submission_{year}.sub",
        skim_report=bookkeeping_dir / f"skim_report_{year}.json",
        template_manifest=condor_dir / f"template_manifest_{year}.json",
        template_submission=condor_dir / f"template_submission_{year}.sub",
        template_report=bookkeeping_dir / f"template_report_{year}.json",
        merge_report=bookkeeping_dir / f"merge_report_{year}.json",
        skim_missing_manifest=condor_dir / f"manifest_missing_skims_{year}.json",
        skim_resubmit_submission=condor_dir / f"submission_missing_skims_{year}.sub",
        template_missing_manifest=condor_dir / f"template_manifest_missing_{year}.json",
        template_resubmit_submission=condor_dir / f"template_submission_missing_{year}.sub",
        status_json=bookkeeping_dir / status_name,
    )


def run_merge_stage(
    template_manifest_path: Path,
    merge_report_path: Path,
    condor_dir: Path,
    dry_run: bool,
    skip_local_copy: bool = False,
) -> StageOutcome:
    """Run template merge and optionally copy to local destination."""
    stage_name = "merge-stage"

    command = [
        "python3",
        "merge_templates.py",
        "--manifest",
        template_manifest_path.name,
        "--report",
        merge_report_path.name,
    ]

    print(f"  Running merge: {' '.join(command)}")
    exit_code = run_command(command, cwd=condor_dir, dry_run=dry_run)

    details: Dict[str, Any] = {
        "report": str(merge_report_path),
        "merged_count": None,
        "failed_count": None,
    }

    if not dry_run and merge_report_path.exists():
        try:
            report_data = load_json(merge_report_path)
            details["merged_count"] = report_data.get("n_merged", 0) + report_data.get("n_copied", 0)
            details["failed_count"] = report_data.get("n_failed", 0)
        except RuntimeError:
            pass

    if exit_code != 0:
        return StageOutcome(name=stage_name, ok=False, details={**details, "error": "Merge failed"})

    if not skip_local_copy and not dry_run:
        print(f"  Copying merged templates to local: {LOCAL_MERGED_TEMPLATES_DIR}")
        try:
            local_dir = Path(LOCAL_MERGED_TEMPLATES_DIR)
            local_dir.mkdir(parents=True, exist_ok=True)
            details["local_copy_dir"] = str(local_dir.resolve())
        except OSError as exc:
            print(f"  Warning: Failed to create local directory: {exc}")
            details["local_copy_warning"] = str(exc)

    stage_ok = exit_code == 0
    return StageOutcome(name=stage_name, ok=stage_ok, details=details)


def main() -> int:
    """Controller entrypoint."""
    args = parse_args()

    condor_dir = Path(__file__).resolve().parent
    paths = resolve_paths(condor_dir=condor_dir, year=args.year, status_json_override=args.status_json)

    status_payload: Dict[str, Any] = {
        "year": args.year,
        "dry_run": args.dry_run,
        "resubmit_missing": args.resubmit_missing,
        "auto_submit": args.auto_submit,
        "bookkeeping_dir": str(paths.bookkeeping_dir),
        "stages": {},
    }

    print("=" * 88)
    print("Condor Controller (M1 + M2 + M3)")
    print("=" * 88)
    print(f"Year:              {args.year}")
    print(f"All datasets:      {'yes' if args.all_datasets else 'no'}")
    print(f"Datasets:          {', '.join(args.datasets) if args.datasets else '(all)'}")
    print(f"Test mode:         {'yes' if args.test else 'no'}")
    print(f"Dry run:           {'yes' if args.dry_run else 'no'}")
    print(f"Auto submit:       {'yes' if args.auto_submit else 'no'}")
    print(f"Resubmit missing:  {'yes' if args.resubmit_missing else 'no'}")
    print(f"Force regen:       {'yes' if args.force_regenerate_manifests else 'no'}")
    print(f"Campaign:          {CAMPAIGN}")
    print(f"Bookkeeping dir:   {paths.bookkeeping_dir}")
    print("=" * 88)

    if args.dry_run:
        print(f"  DRY-RUN: would ensure bookkeeping directory exists: {paths.bookkeeping_dir}")
    else:
        paths.bookkeeping_dir.mkdir(parents=True, exist_ok=True)

    try:
        print("\n[1/5] Preselection planning")
        preselection_action = choose_step_action("preselection planning")
        if preselection_action == "run":
            ensure_manifest(
                manifest_path=paths.manifest,
                condor_dir=condor_dir,
                year=args.year,
                all_datasets=args.all_datasets,
                datasets=args.datasets,
                expected_campaign=CAMPAIGN,
                force_regenerate=args.force_regenerate_manifests,
                dry_run=args.dry_run,
            )
            if not args.dry_run:
                ensure_campaign_consistency(paths.manifest, CAMPAIGN)
            ensure_submission(
                submission_path=paths.submission,
                manifest_path=paths.manifest,
                condor_dir=condor_dir,
                test_mode=args.test,
                force_regenerate=args.force_regenerate_manifests,
                dry_run=args.dry_run,
            )
            maybe_submit(paths.submission, condor_dir=condor_dir, auto_submit=args.auto_submit, dry_run=args.dry_run)
            status_payload["stages"]["preselection-planning"] = {
                "ok": True,
                "executed": True,
                "manifest": str(paths.manifest),
                "submission": str(paths.submission),
            }
        else:
            print("  Preselection planning skipped by user")
            status_payload["stages"]["preselection-planning"] = {"ok": True, "executed": False, "reason": "user-skipped"}
            if not valid_manifest(paths.manifest):
                print("  Stage gate: no valid preselection manifest available")
                status_payload["stage_gate_block"] = "missing-preselection-manifest"
                write_json(paths.status_json, status_payload, dry_run=args.dry_run)
                return 2

        print("\n[2/5] Skim completeness check")
        skim_action = choose_step_action("skim completeness check", report_path=paths.skim_report)
        if skim_action == "reuse":
            skim_outcome = outcome_from_existing_report("skim-check", paths.skim_report)
        elif skim_action == "run":
            skim_outcome = run_skim_check(
                manifest_path=paths.manifest,
                report_path=paths.skim_report,
                dry_run=args.dry_run,
            )
        else:
            skim_outcome = StageOutcome(
                name="skim-check",
                ok=True,
                details={"executed": False, "reason": "user-skipped", "report": str(paths.skim_report)},
            )
        status_payload["stages"][skim_outcome.name] = {"ok": skim_outcome.ok, **skim_outcome.details}

        skim_missing_batch_ids = skim_outcome.details.get("missing_batch_ids", []) or []
        if skim_missing_batch_ids:
            print(f"  Missing skim outputs: {len(skim_missing_batch_ids)}")
            if args.resubmit_missing:
                source_manifest = load_json(paths.manifest) if not args.dry_run else {"datasets": {}}
                missing_manifest = filter_manifest_to_batches(source_manifest, skim_missing_batch_ids)
                write_json(paths.skim_missing_manifest, missing_manifest, dry_run=args.dry_run)
                ensure_submission(
                    submission_path=paths.skim_resubmit_submission,
                    manifest_path=paths.skim_missing_manifest,
                    condor_dir=condor_dir,
                    test_mode=args.test,
                    force_regenerate=args.force_regenerate_manifests,
                    dry_run=args.dry_run,
                )
                maybe_submit(
                    paths.skim_resubmit_submission,
                    condor_dir=condor_dir,
                    auto_submit=args.auto_submit,
                    dry_run=args.dry_run,
                )
            else:
                print("  Stage gate: stopping before template stage (use --resubmit-missing to continue)")
                status_payload["stage_gate_block"] = "skim-missing"
                write_json(paths.status_json, status_payload, dry_run=args.dry_run)
                return 2

        print("\n[3/5] Template planning")
        template_planning_action = choose_step_action("template planning")
        if template_planning_action == "run":
            ensure_template_manifest(
                template_manifest_path=paths.template_manifest,
                skim_manifest_path=paths.manifest,
                condor_dir=condor_dir,
                datasets=args.datasets,
                expected_campaign=CAMPAIGN,
                force_regenerate=args.force_regenerate_manifests,
                dry_run=args.dry_run,
            )
            if not args.dry_run:
                ensure_campaign_consistency(paths.template_manifest, CAMPAIGN)

            ensure_submission(
                submission_path=paths.template_submission,
                manifest_path=paths.template_manifest,
                condor_dir=condor_dir,
                test_mode=args.test,
                force_regenerate=args.force_regenerate_manifests,
                dry_run=args.dry_run,
            )
            maybe_submit(paths.template_submission, condor_dir=condor_dir, auto_submit=args.auto_submit, dry_run=args.dry_run)
            status_payload["stages"]["template-planning"] = {
                "ok": True,
                "executed": True,
                "template_manifest": str(paths.template_manifest),
                "template_submission": str(paths.template_submission),
            }
        else:
            print("  Template planning skipped by user")
            status_payload["stages"]["template-planning"] = {"ok": True, "executed": False, "reason": "user-skipped"}
            if not valid_manifest(paths.template_manifest):
                print("  Stage gate: no valid template manifest available")
                status_payload["stage_gate_block"] = "missing-template-manifest"
                write_json(paths.status_json, status_payload, dry_run=args.dry_run)
                return 3

        print("\n[4/5] Template completeness check")
        template_check_action = choose_step_action("template completeness check", report_path=paths.template_report)
        if template_check_action == "reuse":
            template_outcome = outcome_from_existing_report("template-check", paths.template_report)
        elif template_check_action == "run":
            template_outcome = run_template_check(
                manifest_path=paths.template_manifest,
                report_path=paths.template_report,
                dry_run=args.dry_run,
            )
        else:
            template_outcome = StageOutcome(
                name="template-check",
                ok=True,
                details={"executed": False, "reason": "user-skipped", "report": str(paths.template_report)},
            )
        status_payload["stages"][template_outcome.name] = {"ok": template_outcome.ok, **template_outcome.details}

        template_missing_batch_ids = template_outcome.details.get("missing_batch_ids", []) or []
        if template_missing_batch_ids:
            print(f"  Missing template outputs: {len(template_missing_batch_ids)}")
            if args.resubmit_missing:
                source_manifest = load_json(paths.template_manifest) if not args.dry_run else {"datasets": {}}
                missing_manifest = filter_manifest_to_batches(source_manifest, template_missing_batch_ids)
                write_json(paths.template_missing_manifest, missing_manifest, dry_run=args.dry_run)
                ensure_submission(
                    submission_path=paths.template_resubmit_submission,
                    manifest_path=paths.template_missing_manifest,
                    condor_dir=condor_dir,
                    test_mode=args.test,
                    force_regenerate=args.force_regenerate_manifests,
                    dry_run=args.dry_run,
                )
                maybe_submit(
                    paths.template_resubmit_submission,
                    condor_dir=condor_dir,
                    auto_submit=args.auto_submit,
                    dry_run=args.dry_run,
                )
            else:
                print("  Stage gate: stopping before merge stage (use --resubmit-missing to continue)")
                status_payload["stage_gate_block"] = "template-missing"
                write_json(paths.status_json, status_payload, dry_run=args.dry_run)
                return 3

        print("\n[5/5] Merge stage")
        if args.skip_merge:
            print("  Merge stage skipped via --skip-merge")
            status_payload["stages"]["merge-stage"] = {
                "ok": True,
                "executed": False,
                "reason": "skipped",
            }
        else:
            merge_action = choose_step_action("merge stage", report_path=paths.merge_report)
            if merge_action == "reuse":
                merge_outcome = outcome_from_existing_report("merge-stage", paths.merge_report)
            elif merge_action == "run":
                merge_outcome = run_merge_stage(
                    template_manifest_path=paths.template_manifest,
                    merge_report_path=paths.merge_report,
                    condor_dir=condor_dir,
                    dry_run=args.dry_run,
                    skip_local_copy=args.dry_run,
                )
            else:
                merge_outcome = StageOutcome(
                    name="merge-stage",
                    ok=True,
                    details={"executed": False, "reason": "user-skipped", "report": str(paths.merge_report)},
                )
            status_payload["stages"]["merge-stage"] = {"ok": merge_outcome.ok, **merge_outcome.details}

        write_json(paths.status_json, status_payload, dry_run=args.dry_run)

        print("\n" + "=" * 88)
        print("Campaign Completion Summary")
        print("=" * 88)
        print(f"Campaign:          {CAMPAIGN}")
        print(f"Year:              {args.year}")
        print(f"Status JSON:       {paths.status_json}")
        if not args.dry_run:
            skim_report_exists = paths.skim_report.exists()
            template_report_exists = paths.template_report.exists()
            merge_report_exists = paths.merge_report.exists()

            if skim_report_exists:
                try:
                    skim_data = load_json(paths.skim_report)
                    print(f"Skim outputs:      {skim_data.get('n_ok', 0)}/{skim_data.get('total', 0)} OK")
                except RuntimeError:
                    pass

            if template_report_exists:
                try:
                    template_data = load_json(paths.template_report)
                    print(f"Template outputs:  {template_data.get('n_ok', 0)}/{template_data.get('total', 0)} OK")
                except RuntimeError:
                    pass

            if merge_report_exists:
                try:
                    merge_data = load_json(paths.merge_report)
                    total_merged = merge_data.get("n_merged", 0) + merge_data.get("n_copied", 0)
                    print(f"Merged processes:  {total_merged} ({merge_data.get('n_failed', 0)} failed)")
                except RuntimeError:
                    pass

        print("=" * 88)
        print("Controller completed")
        print("=" * 88)
        return 0

    except Exception as exc:
        status_payload["error"] = str(exc)
        write_json(paths.status_json, status_payload, dry_run=args.dry_run)
        print(f"\nERROR: {exc}")
        print(f"Status JSON: {paths.status_json}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
