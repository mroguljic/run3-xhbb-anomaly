#!/usr/bin/env python3
"""End-to-end Condor controller (M1).

This orchestrator reuses existing Condor primitives to coordinate:
  1. Preselection planning (manifest + submission)
  2. Skim completeness checking
  3. Template planning (template manifest + submission)
  4. Template completeness checking

The controller is idempotent:
  - If an artifact exists and is valid, it is reused.
  - If missing/invalid, it is regenerated.

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
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.check_skim_outputs import check_all_skims
from condor.check_template_outputs import check_all_templates
from condor.config import (
    AUTO_RESUBMIT_MISSING,
    AUTO_SUBMIT,
    CONTROLLER_INTERACTIVE,
    DEFAULT_YEAR,
    YEARS_TO_PROCESS,
)


@dataclass
class ControllerPaths:
    """Resolved artifact paths for a given year."""

    manifest: Path
    submission: Path
    skim_report: Path
    template_manifest: Path
    template_submission: Path
    template_report: Path
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
    parser.add_argument("--skip-merge", action="store_true", help="Reserved for future merge-stage integration")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without creating or submitting artifacts")
    parser.add_argument(
        "--no-interactive",
        action="store_true",
        help="Disable interactive prompts if added in future stages",
    )
    parser.add_argument(
        "--status-json",
        default=None,
        help="Optional path for machine-readable status report (default: controller_status_<year>.json)",
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

    with path.open("w") as file_handle:
        json.dump(payload, file_handle, indent=2)


def valid_manifest(path: Path) -> bool:
    """Check if a manifest JSON exists and has required keys."""
    if not path.exists():
        return False
    try:
        data = load_json(path)
    except RuntimeError:
        return False
    return isinstance(data.get("datasets"), dict) and "year" in data and "campaign" in data


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
    dry_run: bool,
) -> None:
    """Create/reuse preselection manifest."""
    if valid_manifest(manifest_path):
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


def ensure_submission(
    submission_path: Path,
    manifest_path: Path,
    condor_dir: Path,
    test_mode: bool,
    dry_run: bool,
) -> None:
    """Create/reuse submission file for a manifest."""
    if valid_submission(submission_path):
        print(f"  Reusing existing submission: {submission_path.name}")
        return

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
    status_name = status_json_override or f"controller_status_{year}.json"

    return ControllerPaths(
        manifest=condor_dir / f"manifest_{year}.json",
        submission=condor_dir / f"submission_{year}.sub",
        skim_report=condor_dir / f"skim_report_{year}.json",
        template_manifest=condor_dir / f"template_manifest_{year}.json",
        template_submission=condor_dir / f"template_submission_{year}.sub",
        template_report=condor_dir / f"template_report_{year}.json",
        skim_missing_manifest=condor_dir / f"manifest_missing_skims_{year}.json",
        skim_resubmit_submission=condor_dir / f"submission_missing_skims_{year}.sub",
        template_missing_manifest=condor_dir / f"template_manifest_missing_{year}.json",
        template_resubmit_submission=condor_dir / f"template_submission_missing_{year}.sub",
        status_json=condor_dir / status_name,
    )


def main() -> int:
    """Controller entrypoint."""
    args = parse_args()

    condor_dir = Path(__file__).resolve().parent
    paths = resolve_paths(condor_dir=condor_dir, year=args.year, status_json_override=args.status_json)

    interactive_enabled = CONTROLLER_INTERACTIVE and not args.no_interactive
    _ = interactive_enabled  # reserved for future interactive prompts

    status_payload: Dict[str, Any] = {
        "year": args.year,
        "dry_run": args.dry_run,
        "resubmit_missing": args.resubmit_missing,
        "auto_submit": args.auto_submit,
        "stages": {},
    }

    print("=" * 88)
    print("Condor Controller (M1)")
    print("=" * 88)
    print(f"Year:              {args.year}")
    print(f"All datasets:      {'yes' if args.all_datasets else 'no'}")
    print(f"Datasets:          {', '.join(args.datasets) if args.datasets else '(all)'}")
    print(f"Test mode:         {'yes' if args.test else 'no'}")
    print(f"Dry run:           {'yes' if args.dry_run else 'no'}")
    print(f"Auto submit:       {'yes' if args.auto_submit else 'no'}")
    print(f"Resubmit missing:  {'yes' if args.resubmit_missing else 'no'}")
    print("=" * 88)

    try:
        print("\n[1/5] Preselection planning")
        ensure_manifest(
            manifest_path=paths.manifest,
            condor_dir=condor_dir,
            year=args.year,
            all_datasets=args.all_datasets,
            datasets=args.datasets,
            dry_run=args.dry_run,
        )
        ensure_submission(
            submission_path=paths.submission,
            manifest_path=paths.manifest,
            condor_dir=condor_dir,
            test_mode=args.test,
            dry_run=args.dry_run,
        )
        maybe_submit(paths.submission, condor_dir=condor_dir, auto_submit=args.auto_submit, dry_run=args.dry_run)
        status_payload["stages"]["preselection-planning"] = {"ok": True, "manifest": str(paths.manifest), "submission": str(paths.submission)}

        print("\n[2/5] Skim completeness check")
        skim_outcome = run_skim_check(
            manifest_path=paths.manifest,
            report_path=paths.skim_report,
            dry_run=args.dry_run,
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
        if valid_manifest(paths.template_manifest):
            print(f"  Reusing existing template manifest: {paths.template_manifest.name}")
        else:
            command = [
                "python3",
                "generate_template_manifest.py",
                "--skim-manifest",
                paths.manifest.name,
                "--output",
                paths.template_manifest.name,
            ]
            if args.datasets:
                command.extend(["--datasets", *args.datasets])
            print(f"  Generating template manifest: {paths.template_manifest.name}")
            exit_code = run_command(command, cwd=condor_dir, dry_run=args.dry_run)
            if exit_code != 0:
                raise RuntimeError("Failed to generate template manifest")

        ensure_submission(
            submission_path=paths.template_submission,
            manifest_path=paths.template_manifest,
            condor_dir=condor_dir,
            test_mode=args.test,
            dry_run=args.dry_run,
        )
        maybe_submit(paths.template_submission, condor_dir=condor_dir, auto_submit=args.auto_submit, dry_run=args.dry_run)
        status_payload["stages"]["template-planning"] = {
            "ok": True,
            "template_manifest": str(paths.template_manifest),
            "template_submission": str(paths.template_submission),
        }

        print("\n[4/5] Template completeness check")
        template_outcome = run_template_check(
            manifest_path=paths.template_manifest,
            report_path=paths.template_report,
            dry_run=args.dry_run,
        )
        status_payload["stages"][template_outcome.name] = {"ok": template_outcome.ok, **template_outcome.details}

        template_missing_batch_ids = template_outcome.details.get("missing_batch_ids", []) or []
        if template_missing_batch_ids and args.resubmit_missing:
            print(f"  Missing template outputs: {len(template_missing_batch_ids)}")
            source_manifest = load_json(paths.template_manifest) if not args.dry_run else {"datasets": {}}
            missing_manifest = filter_manifest_to_batches(source_manifest, template_missing_batch_ids)
            write_json(paths.template_missing_manifest, missing_manifest, dry_run=args.dry_run)
            ensure_submission(
                submission_path=paths.template_resubmit_submission,
                manifest_path=paths.template_missing_manifest,
                condor_dir=condor_dir,
                test_mode=args.test,
                dry_run=args.dry_run,
            )
            maybe_submit(
                paths.template_resubmit_submission,
                condor_dir=condor_dir,
                auto_submit=args.auto_submit,
                dry_run=args.dry_run,
            )

        print("\n[5/5] Merge stage")
        if args.skip_merge:
            print("  Merge stage skipped via --skip-merge")
        else:
            print("  Merge stage is reserved for M2/M3 (not executed in M1)")

        status_payload["stages"]["merge-stage"] = {
            "ok": True,
            "executed": False,
            "reason": "M1 scope",
        }

        write_json(paths.status_json, status_payload, dry_run=args.dry_run)

        print("\n" + "=" * 88)
        print("Controller completed")
        print("=" * 88)
        print(f"Status JSON: {paths.status_json}")
        return 0

    except Exception as exc:
        status_payload["error"] = str(exc)
        write_json(paths.status_json, status_payload, dry_run=args.dry_run)
        print(f"\nERROR: {exc}")
        print(f"Status JSON: {paths.status_json}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
