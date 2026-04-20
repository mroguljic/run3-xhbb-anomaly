#!/usr/bin/env python3
"""
Template output checker.

Validates template chunk files produced by template jobs by checking:
  1. Existence on EOS (via xrdfs stat)
  2. File size > 0

Can be used as a standalone CLI tool or imported as a module by other scripts.

Usage (standalone):
    python3 check_template_outputs.py --manifest template_manifest_2024.json
    python3 check_template_outputs.py --manifest template_manifest_2024.json --report template_report.json
"""

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.config import EOS_ROOT


@dataclass
class TemplateCheckResult:
    """Result of checking one template chunk output."""

    batch_id: str
    dataset: str
    process: str
    eos_path: str
    store_path: str
    exists: bool = False
    size_bytes: int = 0
    error: Optional[str] = None

    @property
    def size_mb(self) -> float:
        """File size in MB."""
        return self.size_bytes / (1024 ** 2)

    @property
    def ok(self) -> bool:
        """True only when all requested checks passed."""
        if not self.exists or self.size_bytes == 0:
            return False
        return True


@dataclass
class TemplateCheckReport:
    """Aggregated results for all template outputs in a manifest."""

    manifest_path: str
    total: int = 0
    n_ok: int = 0
    n_missing: int = 0
    n_empty: int = 0
    results: Dict[str, TemplateCheckResult] = field(default_factory=dict)

    def add(self, result: TemplateCheckResult) -> None:
        """Register one check result and update counters."""
        self.results[result.batch_id] = result
        self.total += 1
        if not result.exists:
            self.n_missing += 1
        elif result.size_bytes == 0:
            self.n_empty += 1
        else:
            self.n_ok += 1

    def ok_results(self) -> List[TemplateCheckResult]:
        """Return only passing check results."""
        return [result for result in self.results.values() if result.ok]

    def bad_results(self) -> List[TemplateCheckResult]:
        """Return only failing check results."""
        return [result for result in self.results.values() if not result.ok]

    def missing_batch_ids(self) -> List[str]:
        """Return batch IDs that are missing, empty, or invalid."""
        return [result.batch_id for result in self.bad_results()]


def store_path_from_eos_url(eos_url: str) -> str:
    """Strip protocol prefix and return bare /store/... path."""
    store_path = re.sub(r"^root://[^/]+/", "", eos_url)
    if not store_path.startswith("/"):
        store_path = "/" + store_path
    return store_path


def stat_eos_file(store_path: str) -> Tuple[bool, int, Optional[str]]:
    """Check EOS file existence and size using xrdfs stat."""
    host = EOS_ROOT.replace("root://", "").rstrip("/")
    command = ["xrdfs", host, "stat", store_path]

    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    except FileNotFoundError:
        return False, 0, "xrdfs not found; ensure XRootD client is installed"

    if result.returncode != 0:
        return False, 0, result.stderr.strip() or result.stdout.strip()

    for line in result.stdout.splitlines():
        if line.strip().startswith("Size:"):
            try:
                size_bytes = int(line.split(":")[1].strip())
                return True, size_bytes, None
            except (IndexError, ValueError):
                return True, 0, f"Could not parse size from: {line!r}"

    return True, 0, "xrdfs stat returned no Size line"

def check_single_template(
    batch_id: str,
    dataset: str,
    process: str,
    eos_path: str,
) -> TemplateCheckResult:
    """Check one template output file."""
    store_path = store_path_from_eos_url(eos_path)
    check_result = TemplateCheckResult(
        batch_id=batch_id,
        dataset=dataset,
        process=process,
        eos_path=eos_path,
        store_path=store_path,
    )

    exists, size_bytes, error = stat_eos_file(store_path)
    check_result.exists = exists
    check_result.size_bytes = size_bytes
    check_result.error = error

    return check_result


def check_all_templates(
    manifest: dict,
    verbose: bool = True,
) -> TemplateCheckReport:
    """Check all template output files listed in a template manifest."""
    manifest_path = manifest.get("_source_path", "unknown")
    report = TemplateCheckReport(manifest_path=manifest_path)

    for dataset_key, dataset_info in manifest["datasets"].items():
        process_name = dataset_info.get("process", dataset_key)
        batches = dataset_info.get("batches", {})

        for batch_id, batch in batches.items():
            eos_path = batch["output_path"]
            result = check_single_template(
                batch_id=batch_id,
                dataset=dataset_key,
                process=process_name,
                eos_path=eos_path
            )
            report.add(result)

            if verbose:
                status = "OK" if result.ok else ("MISSING" if not result.exists else ("EMPTY" if result.size_bytes == 0 else "INVALID"))
                size_string = f"{result.size_mb:.2f} MB" if result.exists else "—"
                print(f"  [{status:7s}] {batch_id} | {size_string}" + (f" | {result.error}" if result.error else ""))

    return report


def main() -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Check template output files listed in a template manifest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 check_template_outputs.py --manifest template_manifest_2024.json
  python3 check_template_outputs.py --manifest template_manifest_2024.json --report template_report.json
        """,
    )
    parser.add_argument("--manifest", required=True, help="Path to template manifest JSON")
    parser.add_argument("--report", default=None, help="Optional path to write JSON report")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file output")

    args = parser.parse_args()

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
    print("Template Output Checker")
    print("=" * 80)
    print(f"Manifest:   {args.manifest}")
    print(f"Campaign:   {manifest.get('campaign', 'unknown')}")
    print(f"Year:       {manifest.get('year', 'unknown')}")
    print("=" * 80 + "\n")

    report = check_all_templates(
        manifest,
        verbose=not args.quiet,
    )

    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"  Total checked:   {report.total}")
    print(f"  OK:              {report.n_ok}")
    print(f"  Missing:         {report.n_missing}")
    print(f"  Empty (0 B):     {report.n_empty}")

    if report.bad_results():
        print("\nBatches needing resubmission:")
        for result in report.bad_results():
            print(f"  {result.batch_id}  ({result.dataset} -> {result.process})")

    if args.report:
        serialisable = {
            "manifest_path": report.manifest_path,
            "total": report.total,
            "n_ok": report.n_ok,
            "n_missing": report.n_missing,
            "n_empty": report.n_empty,
            "missing_batch_ids": report.missing_batch_ids(),
            "results": {batch_id: asdict(result) for batch_id, result in report.results.items()},
        }
        with open(args.report, "w") as file_handle:
            json.dump(serialisable, file_handle, indent=2)
        print(f"\nReport written to {args.report}")

    return 0 if report.n_missing == 0 and report.n_empty == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
