#!/usr/bin/env python3
"""
Skim output checker.

Validates skim files produced by the preselection stage by checking:
  1. Existence on EOS (via xrdfs stat)
  2. File size > 0
  3. (Optional) ROOT validity: file opens without being zombie and contains
     the expected cutflow histogram.

Can be used as a standalone CLI tool or imported as a module by other scripts
(e.g. generate_template_manifest.py).

Usage (standalone):
    # Check all skims listed in a manifest and print a summary:
    python3 check_skim_outputs.py --manifest manifest_2024.json

    # Write a JSON report:
    python3 check_skim_outputs.py --manifest manifest_2024.json --report skim_report.json

    # Also validate ROOT file contents (requires ROOT + access from current env):
    python3 check_skim_outputs.py --manifest manifest_2024.json --check-root
"""

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.config import EOS_ROOT


# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class SkimCheckResult:
    """Result of checking a single skim output file.

    Attributes:
        batch_id: Batch identifier from the manifest.
        dataset: Dataset name.
        eos_path: Full EOS URL of the skim file.
        store_path: /store/... path (without protocol prefix).
        exists: Whether the file was found on EOS.
        size_bytes: File size in bytes as reported by xrdfs stat, or 0 if not found.
        root_valid: Whether the ROOT file opened successfully and contains the cutflow.
            None means the ROOT check was not requested.
        error: Optional error message from any failed check step.
    """

    batch_id: str
    dataset: str
    eos_path: str
    store_path: str
    exists: bool = False
    size_bytes: int = 0
    root_valid: Optional[bool] = None
    error: Optional[str] = None

    @property
    def size_gb(self) -> float:
        """File size in GB."""
        return self.size_bytes / (1024 ** 3)

    @property
    def ok(self) -> bool:
        """True only when all requested checks passed."""
        if not self.exists or self.size_bytes == 0:
            return False
        if self.root_valid is False:
            return False
        return True


@dataclass
class SkimCheckReport:
    """Aggregated results for all skim files in a manifest.

    Attributes:
        manifest_path: Path to the input manifest JSON.
        total: Total number of skim files checked.
        n_ok: Number of files that passed all checks.
        n_missing: Number of files that do not exist on EOS.
        n_empty: Number of files that exist but have zero size.
        n_root_invalid: Number of files that failed the ROOT validity check.
        results: Per-batch check results keyed by batch_id.
    """

    manifest_path: str
    total: int = 0
    n_ok: int = 0
    n_missing: int = 0
    n_empty: int = 0
    n_root_invalid: int = 0
    results: Dict[str, SkimCheckResult] = field(default_factory=dict)

    def add(self, result: SkimCheckResult) -> None:
        """Register a single check result and update counters."""
        self.results[result.batch_id] = result
        self.total += 1
        if not result.exists:
            self.n_missing += 1
        elif result.size_bytes == 0:
            self.n_empty += 1
        elif result.root_valid is False:
            self.n_root_invalid += 1
        else:
            self.n_ok += 1

    def ok_results(self) -> List[SkimCheckResult]:
        """Return only the passing check results."""
        return [r for r in self.results.values() if r.ok]

    def bad_results(self) -> List[SkimCheckResult]:
        """Return only the failing check results."""
        return [r for r in self.results.values() if not r.ok]


# ============================================================================
# EOS Checks
# ============================================================================

def _store_path_from_eos_url(eos_url: str) -> str:
    """Strip the protocol prefix and return the bare /store/... path.

    Args:
        eos_url: EOS URL such as ``root://cmseos.fnal.gov//store/user/...``

    Returns:
        Bare /store/... path string.
    """
    # Remove protocol+host prefix; handle both single and double slash after host
    store_path = re.sub(r"^root://[^/]+/", "", eos_url)
    if not store_path.startswith("/"):
        store_path = "/" + store_path
    return store_path


def stat_eos_file(store_path: str) -> Tuple[bool, int, Optional[str]]:
    """Check existence and size of a file on EOS using ``xrdfs stat``.

    Args:
        store_path: Bare /store/... path.

    Returns:
        Tuple of (exists, size_bytes, error_message).
        If the file does not exist or the stat call fails, exists=False,
        size_bytes=0, and error_message contains a description.
    """
    host = EOS_ROOT.replace("root://", "").rstrip("/")
    cmd = ["xrdfs", host, "stat", store_path]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return False, 0, f"xrdfs stat timed out for {store_path}"
    except FileNotFoundError:
        return False, 0, "xrdfs not found; ensure XRootD client is installed"

    if result.returncode != 0:
        return False, 0, result.stderr.strip() or result.stdout.strip()

    # Parse "Size: NNNNN" from stat output
    for line in result.stdout.splitlines():
        if line.strip().startswith("Size:"):
            try:
                size_bytes = int(line.split(":")[1].strip())
                return True, size_bytes, None
            except (IndexError, ValueError):
                return True, 0, f"Could not parse size from: {line!r}"

    # File found but size not reported — treat as existing with unknown size
    return True, 0, "xrdfs stat returned no Size line"


# ============================================================================
# Optional ROOT Validity Check
# ============================================================================

def check_root_validity(store_path: str) -> Tuple[bool, Optional[str]]:
    """Open the ROOT file via XRootD and verify it contains a cutflow histogram.

    This requires ROOT to be importable in the current environment and the file
    to be reachable via XRootD.

    Args:
        store_path: Bare /store/... path.

    Returns:
        Tuple of (is_valid, error_message).
    """
    try:
        import ROOT  # type: ignore
        ROOT.gErrorIgnoreLevel = ROOT.kError  # Suppress ROOT warnings

        xrd_url = f"{EOS_ROOT}/{store_path}"
        root_file = ROOT.TFile.Open(xrd_url, "READ")

        if not root_file or root_file.IsZombie():
            return False, f"TFile.Open failed or zombie: {xrd_url}"

        for name in ("h_cutflow", "h_cutflow_weighted"):
            if root_file.Get(name):
                root_file.Close()
                return True, None

        root_file.Close()
        return False, "Neither h_cutflow nor h_cutflow_weighted found"

    except ImportError:
        return False, "ROOT not importable in current environment"
    except Exception as exc:
        return False, str(exc)


# ============================================================================
# Main Checking Logic
# ============================================================================

def check_single_skim(
    batch_id: str,
    dataset: str,
    eos_path: str,
    do_root_check: bool = False,
) -> SkimCheckResult:
    """Check a single skim file.

    Args:
        batch_id: Batch identifier from the manifest.
        dataset: Dataset/process name.
        eos_path: Full EOS URL of the skim file.
        do_root_check: If True, also open the ROOT file and check contents.

    Returns:
        Populated SkimCheckResult.
    """
    store_path = _store_path_from_eos_url(eos_path)
    result = SkimCheckResult(
        batch_id=batch_id,
        dataset=dataset,
        eos_path=eos_path,
        store_path=store_path,
    )

    exists, size_bytes, error = stat_eos_file(store_path)
    result.exists = exists
    result.size_bytes = size_bytes
    result.error = error

    if exists and size_bytes > 0 and do_root_check:
        valid, root_error = check_root_validity(store_path)
        result.root_valid = valid
        if not valid:
            result.error = root_error

    return result


def check_all_skims(
    manifest: dict,
    do_root_check: bool = False,
    verbose: bool = True,
) -> SkimCheckReport:
    """Check all skim output files listed in a skim manifest.

    Args:
        manifest: Parsed manifest dict (as loaded from manifest_2024.json).
        do_root_check: If True, validate ROOT file contents for existing files.
        verbose: If True, print per-file status to stdout.

    Returns:
        Populated SkimCheckReport.
    """
    manifest_path = manifest.get("_source_path", "unknown")
    report = SkimCheckReport(manifest_path=manifest_path)

    for dataset_key, dataset_info in manifest["datasets"].items():
        dataset_name = dataset_info.get("process", dataset_key)
        batches = dataset_info.get("batches", {})

        for batch_id, batch in batches.items():
            eos_path = batch["output_path"]
            result = check_single_skim(batch_id, dataset_name, eos_path, do_root_check)
            report.add(result)

            if verbose:
                status = "OK" if result.ok else ("MISSING" if not result.exists else ("EMPTY" if result.size_bytes == 0 else "INVALID"))
                size_str = f"{result.size_gb:.2f} GB" if result.exists else "—"
                print(f"  [{status:7s}] {batch_id} | {size_str}" + (f" | {result.error}" if result.error else ""))

    return report


# ============================================================================
# CLI
# ============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check skim output files listed in a preselection manifest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 check_skim_outputs.py --manifest manifest_2024.json
  python3 check_skim_outputs.py --manifest manifest_2024.json --report skim_report.json
  python3 check_skim_outputs.py --manifest manifest_2024.json --check-root
        """,
    )
    parser.add_argument("--manifest", required=True, help="Path to the preselection manifest JSON")
    parser.add_argument("--report", default=None, help="Optional path to write a JSON report")
    parser.add_argument("--check-root", action="store_true", help="Also validate ROOT file contents (requires ROOT)")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file output")

    args = parser.parse_args()

    try:
        with open(args.manifest) as f:
            manifest = json.load(f)
        manifest["_source_path"] = args.manifest
    except FileNotFoundError:
        print(f"ERROR: manifest not found: {args.manifest}")
        return 1
    except json.JSONDecodeError as exc:
        print(f"ERROR: failed to parse manifest: {exc}")
        return 1

    print("=" * 80)
    print("Skim Output Checker")
    print("=" * 80)
    print(f"Manifest:   {args.manifest}")
    print(f"Campaign:   {manifest.get('campaign', 'unknown')}")
    print(f"Year:       {manifest.get('year', 'unknown')}")
    print(f"ROOT check: {'yes' if args.check_root else 'no'}")
    print("=" * 80 + "\n")

    report = check_all_skims(manifest, do_root_check=args.check_root, verbose=not args.quiet)

    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"  Total checked:   {report.total}")
    print(f"  OK:              {report.n_ok}")
    print(f"  Missing:         {report.n_missing}")
    print(f"  Empty (0 B):     {report.n_empty}")
    if args.check_root:
        print(f"  ROOT invalid:    {report.n_root_invalid}")

    if report.n_missing > 0:
        print("\nMissing batches:")
        for r in report.bad_results():
            if not r.exists:
                print(f"  {r.batch_id}  ({r.dataset})")

    if args.report:
        serialisable = {
            "manifest_path": report.manifest_path,
            "total": report.total,
            "n_ok": report.n_ok,
            "n_missing": report.n_missing,
            "n_empty": report.n_empty,
            "n_root_invalid": report.n_root_invalid,
            "results": {bid: asdict(r) for bid, r in report.results.items()},
        }
        with open(args.report, "w") as f:
            json.dump(serialisable, f, indent=2)
        print(f"\nReport written to {args.report}")

    return 0 if report.n_missing == 0 and report.n_empty == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
