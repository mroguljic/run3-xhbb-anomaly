#!/usr/bin/env python3
"""
Template manifest generator.

Reads the preselection skim manifest (manifest_2024.json), validates that
skim output files exist and have non-zero size (using check_skim_outputs.py),
and groups valid skim files into template batches whose total input size does
not exceed config.TEMPLATE_BATCH_SIZE GB.

The resulting template manifest follows the same high-level structure as the
skim manifest so that generate_submission.py and the condor framework can
consume it with minimal changes.

Template manifest structure:
{
  "campaign": "20260401",
  "source_skim_manifest": "manifest_2024.json",
  "generated_at": "...",
  "year": "2024",
  "template_batch_size_gb": 3.0,
  "datasets": {
    "QCD-4Jets_HT-1000to1200": {
      "n_chunks": 4,
      "n_skims_total": 90,
      "n_skims_missing": 2,
      "batches": {
        "QCD-4Jets_HT-1000to1200_tpl_chunk_0": {
          "skim_batch_ids": ["QCD-4Jets_HT-1000to1200_batch_0", ...],
          "skim_paths": ["root://cmseos.fnal.gov//store/.../...root", ...],
          "total_size_gb": 2.87,
          "output_path": "root://cmseos.fnal.gov//store/.../templates/QCD-4Jets_HT-1000to1200/templates_QCD-4Jets_HT-1000to1200_tpl_chunk_0.root"
        },
        ...
      }
    },
    ...
  }
}

Usage:
    python3 generate_template_manifest.py \\
        --skim-manifest manifest_2024.json \\
        --output template_manifest_2024.json

    # Also validate ROOT file contents (slower, requires ROOT in current env):
    python3 generate_template_manifest.py \\
        --skim-manifest manifest_2024.json \\
        --output template_manifest_2024.json \\
        --check-root
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.check_skim_outputs import SkimCheckResult, check_all_skims
from condor.config import OUTPUT, TEMPLATE_BATCH_SIZE, CAMPAIGN, get_store_eos_path


# ============================================================================
# Chunking
# ============================================================================

def chunk_skims_by_size(
    valid_results: List[SkimCheckResult],
    batch_size_gb: float,
) -> List[List[SkimCheckResult]]:
    """Group skim check results into chunks whose combined size is <= batch_size_gb.

    A single skim file that exceeds batch_size_gb on its own is placed in a
    chunk by itself and a warning is emitted — this should not happen in
    practice but avoids silent data loss.

    Args:
        valid_results: Ordered list of passing SkimCheckResult objects.
        batch_size_gb: Maximum total size per chunk in GB.

    Returns:
        List of chunks; each chunk is a list of SkimCheckResult objects.
    """
    chunks: List[List[SkimCheckResult]] = []
    current_chunk: List[SkimCheckResult] = []
    current_size_gb: float = 0.0

    for result in valid_results:
        file_size_gb = result.size_gb

        if file_size_gb > batch_size_gb:
            print(f"  WARNING: {result.batch_id} is {file_size_gb:.2f} GB, "
                  f"which exceeds the batch limit of {batch_size_gb} GB — placing it alone")

        # Start a new chunk if adding this file would exceed the limit,
        # unless the current chunk is empty (avoid infinite loop for oversized files)
        if current_chunk and (current_size_gb + file_size_gb > batch_size_gb):
            chunks.append(current_chunk)
            current_chunk = []
            current_size_gb = 0.0

        current_chunk.append(result)
        current_size_gb += file_size_gb

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


# ============================================================================
# Manifest Generation
# ============================================================================

def build_template_manifest(
    skim_manifest: dict,
    skim_manifest_path: str,
    batch_size_gb: float,
    verbose: bool = True,
) -> dict:
    """Build the template manifest by checking skims and chunking by size.

    Args:
        skim_manifest: Parsed skim manifest dict.
        skim_manifest_path: Path to the skim manifest file (for provenance).
        batch_size_gb: Maximum total skim size per template batch in GB.
        verbose: Print per-file progress during skim checking.

    Returns:
        Template manifest dict ready to be written as JSON.
    """
    year = skim_manifest.get("year", "unknown")
    campaign = skim_manifest.get("campaign", CAMPAIGN)

    template_manifest: dict = {
        "campaign": campaign,
        "source_skim_manifest": skim_manifest_path,
        "generated_at": datetime.now().isoformat(),
        "year": year,
        "template_batch_size_gb": batch_size_gb,
        "datasets": {},
    }

    total_template_batches = 0
    total_skims_missing = 0

    # Iterate over datasets in the skim manifest, preserving order
    for dataset_key, dataset_info in skim_manifest["datasets"].items():
        dataset_name = dataset_info.get("process", dataset_key)
        n_skim_batches = len(dataset_info.get("batches", {}))

        print(f"\nDataset: {dataset_key}  (process: {dataset_name},  {n_skim_batches} skim batches)")

        # Build a mini-manifest for just this dataset so check_all_skims can be reused
        single_dataset_manifest = {
            "_source_path": skim_manifest_path,
            "datasets": {dataset_key: dataset_info},
        }
        report = check_all_skims(single_dataset_manifest, verbose=verbose)

        valid = report.ok_results()
        bad = report.bad_results()

        n_missing = len(bad)
        total_skims_missing += n_missing

        if n_missing:
            print(f"  -> {n_missing} skim(s) missing/invalid — they will be excluded from templating")

        if not valid:
            print(f"  -> No valid skims for {dataset_key}, skipping")
            continue

        # Sort valid skims by batch_id for reproducibility
        valid.sort(key=lambda r: r.batch_id)

        # Chunk by size
        chunks = chunk_skims_by_size(valid, batch_size_gb)
        print(f"  -> {len(valid)} valid skims ({sum(r.size_gb for r in valid):.2f} GB total)"
              f" -> {len(chunks)} template chunk(s)")

        batches: Dict[str, dict] = {}
        for chunk_index, chunk in enumerate(chunks):
            chunk_id = f"{dataset_key}_tpl_chunk_{chunk_index}"
            total_size_gb = sum(r.size_gb for r in chunk)
            output_store_path = f"{OUTPUT['templates_dir']}/{dataset_name}/templates_{chunk_id}.root"
            eos_output = get_store_eos_path(output_store_path)

            batches[chunk_id] = {
                "skim_batch_ids": [r.batch_id for r in chunk],
                "skim_paths": [r.eos_path for r in chunk],
                "n_skims": len(chunk),
                "total_size_gb": round(total_size_gb, 4),
                "output_path": eos_output,
            }
            total_template_batches += 1

        template_manifest["datasets"][dataset_key] = {
            "process": dataset_name,
            "n_chunks": len(chunks),
            "n_skims_total": n_skim_batches,
            "n_skims_valid": len(valid),
            "n_skims_missing": n_missing,
            "batches": batches,
        }

    template_manifest["_summary"] = {
        "total_template_batches": total_template_batches,
        "total_skims_missing": total_skims_missing,
    }

    return template_manifest


# ============================================================================
# CLI
# ============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a template manifest from a preselection skim manifest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 generate_template_manifest.py \\
      --skim-manifest manifest_2024.json \\
      --output template_manifest_2024.json

  python3 generate_template_manifest.py \\
      --skim-manifest manifest_2024.json \\
      --output template_manifest_2024.json \\
      --batch-size 5.0 \\
      --check-root
        """,
    )
    parser.add_argument("--skim-manifest", required=True, help="Path to the preselection skim manifest JSON")
    parser.add_argument("--output", required=True, help="Output template manifest JSON file")
    parser.add_argument(
        "--batch-size",
        type=float,
        default=TEMPLATE_BATCH_SIZE,
        help=f"Maximum total skim size per template batch in GB (default: {TEMPLATE_BATCH_SIZE})",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=None,
        help="Process only these dataset keys (default: all datasets in the skim manifest)",
    )
    parser.add_argument("--check-root", action="store_true", help="Also validate ROOT file contents (requires ROOT)")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file output from the skim checker")

    args = parser.parse_args()

    # Load skim manifest
    try:
        with open(args.skim_manifest) as f:
            skim_manifest = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: skim manifest not found: {args.skim_manifest}")
        return 1
    except json.JSONDecodeError as exc:
        print(f"ERROR: failed to parse manifest: {exc}")
        return 1

    # Filter datasets if requested
    if args.datasets:
        unknown = [d for d in args.datasets if d not in skim_manifest["datasets"]]
        if unknown:
            print(f"ERROR: unknown dataset key(s): {', '.join(unknown)}")
            print(f"Available: {', '.join(skim_manifest['datasets'].keys())}")
            return 1
        skim_manifest["datasets"] = {k: v for k, v in skim_manifest["datasets"].items() if k in args.datasets}

    print("=" * 80)
    print("Template Manifest Generation")
    print("=" * 80)
    print(f"Skim manifest:    {args.skim_manifest}")
    print(f"Campaign:         {skim_manifest.get('campaign', 'unknown')}")
    print(f"Year:             {skim_manifest.get('year', 'unknown')}")
    print(f"Datasets:         {len(skim_manifest['datasets'])}")
    print(f"Batch size limit: {args.batch_size} GB")
    print(f"Output:           {args.output}")
    print("=" * 80)

    template_manifest = build_template_manifest(
        skim_manifest=skim_manifest,
        skim_manifest_path=args.skim_manifest,
        batch_size_gb=args.batch_size,
        verbose=not args.quiet,
    )

    summary = template_manifest.get("_summary", {})
    print("\n" + "=" * 80)
    print("Template Manifest Summary")
    print("=" * 80)
    print(f"  Datasets processed:       {len(template_manifest['datasets'])}")
    print(f"  Total template batches:   {summary.get('total_template_batches', 0)}")
    print(f"  Total skims missing:      {summary.get('total_skims_missing', 0)}")

    with open(args.output, "w") as f:
        json.dump(template_manifest, f, indent=2)
    print(f"\nTemplate manifest written to {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
