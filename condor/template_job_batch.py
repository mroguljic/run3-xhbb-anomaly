#!/usr/bin/env python3
"""
Template job executor for HTCondor batches.

Reads a template manifest batch, stages skim files to the worker node,
runs selection_and_templating.py, and transfers the output to EOS.

Usage:
    python3 template_job_batch.py \
        --batch-id QCD-4Jets_HT-1000to1200_tpl_batch_0 \
        --manifest template_manifest_2024.json
"""

import argparse
import json
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.config import get_store_xrd_path


# ============================================================================
# File Operations
# ============================================================================

def stage_input_files(eos_paths: List[str], staging_dir: str) -> List[str]:
    """Copy skim files from EOS to local scratch using xrdcp.

    Args:
        eos_paths: List of EOS URLs (root://cmseos.fnal.gov//store/...).
        staging_dir: Local directory to copy files into.

    Returns:
        List of local file paths.

    Raises:
        RuntimeError: If any xrdcp call fails.
    """
    local_files = []
    for i, eos_path in enumerate(eos_paths, 1):
        local_path = Path(staging_dir) / Path(eos_path).name
        cmd = f"xrdcp '{eos_path}' '{local_path}'"
        print(f"[{i}/{len(eos_paths)}] {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"xrdcp failed for {eos_path}:\n{result.stderr or result.stdout}")
        if not local_path.exists():
            raise RuntimeError(f"File not found after copy: {local_path}")
        print(f"  -> {local_path.name}  ({local_path.stat().st_size / 1024**2:.1f} MB)")
        local_files.append(str(local_path))
    print(f"Staged {len(local_files)} skim(s) successfully\n")
    return local_files


def create_filelist_txt(file_paths: List[str], output_path: str) -> None:
    """Write a newline-separated list of file paths to a .txt file.

    Args:
        file_paths: List of local file paths.
        output_path: Destination .txt file path.
    """
    with open(output_path, "w") as f:
        for path in file_paths:
            f.write(path + "\n")
    print(f"Filelist written: {output_path}  ({len(file_paths)} files)")


def transfer_output_to_store(local_output: str, remote_output: str) -> None:
    """Transfer the output ROOT file to EOS via xrdcp.

    Args:
        local_output: Local path to the output ROOT file.
        remote_output: EOS URL destination (root://cmseos.fnal.gov//store/...).

    Raises:
        RuntimeError: If xrdcp fails.
    """
    cmd = f"xrdcp '{local_output}' '{remote_output}'"
    print(f"Transferring output...\n  {local_output}\n  -> {remote_output}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"xrdcp transfer failed:\n{result.stderr or result.stdout}")
    print("Transfer complete\n")


# ============================================================================
# Templating Execution
# ============================================================================

def run_templating(filelist_txt: str, output_file: str, year: str) -> None:
    """Run selection_and_templating.py on the staged skim filelist.

    Args:
        filelist_txt: Path to the .txt filelist of local skim files.
        output_file: Path for the output template ROOT file.
        year: Data-taking year.

    Raises:
        RuntimeError: If the script exits with a non-zero return code.
    """
    cmd = f"python3 selection_and_templating.py -i {filelist_txt} -o {output_file} -y {year}"
    print(f"Running templating...\nCommand: {cmd}\n")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print("=== stdout ===")
        print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Templating failed:\n{result.stderr or result.stdout or 'unknown error'}")
    print("Templating completed successfully\n")


# ============================================================================
# Main
# ============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Template job executor for HTCondor",
        epilog="""
Examples:
  python3 template_job_batch.py \\
      --batch-id QCD-4Jets_HT-1000to1200_tpl_batch_0 \\
      --manifest template_manifest_2024.json
        """,
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Template batch identifier (from manifest)",
    )
    parser.add_argument("--manifest", required=True, help="Path to the template manifest JSON")
    args = parser.parse_args()

    batch_id = args.batch_id
    manifest_path = args.manifest

    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: manifest not found: {manifest_path}")
        return 1
    except json.JSONDecodeError as exc:
        print(f"ERROR: failed to parse manifest: {exc}")
        return 1

    # Find batch across all datasets
    batch = None
    dataset_name = None
    for ds_info in manifest["datasets"].values():
        if batch_id in ds_info["batches"]:
            batch = ds_info["batches"][batch_id]
            dataset_name = ds_info.get("process", batch_id)
            break

    if batch is None:
        print(f"ERROR: batch '{batch_id}' not found in manifest")
        return 1

    skim_paths = batch["skim_paths"]
    year = manifest["year"]
    output_path = batch["output_path"]
    start_time = time.time()

    print("=" * 80)
    print("HTCondor Template Job Executor")
    print("=" * 80)
    print(f"Batch ID:   {batch_id}")
    print(f"Dataset:    {dataset_name}")
    print(f"Year:       {year}")
    print(f"Skims:      {len(skim_paths)}")
    print(f"Size:       {batch.get('total_size_gb', '?')} GB")
    print(f"Output:     {output_path}")
    print(f"Start:      {datetime.fromtimestamp(start_time).isoformat()}")
    print("=" * 80 + "\n")

    try:
        with tempfile.TemporaryDirectory(prefix=f"template_{batch_id}_") as staging_dir:
            print(f"Staging directory: {staging_dir}\n")

            print("=== Staging Input Skims ===\n")
            local_files = stage_input_files(skim_paths, staging_dir)

            filelist_path = str(Path(staging_dir) / f"filelist_{batch_id}.txt")
            create_filelist_txt(local_files, filelist_path)
            print()

            local_output = str(Path(staging_dir) / f"templates_{batch_id}.root")

            print("=== Running Templating ===\n")
            run_templating(filelist_path, local_output, year)

            print("=== Transferring Output ===\n")
            transfer_output_to_store(local_output, output_path)

        end_time = time.time()
        print("=" * 80)
        print(f"Job completed successfully in {end_time - start_time:.1f}s")
        print("=" * 80)
        return 0

    except Exception as exc:
        end_time = time.time()
        print(f"\n{'=' * 80}\nERROR: {exc}\n{'=' * 80}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
