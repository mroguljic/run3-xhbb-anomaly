#!/usr/bin/env python3
"""
Phase 2: Preselection job executor for HTCondor batches.

Reads batch metadata from a manifest JSON file and executes preselection.

Handles:
  - Input file copying (xrdcp to local scratch)
  - Preselection execution
  - Output transfer (xrdcp to /STORE)
  - Status logging (JSON)

Usage:
    python3 preselection_job_batch.py \
        --batch-id TTto4Q_batch_0 \
        --manifest-path manifest.json

Environment:
    Assumes TIMBER environment is available (loaded in container)
    Assumes preselection.py exists in parent directory
    Assumes manifest.json is in current directory or specified path
"""

import json
import sys
import os
import argparse
import subprocess
import tempfile
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


# ============================================================================
# Logging and Metadata
# ============================================================================

def write_metadata_json(
    batch_id: str,
    dataset_name: str,
    year: str,
    input_files: List[str],
    output_path: str,
    status: str,
    events: int = 0,
    error_msg: str = None,
    start_time: float = None,
    end_time: float = None,
) -> None:
    """
    Write job metadata to JSON file.
    
    JSON file is named preselection_<batch_id>.json and will be transferred
    back to submit host by HTCondor (via transfer_output_files).
    
    Args:
        batch_id: Unique batch identifier
        dataset_name: Name of dataset (e.g., "TTto4Q")
        year: Data year (e.g., "2024")
        input_files: List of input file paths
        output_path: Output ROOT file path
        status: Job status ("success", "failed", "skipped_already_exists")
        events: Number of events processed
        error_msg: Error message if status is "failed"
        start_time: Unix timestamp of job start
        end_time: Unix timestamp of job end
    """
    metadata = {
        "batch_id": batch_id,
        "dataset_name": dataset_name,
        "year": year,
        "input_files": input_files,
        "input_file_count": len(input_files),
        "output_path": output_path,
        "status": status,
        "events_processed": events,
        "error_msg": error_msg,
        "start_time": datetime.fromtimestamp(start_time).isoformat() if start_time else None,
        "end_time": datetime.fromtimestamp(end_time).isoformat() if end_time else None,
        "duration_seconds": round(end_time - start_time, 1) if start_time and end_time else None,
    }
    
    # Write to current directory (HTCondor will transfer it back)
    output_file = f"preselection_{batch_id}.json"
    with open(output_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Metadata written to {output_file}")


# ============================================================================
# File Operations
# ============================================================================

def stage_input_files(file_paths: List[str], staging_dir: str) -> List[str]:
    """
    Copy files from storage to local staging directory using xrdcp.
    
    Args:
        file_paths: List of XRD file paths (root://... URLs)
        staging_dir: Local directory to stage files in
    
    Returns:
        List of local file paths after staging
    
    Raises:
        RuntimeError: If any file copy fails
    """
    local_files = []
    
    for i, file_path in enumerate(file_paths, 1):
        local_filename = Path(file_path).name
        local_path = Path(staging_dir) / local_filename
        
        cmd = f"xrdcp '{file_path}' '{local_path}'"
        print(f"[{i}/{len(file_paths)}] Copying: {local_filename}")
        
        print(cmd)
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            error = result.stderr or result.stdout
            print f"ERROR copying {file_path}:\n{error}"
            raise RuntimeError(f"Failed to copy {file_path}:\n{error}")
        
        # Verify file was copied
        if not local_path.exists():
            raise RuntimeError(f"File copy verification failed: {local_path}")
        
        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"{local_filename} ({file_size_mb:.1f} MB)")
        
        local_files.append(str(local_path))
    
    print(f"Staged {len(local_files)} files successfully\n")
    return local_files


def create_filelist_txt(file_paths: List[str], output_file: str) -> str:
    """
    Create a .txt file containing line-separated file paths.
    
    TIMBER Analyzer can accept a .txt file with file paths and will chain
    the files together internally.
    
    Args:
        file_paths: List of file paths
        output_file: Path to output .txt file
    
    Returns:
        Path to created filelist
    """
    with open(output_file, 'w') as f:
        for file_path in file_paths:
            f.write(file_path + '\n')
    
    print(f"Created filelist: {output_file}")
    print(f"  Files: {len(file_paths)}")
    return output_file


# ============================================================================
# Preselection Execution
# ============================================================================

def run_preselection(filelist_txt: str, output_file: str, year: str) -> int:
    """
    Execute preselection.py with the given filelist.
    
    Invokes the preselection script and captures output.
    Assumes preselection.py exists in the parent directory.
    
    Args:
        filelist_txt: Path to .txt file with input file list
        output_file: Path to output ROOT file
        year: Data year (e.g., "2024")
    
    Returns:
        Event count from validation
    
    Raises:
        RuntimeError: If preselection fails or output validation fails
    """
    cmd = f"python3 preselection.py -i {filelist_txt} -o {output_file} -y {year}"
    
    print(f"Running preselection...")
    print(f"Command: {cmd}\n")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    # Print preselection output
    if result.stdout:
        print("=== Preselection stdout ===")
        print(result.stdout)
    
    if result.returncode != 0:
        error_msg = result.stderr or result.stdout or "Unknown error"
        raise RuntimeError(f"Preselection failed:\n{error_msg}")
    
    print("Preselection completed successfully\n")
    
    # Validate output
    events = validate_output_file(output_file)
    return events


def validate_output_file(output_file: str) -> int:
    """
    Validate output ROOT file and extract event count.
    
    Reads the cutflow histogram to get the event count.
    
    Args:
        output_file: Path to output ROOT file
    
    Returns:
        Event count from cutflow histogram
    
    Raises:
        RuntimeError: If file is invalid or histogram not found
    """
    try:
        import ROOT
        
        root_file = ROOT.TFile.Open(output_file, "READ")
        if not root_file or root_file.IsZombie():
            raise RuntimeError(f"Cannot open {output_file}")
        
        # Look for cutflow histogram
        hist = root_file.Get("h_cutflow")
        if not hist:
            hist = root_file.Get("h_cutflow_weighted")
        
        if not hist:
            raise RuntimeError(f"No cutflow histogram found in {output_file}")
        
        # Get event count from first bin
        events = int(hist.GetBinContent(1))
        root_file.Close()
        
        print(f"Output validation passed: {events} events\n")
        return events
        
    except Exception as e:
        raise RuntimeError(f"Output validation failed: {e}")


# ============================================================================
# Output Transfer
# ============================================================================

def transfer_output_to_store(local_output: str, remote_output: str) -> None:
    """
    Transfer output ROOT file from local scratch to /STORE using xrdcp.
    
    Args:
        local_output: Path to output file in local scratch
        remote_output: XRD URL to output file on /STORE (root://cmseos.fnal.gov/store/...)
    
    Raises:
        RuntimeError: If transfer fails
    """
    if not str(remote_output).startswith("root://"):
        raise ValueError(f"Remote output must be XRD URL, got: {remote_output}")
    
    cmd = f"xrdcp '{local_output}' '{remote_output}'"
    print(f"Transferring via xrdcp...")
    print(f"Local:  {local_output}")
    print(f"Remote: {remote_output}\n")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        error = result.stderr or result.stdout
        raise RuntimeError(f"Transfer failed:\n{error}")
    
    print(f"Output transferred to {remote_output}")


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Preselection batch job executor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 preselection_job_batch.py \\
      --batch-id TTto4Q_batch_0 \\
      --manifest manifest.json

  python3 preselection_job_batch.py \\
      --batch-id QCD-4Jets_HT-800to1000_batch_2 \\
      --manifest /path/to/manifest_20260401.json
        """
    )
    parser.add_argument("--batch-id", required=True, help="Unique batch identifier (from manifest)")
    parser.add_argument("--manifest", required=True, help="Path to batch manifest JSON file")
    
    args = parser.parse_args()
    
    batch_id = args.batch_id
    manifest_path = args.manifest
    
    # Load manifest
    try:
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Manifest file not found: {manifest_path}")
        return 1
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse manifest: {e}")
        return 1
    
    # Find batch in manifest (search all datasets)
    batch = None
    dataset_name = None
    
    for ds_name, dataset_info in manifest["datasets"].items():
        if batch_id in dataset_info["batches"]:
            batch = dataset_info["batches"][batch_id]
            dataset_name = ds_name
            break
    
    if batch is None:
        print(f"ERROR: Batch '{batch_id}' not found in manifest")
        available_batches = []
        for ds_name, dataset_info in manifest["datasets"].items():
            available_batches.extend(dataset_info["batches"].keys())
        print(f"Available batches: {', '.join(sorted(available_batches))}")
        return 1
    
    input_files = batch["files"]
    year = manifest["year"]
    output_path = batch["output_path"]
    
    start_time = time.time()
    
    print("=" * 80)
    print("HTCondor Preselection Job Executor")
    print("=" * 80)
    print(f"Batch ID:   {batch_id}")
    print(f"Dataset:    {dataset_name}")
    print(f"Year:       {year}")
    print(f"Files:      {len(input_files)}")
    print(f"Events:     {batch.get('n_events', 'unknown'):,}")
    print(f"Output:     {output_path}")
    print(f"Manifest:   {manifest_path}")
    print(f"Start:      {datetime.fromtimestamp(start_time).isoformat()}")
    print("=" * 80 + "\n")
    
    try:
        # Create temporary staging directory for input files
        with tempfile.TemporaryDirectory(prefix=f"preselection_{batch_id}_") as staging_dir:
            print(f"Staging directory: {staging_dir}\n")
            
            # Stage input files locally
            print("=== Staging Input Files ===\n")
            local_files = stage_input_files(input_files, staging_dir)
            
            # Create filelist for TIMBER Analyzer
            filelist_path = Path(staging_dir) / f"filelist_{batch_id}.txt"
            create_filelist_txt(local_files, str(filelist_path))
            print()
            
            # Create local output path in staging directory
            local_output_file = Path(staging_dir) / f"preselection_{batch_id}.root"
            
            # Run preselection
            print("=== Running Preselection ===\n")
            events = run_preselection(str(filelist_path), str(local_output_file), year)
            
            # Transfer output to storage
            print("=== Transferring Output ===\n")
            transfer_output_to_store(str(local_output_file), output_path)
            print()
        
        # Success
        end_time = time.time()
        write_metadata_json(
            batch_id=batch_id,
            dataset_name=dataset_name,
            year=year,
            input_files=input_files,
            output_path=output_path,
            status="success",
            events=events,
            start_time=start_time,
            end_time=end_time,
        )
        
        print("=" * 80)
        print(f"Job completed successfully in {end_time - start_time:.1f}s")
        print("=" * 80)
        return 0
    
    except Exception as e:
        end_time = time.time()
        error_msg = str(e)
        
        print(f"\n{'=' * 80}")
        print(f"ERROR: {error_msg}")
        print("=" * 80 + "\n")
        
        # Log failure
        write_metadata_json(
            batch_id=batch_id,
            dataset_name=dataset_name,
            year=year,
            input_files=input_files,
            output_path=output_path,
            status="failed",
            error_msg=error_msg,
            start_time=start_time,
            end_time=end_time,
        )
        
        return 1


if __name__ == "__main__":
    sys.exit(main())
