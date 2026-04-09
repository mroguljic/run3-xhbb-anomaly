#!/usr/bin/env python3
"""
Phase 2: Preselection job executor for HTCondor batches.

Handles:
  - Input file copying (xrdcp to local scratch)
  - Preselection execution
  - Output transfer (xrdcp to /STORE)
  - Status logging (JSON)

Usage:
    python3 preselection_job_batch.py \
        --batch-id TTto4Q_batch_0 \
        --files root://path/file1.root root://path/file2.root \
        --year 2024 \
        --output /STORE/matej/run3-xhbb-anomaly/skims/preselection_TTto4Q_batch_0.root

Environment:
    Assumes TIMBER environment is available (loaded in container)
    Assumes preselection.py exists in parent directory
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
    
    If a file is already a local path (doesn't start with root://), copy it directly.
    Otherwise use xrdcp for remote files.
    
    Args:
        file_paths: List of file paths (can be local or XRD URLs)
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
        
        # If file is already local, just copy it
        if not file_path.startswith("root://"):
            if not Path(file_path).exists():
                raise RuntimeError(f"Local file not found: {file_path}")
            cmd = f"cp '{file_path}' '{local_path}'"
            print(f"[{i}/{len(file_paths)}] Copying local: {local_filename}")
        else:
            # Use xrdcp for remote files
            cmd = f"xrdcp '{file_path}' '{local_path}'"
            print(f"[{i}/{len(file_paths)}] Copying remote: {local_filename}")
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            error = result.stderr or result.stdout
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
    Transfer output ROOT file from local scratch to /STORE.
    
    Uses xrdcp if remote path is XRD URL, otherwise uses cp.
    
    Args:
        local_output: Path to output file in local scratch
        remote_output: Path to output file on /STORE
    
    Raises:
        RuntimeError: If transfer fails
    """
    # Ensure remote directory exists
    remote_dir = Path(remote_output).parent
    if not str(remote_dir).startswith("root://"):
        remote_dir.mkdir(parents=True, exist_ok=True)
    
    # Choose transfer method based on path
    if str(remote_output).startswith("root://"):
        cmd = f"xrdcp '{local_output}' '{remote_output}'"
        print(f"Transferring via xrdcp...")
    else:
        cmd = f"cp '{local_output}' '{remote_output}'"
        print(f"Transferring via cp...")
    
    print(f"Local:  {local_output}")
    print(f"Remote: {remote_output}\n")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        error = result.stderr or result.stdout
        raise RuntimeError(f"Transfer failed:\n{error}")
    
    # Verify transfer
    if str(remote_output).startswith("root://"):
        print(f"Output transferred to {remote_output} (XRD)")
    else:
        if Path(remote_output).exists():
            size_mb = Path(remote_output).stat().st_size / (1024 * 1024)
            print(f"Output transferred to {remote_output} ({size_mb:.1f} MB)")
        else:
            raise RuntimeError(f"Transfer verification failed: {remote_output} not found")


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Preselection batch job executor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single file
  python3 preselection_job_batch.py \\
      --batch-id batch_0 \\
      --files root://path/file1.root \\
      --year 2024 \\
      --output /STORE/path/preselection_batch_0.root

  # Multiple files (space-separated)
  python3 preselection_job_batch.py \\
      --batch-id batch_1 \\
      --files root://path/file1.root root://path/file2.root root://path/file3.root \\
      --year 2024 \\
      --output /STORE/path/preselection_batch_1.root
        """
    )
    parser.add_argument("--batch-id", required=True, help="Unique batch identifier")
    parser.add_argument("--files", nargs="+", required=True, help="Input files (can be local or XRD URLs)")
    parser.add_argument("--year", required=True, help="Data year (e.g., 2024)")
    parser.add_argument("--output", required=True, help="Output ROOT file path")
    
    args = parser.parse_args()
    
    batch_id = args.batch_id
    input_files = args.files
    year = args.year
    output_path = args.output
    
    # Extract dataset name from batch ID (assumed format: DATASET_batch_N or DATASET_YEAR_batch_N)
    dataset_name = "_".join(batch_id.split("_")[:-2]) or batch_id
    
    start_time = time.time()
    
    print("=" * 80)
    print("HTCondor Preselection Job Executor")
    print("=" * 80)
    print(f"Batch ID:  {batch_id}")
    print(f"Dataset:   {dataset_name}")
    print(f"Year:      {year}")
    print(f"Files:     {len(input_files)}")
    print(f"Output:    {output_path}")
    print(f"Start:     {datetime.fromtimestamp(start_time).isoformat()}")
    print("=" * 80 + "\n")
    
    try:
        # Check if output already exists
        if str(output_path).startswith("root://"):
            print(f"Note: Cannot check if remote output exists (XRD path)")
        else:
            if Path(output_path).exists():
                print(f"Output already exists: {output_path}")
                print("Skipping preselection (resuming previous job)\n")
                
                # Validate existing output
                events = validate_output_file(output_path)
                
                end_time = time.time()
                write_metadata_json(
                    batch_id=batch_id,
                    dataset_name=dataset_name,
                    year=year,
                    input_files=input_files,
                    output_path=output_path,
                    status="skipped_already_exists",
                    events=events,
                    start_time=start_time,
                    end_time=end_time,
                )
                
                print(f"\nJob skipped (already exists) in {end_time - start_time:.1f}s")
                return 0
        
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
