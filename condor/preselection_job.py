#!/usr/bin/env python3
"""
Step 3: Preselection job wrapper for Condor.

Downloads files for a batch, runs preselection, validates output, writes metadata JSON.

Usage:
    python condor/preselection_job.py --batch-id TTto4Q_2024_batch_0 --manifest condor/manifest.json --year 2024
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
from typing import Dict, List, Any
import ROOT

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.config import JOB_EXECUTION, OUTPUT


def log_job(batch_id: str, dataset_name: str, year: str, input_files: List[str], 
            output_path: str, status: str, events: int = 0, error_msg: str = None,
            start_time: float = None, end_time: float = None, log_dir: str = "condor/logs") -> None:
    """Write job metadata JSON to logs directory."""
    
    metadata = {
        "batch_id": batch_id,
        "dataset_name": dataset_name,
        "year": year,
        "input_files": input_files,
        "output_path": output_path,
        "status": status,
        "events_processed": events,
        "error_msg": error_msg,
        "start_time": datetime.fromtimestamp(start_time).isoformat() if start_time else None,
        "end_time": datetime.fromtimestamp(end_time).isoformat() if end_time else None,
        "duration_seconds": round(end_time - start_time, 1) if start_time and end_time else None,
    }
    
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / f"preselection_{batch_id}.json"
    
    with open(log_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Logged to {log_file}")


def copy_files_local(file_paths: List[str], dest_dir: str) -> List[str]:
    """Copy files from storage to local directory using xrdcp.
    
    Returns:
        List of local file paths
    """
    local_files = []
    
    for i, file_path in enumerate(file_paths, 1):
        # Construct full XRD URL if needed
        if not file_path.startswith("root://"):
            file_path = f"root://cms-xrd-global.cern.ch/{file_path}"
        
        local_filename = Path(file_path).name
        local_path = Path(dest_dir) / local_filename
        
        cmd = f"xrdcp '{file_path}' '{local_path}'"
        print(f"[{i}/{len(file_paths)}] Copying: {local_filename}")
        
        result = subprocess.run(cmd, shell=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"xrdcp failed for {file_path}: {result.stderr}")
        
        local_files.append(str(local_path))
    
    return local_files


def create_filelist_txt(file_paths: List[str], output_file: str) -> str:
    """Create a .txt file containing line-separated file paths for TIMBER Analyzer.
    
    The Analyzer can accept a .txt file with file paths, which it will chain together
    without needing external hadding.
    
    Returns:
        Path to the created .txt file
    """
    with open(output_file, 'w') as f:
        for file_path in file_paths:
            f.write(file_path + '\n')
    
    print(f"Created filelist: {output_file} with {len(file_paths)} files")
    return output_file


def validate_output(output_file: str) -> int:
    """
    Validate output ROOT file and extract event count.
    
    Returns:
        Event count from cutflow histogram
    """
    try:
        root_file = ROOT.TFile.Open(output_file, "READ")
        if not root_file or root_file.IsZombie():
            raise RuntimeError(f"Cannot open {output_file}")
        
        # Check for expected histograms
        hist_name = "h_cutflow"
        hist = root_file.Get(hist_name)
        if not hist:
            hist_name = "h_cutflow_weighted"
            hist = root_file.Get(hist_name)
        
        if not hist:
            raise RuntimeError(f"No cutflow histogram found in {output_file}")
        
        # Get event count from histogram
        events = int(hist.GetBinContent(1))  # First bin typically has total events
        root_file.Close()
        
        print(f"Output validation passed: {events} events")
        return events
        
    except Exception as e:
        raise RuntimeError(f"Output validation failed: {e}")


def run_preselection(filelist_txt: str, output_file: str, year: str) -> int:
    """
    Run preselection.py directly (assumed that we are inside singularity container).
    
    The filelist_txt is passed to the Analyzer, which chains the files together internally.
    
    Returns:
        Event count from validation
    """
    
    cmd = f"python3 preselection.py -i {filelist_txt} -o {output_file} -y {year}"
    
    print(f"Running preselection...")
    print(f"Command: {cmd}")
    
    result = subprocess.run(cmd, shell=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Preselection failed: {result.stderr}")
    
    print(result.stdout)
    
    # Validate output
    events = validate_output(output_file)
    return events


def copy_output_to_storage(local_output: str, remote_output: str) -> None:
    """Copy ROOT output file from local staging to STORE (lorien server)
    
    Args:
        local_output: Path to local output file in scratch directory
        remote_output: Path to remote output file on /STORE 
    """
    # Ensure remote directory exists
    remote_dir = Path(remote_output).parent
    remote_dir.mkdir(parents=True, exist_ok=True)
    
    cmd = f"cp '{local_output}' '{remote_output}'"
    
    print(f"Copying output file to storage...")
    print(f"Local: {local_output}")
    print(f"Remote: {remote_output}")
    
    result = subprocess.run(cmd, shell=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to copy output to storage: {result.stderr}")
    
    print(f"Output file successfully copied to {remote_output}")


def find_batch_in_manifest(batch_id: str, manifest: Dict) -> Dict:
    """Find batch definition in manifest."""
    
    for dataset in manifest['datasets']:
        for batch in dataset['batches']:
            if batch['batch_id'] == batch_id:
                return {
                    'batch': batch,
                    'dataset_name': dataset['name'],
                    'year': dataset['year']
                }
    
    raise ValueError(f"Batch {batch_id} not found in manifest")


def main():
    parser = argparse.ArgumentParser(description="Preselection job wrapper for Condor")
    parser.add_argument("--batch-id", required=True, help="Batch ID from manifest")
    parser.add_argument("--manifest", default="condor/manifest.json", help="Path to manifest JSON")
    parser.add_argument("--year", default="2024", help="Data year")
    parser.add_argument("--output-dir", default=OUTPUT.get('skims_dir', 'output/skims'), help="Output directory for preselection files")
    parser.add_argument("--log-dir", default=OUTPUT.get('logs_dir', 'condor/logs'), help="Log directory for metadata JSON")
    
    args = parser.parse_args()
    
    # Use Condor scratch directory for staging input files (local to compute node, faster than network)
    working_dir = os.environ.get('_CONDOR_SCRATCH_DIR')
    if not working_dir:
        raise RuntimeError("_CONDOR_SCRATCH_DIR environment variable not set. This job must run under Condor.")
    print(f"Using Condor scratch directory: {working_dir}")
    
    start_time = time.time()
    
    try:
        # Load manifest
        with open(args.manifest) as f:
            manifest = json.load(f)
        
        # Find batch
        batch_info = find_batch_in_manifest(args.batch_id, manifest)
        batch = batch_info['batch']
        dataset_name = batch_info['dataset_name']
        
        input_files = batch['files']
        if not input_files:
            raise ValueError(f"No files in batch {args.batch_id}")
        
        print(f"Processing batch: {args.batch_id}")
        print(f"Dataset: {dataset_name}")
        print(f"Files: {len(input_files)}")
        
        # Use scratch directory subdirectory for staging input files
        staging_dir = Path(working_dir) / f"preselection_{args.batch_id}"
        staging_dir.mkdir(parents=True, exist_ok=True)
        print(f"Staging directory: {staging_dir}")
        
        # Remote output path on /STORE
        Path(args.output_dir).mkdir(parents=True, exist_ok=True)
        remote_output_file = Path(args.output_dir) / f"preselection_{args.batch_id}.root"
        
        # Skip if output already exists on remote storage (safe resume)
        if remote_output_file.exists():
            print(f"Output already exists: {remote_output_file}")
            print("Skipping preselection (job already completed)")
            # Validate existing output and get event count
            events = validate_output(str(remote_output_file))
            # Log as success
            end_time = time.time()
            log_job(
                batch_id=args.batch_id,
                dataset_name=dataset_name,
                year=args.year,
                input_files=input_files,
                output_path=str(remote_output_file),
                status="skipped_already_exists",
                events=events,
                start_time=start_time,
                end_time=end_time,
                log_dir=args.log_dir
            )
            print(f"Job skipped (already exists) in {end_time - start_time:.1f} seconds")
            return
        

        # Copy files locally for faster read performance
        print(f"Copying {len(input_files)} files to local staging directory...")
        local_files = copy_files_local(input_files, str(staging_dir))
        
        # Create filelist with local paths
        # TIMBER Analyzer can accept a .txt file with file paths and chain them internally
        # Much faster than using network processing in TChain
        filelist_path = staging_dir / f"filelist_{args.batch_id}.txt"
        create_filelist_txt(local_files, str(filelist_path))

        # Run preselection and write output to local scratch disk (faster I/O)
        local_output_file = staging_dir / f"preselection_{args.batch_id}.root"
        events = run_preselection(str(filelist_path), str(local_output_file), args.year)
        
        # Copy output file to remote storage on /STORE
        copy_output_to_storage(str(local_output_file), str(remote_output_file))
        
        # Success
        end_time = time.time()
        log_job(
            batch_id=args.batch_id,
            dataset_name=dataset_name,
            year=args.year,
            input_files=input_files,
            output_path=str(remote_output_file),
            status="success",
            events=events,
            start_time=start_time,
            end_time=end_time,
            log_dir=args.log_dir
        )
        print(f"Job completed successfully in {end_time - start_time:.1f} seconds")
    
    except Exception as e:
        end_time = time.time()
        error_msg = str(e)
        print(f"ERROR: {error_msg}", file=sys.stderr)
        
        try:
            batch_info = find_batch_in_manifest(args.batch_id, manifest)
            log_job(
                batch_id=args.batch_id,
                dataset_name=batch_info['dataset_name'],
                year=args.year,
                input_files=[],
                output_path="",
                status="failed",
                error_msg=error_msg,
                start_time=start_time,
                end_time=end_time,
                log_dir=args.log_dir
            )
        except:
            pass
        
        sys.exit(1)


if __name__ == "__main__":
    main()
