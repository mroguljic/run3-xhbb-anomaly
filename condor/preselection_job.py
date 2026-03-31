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


def download_file(file_path: str, dest_dir: str) -> str:
    """Download file via xrdcp, return local path."""
    
    # Construct full XRD URL if needed
    if not file_path.startswith("root://"):
        file_path = f"root://cms-xrd-global.cern.ch/{file_path}"
    
    dest_file = Path(dest_dir) / Path(file_path).name
    
    cmd = f"xrdcp '{file_path}' '{dest_file}'"
    print(f"Downloading: {file_path}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"xrdcp failed: {result.stderr}")
    
    return str(dest_file)


def merge_files(input_files: List[str], output_file: str) -> None:
    """Merge multiple ROOT files using hadd."""
    
    if len(input_files) == 1:
        # Single file, no merge needed
        import shutil
        shutil.copy(input_files[0], output_file)
        print(f"Copied single file to {output_file}")
        return
    
    cmd = f"hadd -f {output_file} " + " ".join(input_files)
    print(f"Merging {len(input_files)} files...")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"hadd failed: {result.stderr}")
    
    print(f"Merged to {output_file}")


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


def run_preselection(merged_file: str, output_file: str, year: str) -> int:
    """
    Run preselection inside singularity container.
    
    Executes preselection.py through singularity to ensure all dependencies
    (ROOT, TIMBER) are available.
    
    Returns:
        Event count from validation
    """
    
    # Construct singularity command to run preselection.py
    singularity_cmd = (
        "singularity exec "
        "--bind \"$(readlink $HOME)\" "
        "--bind /etc/grid-security/certificates "
        "--bind /cvmfs "
        "/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/jhu-tools/timber:run3/ "
        f"python3 preselection.py -i {merged_file} -o {output_file} -y {year}"
    )
    
    print(f"Running preselection...")
    print(f"Command: {singularity_cmd}")
    
    result = subprocess.run(singularity_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Preselection failed: {result.stderr}")
    
    print(result.stdout)
    
    # Validate output
    events = validate_output(output_file)
    return events


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
    parser.add_argument("--output-dir", default="output/skims", help="Output directory for preselection files")
    parser.add_argument("--log-dir", default="condor/logs", help="Log directory for metadata JSON")
    parser.add_argument("--temp-dir", default="/tmp", help="Temporary directory for file staging")
    
    args = parser.parse_args()
    
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
        
        # Create temp work directory with unique job ID to avoid conflicts on shared filesystem
        # Use Condor cluster and proc IDs if available, otherwise use timestamp
        cluster_id = os.environ.get('CONDOR_CLUSTER', 'local')
        proc_id = os.environ.get('CONDOR_PROC', '0')
        job_id = f"{cluster_id}_{proc_id}"
        
        work_dir = Path(args.temp_dir) / f"batch_{args.batch_id}_{job_id}"
        work_dir.mkdir(parents=True, exist_ok=True)
        print(f"Work directory: {work_dir}")
        
        try:
            # Download files
            print(f"Downloading {len(input_files)} files...")
            downloaded = []
            for file_path in input_files:
                local_path = download_file(file_path, str(work_dir))
                downloaded.append(local_path)
            
            # Merge if multiple files
            merged_file = work_dir / f"merged_{args.batch_id}.root"
            merge_files(downloaded, str(merged_file))
            
            # Run preselection
            Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            output_file = Path(args.output_dir) / f"preselection_{args.batch_id}.root"
            
            # Skip if output already exists (safe resume)
            if output_file.exists():
                print(f"Output already exists: {output_file}")
                print("Skipping preselection (job already completed)")
                # Validate existing output and get event count
                events = validate_output(str(output_file))
                # Log as success
                end_time = time.time()
                log_job(
                    batch_id=args.batch_id,
                    dataset_name=dataset_name,
                    year=args.year,
                    input_files=input_files,
                    output_path=str(output_file),
                    status="skipped_already_exists",
                    events=events,
                    start_time=start_time,
                    end_time=end_time,
                    log_dir=args.log_dir
                )
                print(f"Job skipped (already exists) in {end_time - start_time:.1f} seconds")
                return
            
            events = run_preselection(str(merged_file), str(output_file), args.year)
            
            # Success
            end_time = time.time()
            log_job(
                batch_id=args.batch_id,
                dataset_name=dataset_name,
                year=args.year,
                input_files=input_files,
                output_path=str(output_file),
                status="success",
                events=events,
                start_time=start_time,
                end_time=end_time,
                log_dir=args.log_dir
            )
            print(f"Job completed successfully in {end_time - start_time:.1f} seconds")
            
        finally:
            # Cleanup temp directory and all files
            import shutil
            if work_dir.exists():
                try:
                    shutil.rmtree(work_dir)
                    print(f"Cleaned up {work_dir}")
                except Exception as cleanup_err:
                    print(f"WARNING: Failed to cleanup {work_dir}: {cleanup_err}", file=sys.stderr)
    
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
