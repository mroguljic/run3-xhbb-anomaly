#!/usr/bin/env python3
"""
Phase 3: Batch manifest generation.

Reads dataset filelists, groups files into batches by event count,
and generates a manifest JSON file that jobs will use to look up
their input files, output paths, and other metadata.

Manifest structure:
{
  "campaign": "20260401",
  "base_store_path": "/store/user/roguljic/run3-xhbb-anomaly",
  "batches": {
    "TTto4Q_batch_0": {
      "dataset": "TTto4Q",
      "year": "2024",
      "files": ["root://...", "root://..."],
      "output_path": "root://cmseos.fnal.gov/store/user/.../skims/preselection_TTto4Q_batch_0.root"
    },
    ...
  }
}

Usage:
    python3 generate_batches.py \
        --datasets TTto4Q QCD-4Jets_HT-800to1000 \
        --year 2024 \
        --output manifest_20260401.json
"""

import json
import sys
import argparse
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.config import BATCH_TARGET_EVENTS, CAMPAIGN, BASE_STORE_PATH, OUTPUT, get_store_xrd_path
from filelists.Nano_v15 import mc_bkg, mc_sig, jetmet


# ============================================================================
# DAS Querying
# ============================================================================

def query_das_files(dataset_path: str) -> List[Tuple[str, int]]:
    """
    Query DAS for files in a dataset and get file paths and event counts.
    
    Args:
        dataset_path (str): Full DAS dataset path
    
    Returns:
        List of tuples (file_path, n_events)
    
    Raises:
        RuntimeError: If DAS query fails
    """
    try:
        command = f'dasgoclient -query="file dataset={dataset_path}" -json'
        result = subprocess.check_output(command, shell=True, text=True)
        
        files = json.loads(result)
        file_list = []
        
        for entry in files:
            for file_info in entry.get('file', []):
                file_path = file_info['name']
                n_events = file_info.get('nevents', 0)
                file_list.append((file_path, n_events))
        
        # Sort for reproducibility
        file_list.sort(key=lambda x: x[0])
        return file_list
    
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"DAS query failed for {dataset_path}: {e}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Error parsing DAS output: {e}")


# ============================================================================
# Batch Generation
# ============================================================================

def group_files_into_batches(
    files: List[Tuple[str, int]],
    target_events: int,
    dataset_name: str
) -> List[Dict[str, Any]]:
    """
    Group files into batches based on target event count.
    
    Args:
        files: List of (file_path, n_events) tuples
        target_events: Target number of events per batch
        dataset_name: Name of dataset for batch ID
    
    Returns:
        List of batch dictionaries with file lists and event counts
    """
    batches = []
    current_batch = []
    current_events = 0
    batch_count = 0
    
    for file_path, n_events in files:
        current_batch.append(file_path)
        current_events += n_events
        
        # Start new batch if we've reached target (unless it's the first file)
        if current_events >= target_events and len(current_batch) > 0:
            batch_id = f"{dataset_name}_batch_{batch_count}"
            batches.append({
                "batch_id": batch_id,
                "files": current_batch,
                "n_events": current_events
            })
            batch_count += 1
            current_batch = []
            current_events = 0
    
    # Add remaining files as final batch
    if current_batch:
        batch_id = f"{dataset_name}_batch_{batch_count}"
        batches.append({
            "batch_id": batch_id,
            "files": current_batch,
            "n_events": current_events
        })
    
    return batches


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate batch manifest for HTCondor submission",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate manifest for specific datasets
  python3 generate_batches.py \\
      --datasets TTto4Q QCD-4Jets_HT-800to1000 \\
      --year 2024 \\
      --output manifest_20260401.json

  # Generate manifest for all 2024 datasets
  python3 generate_batches.py \\
      --year 2024 \\
      --all-datasets \\
      --output manifest_20260401.json
        """
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        help="Dataset names to process (e.g., TTto4Q QCD-4Jets_HT-800to1000)"
    )
    parser.add_argument(
        "--all-datasets",
        action="store_true",
        help="Process all available datasets for the year"
    )
    parser.add_argument(
        "--year",
        required=True,
        choices=["2022", "2023", "2024"],
        help="Data year"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output manifest JSON file"
    )
    parser.add_argument(
        "--target-events",
        type=int,
        default=BATCH_TARGET_EVENTS,
        help=f"Target events per batch (default: {BATCH_TARGET_EVENTS})"
    )
    
    args = parser.parse_args()
    
    year = args.year
    target_events = args.target_events
    output_file = args.output
    
    print("=" * 80)
    print("Batch Manifest Generation")
    print("=" * 80)
    print(f"Campaign:       {CAMPAIGN}")
    print(f"Year:           {year}")
    print(f"Target events:  {target_events:,}")
    print(f"Output:         {output_file}")
    print("=" * 80 + "\n")
    
    # Determine which datasets to process
    datasets_to_process = {}
    
    if args.all_datasets:
        print("Processing all available datasets for year...\n")
        # Collect all datasets for the year
        if year in mc_bkg:
            for category in mc_bkg[year]:
                for dataset_name in mc_bkg[year][category]:
                    datasets_to_process[dataset_name] = mc_bkg[year][category][dataset_name]
        if year in mc_sig:
            for category in mc_sig[year]:
                for dataset_name in mc_sig[year][category]:
                    datasets_to_process[dataset_name] = mc_sig[year][category][dataset_name]
        if year in jetmet:
            for category in jetmet[year]:
                for dataset_name in jetmet[year][category]:
                    if isinstance(jetmet[year][category][dataset_name], list):
                        # Data has multiple entries per dataset
                        for entry in jetmet[year][category][dataset_name]:
                            datasets_to_process[f"{dataset_name}_{len(datasets_to_process)}"] = entry
                    else:
                        datasets_to_process[dataset_name] = jetmet[year][category][dataset_name]
    else:
        if not args.datasets:
            parser.error("Either --datasets or --all-datasets must be specified")
        
        print(f"Processing specified datasets: {', '.join(args.datasets)}\n")
        
        # Look up datasets
        for dataset_name in args.datasets:
            found = False
            
            # Check MC backgrounds
            if year in mc_bkg:
                for category in mc_bkg[year]:
                    if dataset_name in mc_bkg[year][category]:
                        datasets_to_process[dataset_name] = mc_bkg[year][category][dataset_name]
                        found = True
                        break
            
            # Check MC signal
            if not found and year in mc_sig:
                for category in mc_sig[year]:
                    if dataset_name in mc_sig[year][category]:
                        datasets_to_process[dataset_name] = mc_sig[year][category][dataset_name]
                        found = True
                        break
            
            # Check data
            if not found and year in jetmet:
                for category in jetmet[year]:
                    if dataset_name in jetmet[year][category]:
                        datasets_to_process[dataset_name] = jetmet[year][category][dataset_name]
                        found = True
                        break
            
            if not found:
                print(f"Warning: Dataset '{dataset_name}' not found in Nano_v15.py")
    
    if not datasets_to_process:
        print("ERROR: No datasets to process")
        return 1
    
    # Generate batches for each dataset
    manifest = {
        "campaign": CAMPAIGN,
        "base_store_path": BASE_STORE_PATH,
        "generated_at": datetime.now().isoformat(),
        "year": year,
        "target_events": target_events,
        "datasets": {}
    }
    
    total_batches = 0
    total_events = 0
    
    for dataset_name, das_path in sorted(datasets_to_process.items()):
        print(f"Processing: {dataset_name}")
        print(f"  DAS path: {das_path}")
        
        try:
            # Query DAS
            print(f"  Querying DAS...")
            files = query_das_files(das_path)
            print(f"  Found {len(files)} files")
            
            if not files:
                print(f"  Skipping (no files found)\n")
                continue
            
            # Calculate total events
            n_events_total = sum(n_events for _, n_events in files)
            print(f"  Total events: {n_events_total:,}")
            
            # Group into batches
            batches = group_files_into_batches(files, target_events, dataset_name)
            print(f"  Created {len(batches)} batches\n")
            
            # Add to manifest under dataset
            manifest["datasets"][dataset_name] = {
                "das_path": das_path,
                "n_batches": len(batches),
                "n_events_total": n_events_total,
                "batches": {}
            }
            
            for batch in batches:
                batch_id = batch["batch_id"]
                output_path = f"{OUTPUT['skims_dir']}/preselection_{batch_id}.root"
                xrd_output = get_store_xrd_path(output_path)
                
                manifest["datasets"][dataset_name]["batches"][batch_id] = {
                    "files": batch["files"],
                    "n_files": len(batch["files"]),
                    "n_events": batch["n_events"],
                    "output_path": xrd_output
                }
                
                total_batches += 1
                total_events += batch["n_events"]
        
        except RuntimeError as e:
            print(f"  ERROR: {e}\n")
            continue
    
    # Write manifest
    print("=" * 80)
    print(f"Manifest Summary")
    print("=" * 80)
    print(f"Total batches:  {total_batches}")
    print(f"Total events:   {total_events:,}")
    print(f"Output file:    {output_file}\n")
    
    with open(output_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"Manifest written to {output_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
