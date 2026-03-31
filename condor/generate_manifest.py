#!/usr/bin/env python3
"""
Step 2: Dataset enumeration and manifest generation.

Enumerates all datasets from filelists/Nano_v15.py, groups files into batches,
and generates a JSON manifest that serves as input for job submission.

Usage:
    python condor/generate_manifest.py [--year 2024] [--output condor/manifest.json]
"""

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Any

# Add parent directory to path so we can import filelists
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from filelists.dataset_utils import list_files_in_dataset
from filelists.Nano_v15 import mc_bkg, mc_sig, jetmet


def load_config(config_path: str = "condor/config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def flatten_datasets(mc_bkg_dict: Dict, mc_sig_dict: Dict, jetmet_dict: Dict, 
                     year_filter: List[str] = None, pattern_filter: List[str] = None) -> List[Tuple[str, str, str]]:
    """
    Flatten dataset hierarchy into list of (dataset_name, year, das_path) tuples.
    
    Args:
        mc_bkg_dict: Background MC datasets dictionary
        mc_sig_dict: Signal MC datasets dictionary
        jetmet_dict: Data datasets dictionary
        year_filter: If provided, only include these years
        pattern_filter: If provided, only include datasets matching these patterns
    
    Returns:
        List of (dataset_name, year, das_path) tuples
    """
    datasets = []
    
    # Process MC backgrounds
    for year, categories in mc_bkg_dict.items():
        if year_filter and year not in year_filter:
            continue
        for category, dataset_dict in categories.items():
            for dataset_name, das_path in dataset_dict.items():
                if pattern_filter:
                    if any(pattern in dataset_name for pattern in pattern_filter):
                        datasets.append((dataset_name, year, das_path))
                else:
                    datasets.append((dataset_name, year, das_path))
    
    # Process MC signals
    for year, categories in mc_sig_dict.items():
        if year_filter and year not in year_filter:
            continue
        for category, dataset_dict in categories.items():
            for dataset_name, das_path in dataset_dict.items():
                if pattern_filter:
                    if any(pattern in dataset_name for pattern in pattern_filter):
                        datasets.append((dataset_name, year, das_path))
                else:
                    datasets.append((dataset_name, year, das_path))
    
    # Process data (JetMET)
    for year, dataset_groups in jetmet_dict.items():
        if year_filter and year not in year_filter:
            continue
        for group_name, run_dict in dataset_groups.items():
            for run_name, das_paths in run_dict.items():
                # Handle both single path (str) and multiple paths (list)
                if isinstance(das_paths, str):
                    das_paths = [das_paths]
                
                for das_path in das_paths:
                    if pattern_filter:
                        if any(pattern in run_name for pattern in pattern_filter):
                            datasets.append((run_name, year, das_path))
                    else:
                        datasets.append((run_name, year, das_path))
    
    return datasets


def batch_files_by_events(files: List[Tuple[str, int, int]], target_events: int) -> List[List[str]]:
    """
    Group files into batches targeting approximately equal event counts.
    
    Args:
        files: List of (file_path, file_size_bytes, nevents) tuples from DAS
        target_events: Target event count per batch
    
    Returns:
        List of batches, where each batch is a list of file paths
    """
    if not files:
        return []
    
    batches = []
    current_batch = []
    current_events = 0
    
    for file_path, file_size, nevents in files:
        if current_events + nevents > target_events and current_batch:
            # Start new batch
            batches.append(current_batch)
            current_batch = [file_path]
            current_events = nevents
        else:
            # Add to current batch
            current_batch.append(file_path)
            current_events += nevents
    
    # Add remaining batch
    if current_batch:
        batches.append(current_batch)
    
    return batches


def get_file_events(files: List[Tuple[str, int]]) -> List[Tuple[str, int, int]]:
    """
    Extract file paths, sizes, and event counts.
    
    Args:
        files: List of (file_path, file_size_bytes) from list_files_in_dataset
    
    Returns:
        List of (file_path, file_size_bytes, nevents) tuples
    """
    # Estimate events from size: 40KB per event (heuristic)
    result = []
    for path, size in files:
        nevents = int(size / 40000)
        result.append((path, size, nevents))
    return result


def generate_manifest(config_path: str = "condor/config.yaml", 
                      year_filter: List[str] = None,
                      pattern_filter: List[str] = None,
                      output_path: str = "condor/manifest.json") -> Dict[str, Any]:
    """
    Generate job manifest from datasets.
    
    Args:
        config_path: Path to config.yaml
        year_filter: Filter by years (e.g., ["2024"])
        pattern_filter: Filter by dataset name patterns
        output_path: Path to output manifest JSON
    
    Returns:
        Manifest dictionary
    """
    config = load_config(config_path)
    
    # Use config filters if not provided
    if year_filter is None:
        year_filter = config['datasets']['years'] or None
    if pattern_filter is None:
        pattern_filter = config['datasets']['patterns'] or None
    
    batch_target_events = config['batch']['target_events'] # Target number of events per job
    
    # Flatten datasets
    print("Enumerating datasets...")
    all_datasets = flatten_datasets(mc_bkg, mc_sig, jetmet, year_filter, pattern_filter)
    print(f"Found {len(all_datasets)} datasets")
    
    manifest = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "batch_target_events": batch_target_events,
            "total_datasets": 0,
            "total_batches": 0,
            "total_events": 0,
            "total_size_gb": 0.0,
            "year_filter": year_filter,
            "pattern_filter": pattern_filter,
        },
        "datasets": []
    }
    
    total_batches = 0
    total_events = 0
    total_size_bytes = 0
    
    for dataset_name, year, das_path in all_datasets:
        print(f"Processing {dataset_name} ({year})...")
        print(f"  DAS: {das_path}")
        
        # List files in dataset
        files = list_files_in_dataset(das_path)
        if not files:
            print("  [skip] No files found")
            continue
        
        print(f"  Found {len(files)} files, {sum(s for _, s in files) / 1e9:.2f} GB")
        
        # Get event counts
        files_with_events = get_file_events(files)
        total_dataset_events = sum(nevents for _, _, nevents in files_with_events)
        total_dataset_size = sum(size for _, size, _ in files_with_events)
        
        # Batch files
        batches = batch_files_by_events(files_with_events, batch_target_events)
        print(f"  Grouped into {len(batches)} batches")
        
        # Build dataset entry
        dataset_entry = {
            "name": dataset_name,
            "year": year,
            "das_path": das_path,
            "total_files": len(files),
            "total_batches": len(batches),
            "total_events": total_dataset_events,
            "total_size_gb": round(total_dataset_size / 1e9, 2),
            "batches": []
        }
        
        for batch_idx, batch_files in enumerate(batches):
            batch_id = f"{dataset_name}_{batch_idx}"
            batch_events = sum(nevents for path, size, nevents in files_with_events if path in batch_files)
            batch_size = sum(size for path, size, nevents in files_with_events if path in batch_files)
            
            batch_entry = {
                "batch_id": batch_id,
                "batch_index": batch_idx,
                "files": batch_files,
                "file_count": len(batch_files),
                "size_gb": round(batch_size / 1e9, 2),
                "events": batch_events,
            }
            
            dataset_entry["batches"].append(batch_entry)
            total_batches += 1
            total_events += batch_events
        
        manifest["datasets"].append(dataset_entry)
        total_size_bytes += total_dataset_size
    
    manifest["metadata"]["total_datasets"] = len(manifest["datasets"])
    manifest["metadata"]["total_batches"] = total_batches
    manifest["metadata"]["total_events"] = total_events
    manifest["metadata"]["total_size_gb"] = round(total_size_bytes / 1e9, 2)
    
    # Write manifest to file
    with open(output_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\nManifest written to {output_path}")
    print(f"  Datasets: {manifest['metadata']['total_datasets']}")
    print(f"  Batches: {manifest['metadata']['total_batches']}")
    print(f"  Events: {manifest['metadata']['total_events']:,}")
    print(f"  Size: {manifest['metadata']['total_size_gb']:.2f} GB")
    
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Generate condor job manifest from datasets")
    parser.add_argument("--year", type=str, nargs="+", help="Filter by year(s) (e.g., 2024 or 2023 2024)")
    parser.add_argument("--pattern", type=str, nargs="+", help="Filter by dataset name patterns (substring match)")
    parser.add_argument("--output", type=str, default="condor/manifest.json", help="Output manifest path")
    parser.add_argument("--config", type=str, default="condor/config.yaml", help="Config file path")
    
    args = parser.parse_args()
    
    generate_manifest(
        config_path=args.config,
        year_filter=args.year,
        pattern_filter=args.pattern,
        output_path=args.output
    )


if __name__ == "__main__":
    main()
