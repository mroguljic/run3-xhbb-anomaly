#!/usr/bin/env python3
"""
Step 3: Generate Condor submission files from manifest.

Reads manifest.json and generates preselection.sub with one job per batch.

Usage:
    python condor/generate_submission_files.py --manifest condor/manifest.json --output condor/preselection.sub
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List
import os
from config import USER_ENV, OUTPUT, JOB_EXECUTION


def generate_preselection_sub(manifest_file: str, output_file: str, executable: str = "condor/preselection_wrapper.sh") -> None:
    """Generate preselection.sub Condor submission file from manifest."""

    # Load user environment variables from config
    home = USER_ENV.get('home', '/users/mrogul')
    x509_proxy_filename = USER_ENV.get('x509_proxy', 'x509up_u57413')
    x509_proxy = f"{home}/{x509_proxy_filename}"
    manifest = json.load(open(manifest_file))

    # Get paths and working directory from config
    logs_dir = OUTPUT.get('logs_dir', 'condor/logs')
    skims_dir = OUTPUT.get('skims_dir', 'output/skims')
    working_dir = JOB_EXECUTION.get('working_dir', '/users/mrogul/Work/anomaly-tagging/run3-xhbb-anomaly/')

    os.makedirs(logs_dir, exist_ok=True)
    os.makedirs(skims_dir, exist_ok=True)
    
    lines = [
        "# Preselection job submission file",
        "# Generated from manifest",
        "",
        f"executable = {executable}",
        "request_cpus = 1",
        "transfer_executable = False",
        f"transfer_input_files = timber_run3.sif,{manifest_file}",
        "should_transfer_files = YES",
        "",
        f"output = {logs_dir}/preselection_$(BATCH_ID).out",
        f"error = {logs_dir}/preselection_$(BATCH_ID).err",
        f"log = {logs_dir}/preselection_$(BATCH_ID).log",
        "",
        f"environment = \"HOME={home} X509_USER_PROXY={x509_proxy} WORKING_DIR={working_dir}\"",
        f"arguments = --batch-id $(BATCH_ID) --manifest $(MANIFEST_FILE) --year $(YEAR) --output-dir {skims_dir} --log-dir {logs_dir}",
        "",
        "queue BATCH_ID, YEAR, MANIFEST_FILE from (",
    ]
    
    # Extract batch info from manifest and filter to only queue batches with missing output
    batch_count = 0
    skipped_count = 0
    for dataset in manifest['datasets']:
        year = dataset['year']
        for batch in dataset['batches']:
            batch_id = batch['batch_id']
            output_file_path = Path(skims_dir) / f"preselection_{batch_id}.root"
            
            if output_file_path.exists():
                print(f"Skipping {batch_id} (output already exists)")
                skipped_count += 1
            else:
                lines.append(f"  {batch_id}, {year}, {manifest_file}")
                batch_count += 1
    
    lines.append(")")
    lines.append("")
    
    # Write to file
    with open(output_file, 'w') as f:
        f.write('\n'.join(lines))
    
    print(f"Generated {output_file} with {batch_count} jobs (skipped {skipped_count} with existing output)")
    return batch_count


def main():
    parser = argparse.ArgumentParser(description="Generate Condor submission files from manifest")
    parser.add_argument("--manifest", default="condor/manifest.json", help="Path to manifest JSON")
    parser.add_argument("--output", default="condor/preselection.sub", help="Output .sub file path")
    parser.add_argument("--executable", default="condor/preselection_wrapper.sh", help="Path to executable")
    
    args = parser.parse_args()
    
    # Load manifest
    with open(args.manifest) as f:
        manifest = json.load(f)
    
    # Generate submission file
    batch_count = generate_preselection_sub(args.manifest, args.output, args.executable)
    print(f"\nSummary:")
    print(f"  Jobs queued: {batch_count}")
    print(f"  Total events: {manifest['metadata']['total_events']}")
    print(f"  Total datasets: {manifest['metadata']['total_datasets']}")
    
    # Print sample
    print("\nFirst 20 lines of generated file:")
    with open(args.output) as f:
        for i, line in enumerate(f):
            if i < 20:
                print(line.rstrip())
            else:
                break


if __name__ == "__main__":
    main()
