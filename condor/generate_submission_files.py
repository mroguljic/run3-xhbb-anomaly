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
import yaml


def load_user_env(config_path: str = "condor/config.yaml") -> Dict[str, str]:
    """Load user-specific environment variables from config.yaml."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            user_env = config.get('user_env', {})
            return {
                'HOME': user_env.get('home', '/users/mrogul'),
                'X509_USER_PROXY': user_env.get('x509_proxy', 'x509up_u57413')
            }
    except FileNotFoundError:
        print(f"Warning: {config_path} not found. Using default values.")
        return {'HOME': '/users/mrogul', 'X509_USER_PROXY': 'x509up_u57413'}


def generate_preselection_sub(manifest: Dict, output_file: str, executable: str = "condor/preselection_wrapper.sh") -> None:
    """Generate preselection.sub Condor submission file from manifest."""

    # Load user environment variables
    user_env = load_user_env()
    home = user_env['HOME']
    x509_proxy = f"{home}/{user_env['X509_USER_PROXY']}"

    lines = [
        "# Preselection job submission file",
        "# Generated from manifest",
        "",
        f"executable = {executable}",
        "request_cpus = 1",
        "transfer_executable = False",
        "",
        "output = condor/logs/preselection_$(BATCH_ID).out",
        "error = condor/logs/preselection_$(BATCH_ID).err",
        "log = condor/logs/preselection_$(BATCH_ID).log",
        "",
        f"environment = \"HOME={home} X509_USER_PROXY={x509_proxy}\"",
        "arguments = --batch-id $(BATCH_ID) --manifest condor/manifest.json --year $(YEAR) --output-dir output/skims --log-dir condor/logs",
        "",
        "queue BATCH_ID, YEAR from (",
    ]
    
    # Extract batch info from manifest
    batch_count = 0
    for dataset in manifest['datasets']:
        year = dataset['year']
        for batch in dataset['batches']:
            batch_id = batch['batch_id']
            
            lines.append(f"  {batch_id}, {year}")
            batch_count += 1
    
    lines.append(")")
    lines.append("")
    
    # Write to file
    with open(output_file, 'w') as f:
        f.write('\n'.join(lines))
    
    print(f"Generated {output_file} with {batch_count} jobs")
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
    batch_count = generate_preselection_sub(manifest, args.output, args.executable)
    print(f"Total batches: {batch_count}")
    print(f"Total events: {manifest['metadata']['total_events']}")
    print(f"Total datasets: {manifest['metadata']['total_datasets']}")
    
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
