#!/usr/bin/env python3
"""
Phase 4: HTCondor submission file generation.

Reads batch manifest and generates a .sub file ready for HTCondor submission.

The generated .sub file uses HTCondor's queue directive to spawn one job per batch,
with each job receiving its batch_id and manifest path as arguments.

Usage:
    python3 generate_submission.py \
        --manifest manifest_20260401.json \
        --output submission_20260401.sub
"""

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from condor.config import CAMPAIGN, EOS_LS, EOS_MKDIR


# ============================================================================
# Templates
# ============================================================================

SUB_TEMPLATE = """\
# HTCondor Submission File - Generated {generated_at}
# Campaign: {campaign}
# Manifest: {manifest_file}

universe = vanilla
executable = {wrapper_script}
transfer_executable = True
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
transfer_input_files = {manifest_file}

+SingularityImage = "/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/jhu-tools/timber:run3/"

output   = logs/{log_subdir}/$(BATCH_ID).out
error    = logs/{log_subdir}/$(BATCH_ID).err
log      = logs/{log_subdir}/$(BATCH_ID).log

# Arguments passed to condor_wrapper.sh, which passes them to preselection_job_batch.py
arguments = --batch-id $(BATCH_ID) --manifest {manifest_file}

# Queue all batches from manifest
# Each line: BATCH_ID
queue BATCH_ID from (
{queue_entries}
)
"""


# ============================================================================
# Submission Generation
# ============================================================================

def eos_directory_exists(store_dir: str) -> bool:
    """Check whether an EOS directory already exists."""
    import subprocess

    cmd = f"{EOS_LS} {store_dir}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode == 0


def create_output_directory(output_xrd_path: str) -> str:
    """Ensure output directory exists on EOS.

    Returns:
        str: One of "created", "exists", or "failed".
    """
    import subprocess
    from pathlib import Path
    
    store_path = output_xrd_path.replace('root://cmseos.fnal.gov', '')
    store_dir = str(Path(store_path).parent)

    if eos_directory_exists(store_dir):
        return "exists"
    
    cmd = f"{EOS_MKDIR} -p {store_dir}"
    result = subprocess.run(cmd, shell=True, capture_output=True)
    if result.returncode == 0:
        return "created"
    return "failed"


def check_output_exists(output_xrd_path: str) -> bool:
    """
    Check if output file exists on XRD storage using eosls.
    
    Args:
        output_xrd_path (str): XRD URL path (root://cmseos.fnal.gov/store/...)
    
    Returns:
        bool: True if file exists, False otherwise
    """
    import subprocess
    
    try:
        # Extract /store path from XRD URL
        store_path = output_xrd_path.replace('root://cmseos.fnal.gov', '')
        
        # Use eosls to check if file exists
        cmd = f"{EOS_LS} {store_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0
    except Exception:
        # If we can't check, assume it doesn't exist (conservative)
        return False


def detect_job_stage(manifest_dict: dict) -> str:
    """Detect submission stage from batch naming convention.

    Returns:
        "templates" if any batch ID contains "_tpl_chunk_", else "preselection".
    """
    for dataset_info in manifest_dict["datasets"].values():
        for batch_id in dataset_info["batches"].keys():
            if "_tpl_chunk_" in batch_id:
                return "templates"
    return "preselection"


def generate_submission(manifest_dict: dict, manifest_file: str, test: bool, log_subdir: str, wrapper_script: str) -> tuple:
    """
    Generate HTCondor submission file content from manifest.
    
    Filters out batches whose output already exists.
    
    Args:
        manifest_dict: Parsed manifest JSON
        manifest_file: Path to manifest file (for reference in .sub)
        test: If True, only include one batch in the submission file for testing.
    
    Returns:
        Tuple of (sub_file_content, list_of_queued_batch_ids, list_of_skipped_batch_ids)
    """
    # Extract all batch IDs and check if outputs exist
    queued_batches = []
    skipped_batches = []
    
    for dataset_name, dataset_info in manifest_dict["datasets"].items():
        n_batches_dataset = 0
        for batch_id in sorted(dataset_info["batches"].keys()):
            if test and n_batches_dataset >= 1:
                break
            batch = dataset_info["batches"][batch_id]
            output_path = batch["output_path"]
            
            # Check if output already exists
            if check_output_exists(output_path):
                skipped_batches.append((batch_id, output_path))
            else:
                n_batches_dataset += 1
                queued_batches.append(batch_id)
    
    # Create queue entries (one per batch without output)
    queue_entries = "\n".join(f"  {batch_id}" for batch_id in queued_batches)
    
    # Generate .sub content
    sub_content = SUB_TEMPLATE.format(
        generated_at=datetime.now().isoformat(),
        campaign=manifest_dict["campaign"],
        manifest_file=manifest_file,
        log_subdir=log_subdir,
        wrapper_script=wrapper_script,
        queue_entries=queue_entries
    )
    
    return sub_content, queued_batches, skipped_batches


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate HTCondor submission file from batch manifest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 generate_submission.py \\
      --manifest manifest_20260401.json \\
      --output submission_20260401.sub

  # After generation, submit with:
  #   condor_submit submission_20260401.sub
        """
    )
    parser.add_argument("--manifest", required=True, help="Path to batch manifest JSON file")
    parser.add_argument("--output", required=True, help="Output .sub file path")
    parser.add_argument("--test", required=False, help="Only generate one batch in submission file for testing", action="store_true")
    
    args = parser.parse_args()
    
    manifest_path = args.manifest
    output_path = args.output
    
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
    
    # Count total batches
    total_batches = 0
    for dataset_info in manifest["datasets"].values():
        total_batches += len(dataset_info["batches"])
    
    print("=" * 80)
    print("HTCondor Submission File Generation")
    print("=" * 80)
    print(f"Manifest:       {manifest_path}")
    print(f"Campaign:       {manifest['campaign']}")
    print(f"Year:           {manifest['year']}")
    print(f"Total datasets: {len(manifest['datasets'])}")
    print(f"Output:         {output_path}")
    print("=" * 80 + "\n")
    
    # Determine stage and create logs directory if it doesn't exist
    log_subdir = detect_job_stage(manifest)
    logs_dir = Path("logs") / log_subdir
    if not logs_dir.exists():
        logs_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created logs directory: {logs_dir}/\n")
    
    # Create output directory on EOS
    print("Creating output directories on EOS...")
    output_dirs_created = set()
    
    for dataset_name, dataset_info in manifest["datasets"].items():
        for batch_id, batch in dataset_info["batches"].items():
            batch_output_path = batch["output_path"]
            output_dir = str(Path(batch_output_path).parent)
            
            # Avoid redundant mkdir calls for same directory
            if output_dir not in output_dirs_created:
                directory_status = create_output_directory(batch_output_path)
                output_dirs_created.add(output_dir)

                if directory_status == "created":
                    print(f"  Created: {output_dir}")
                elif directory_status == "exists":
                    print(f"  Exists:  {output_dir}")
                else:
                    print(f"  Warning: Failed to create {output_dir}")
    
    print()
    wrapper_script = "template_wrapper.sh" if log_subdir == "templates" else "condor_wrapper.sh"

    sub_content, queued_batches, skipped_batches = generate_submission(
        manifest,
        manifest_path,
        args.test,
        log_subdir,
        wrapper_script,
    )
    
    # Count total batches in manifest
    total_batches = 0
    for dataset_info in manifest["datasets"].values():
        total_batches += len(dataset_info["batches"])
    
    print(f"Batch Summary:")
    print(f"  Total batches:   {total_batches}")
    print(f"  Queued batches:  {len(queued_batches)}")
    print(f"  Skipped batches: {len(skipped_batches)}")
    
    if skipped_batches:
        print(f"\nSkipped batches (output already exists):")
        for batch_id, _ in skipped_batches:
            print(f"  - {batch_id}")
    
    print()
    
    # Write to file
    try:
        with open(output_path, 'w') as f:
            f.write(sub_content)
    except IOError as e:
        print(f"ERROR: Failed to write submission file: {e}")
        return 1
    
    print(f"Submission file generated: {output_path}\n")
    
    # Print preview
    print("Preview of generated .sub file:")
    print("-" * 80)
    lines = sub_content.split('\n')
    # Show first 30 lines and last 20 lines
    if len(lines) > 50:
        print('\n'.join(lines[:30]))
        print(f"\n... ({len(lines) - 50} more lines) ...\n")
        print('\n'.join(lines[-20:]))
    else:
        print(sub_content)
    print("-" * 80)
    
    print(f"\nReady to submit with: condor_submit {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
