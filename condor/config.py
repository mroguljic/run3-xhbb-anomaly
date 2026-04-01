"""
Condor Submission Framework Configuration
"""

BATCH_TARGET_EVENTS = 1000000

OUTPUT = {
    "skims_dir": "output/skims",
    "templates_dir": "output/templates"
}
JOB_EXECUTION = {
    "working_dir": "/users/mrogul/Work/anomaly-tagging/run3-xhbb-anomaly/",  # Jobs will cd here before running, hardcoded in .sh file for now, to be implemented
    "temp_base_dir": "/users/mrogul/Work/anomaly-tagging/run3-xhbb-anomaly/condor/temp",  # Base directory for temporary files per job; each job creates {temp_base_dir}/preselection_{batch_id}/ used to stage downloaded ROOT files; deleted after job completes
}

# Dataset filtering (for manifest generation)
# Leave empty to process all datasets
DATASET_FILTER = {
    "years": [],  # Process only specific years (e.g., [2024] or [2023, 2024]); if empty, all years are processed
    "patterns": [],  # Process only datasets matching these patterns (substring match); empty = all datasets
}

# User environment variables for Condor job submission
USER_ENV = {
    "home": "/users/mrogul",
    "x509_proxy": "x509up_u57413",
}
