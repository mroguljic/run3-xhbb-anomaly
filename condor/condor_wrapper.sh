#!/bin/bash
# Minimal HTCondor wrapper for preselection job
# Runs inside Singularity container (managed by HTCondor via +SingularityImage)
#
# Usage:
#   condor_wrapper.sh --batch-id <ID> --files <FILE1> [FILE2 ...] --year <YEAR> --output <PATH>
#
#
# This script:
#   1. Clones the repository
#   2. Runs preselection_job_batch.py with provided arguments
#   3. Returns status

set -ex

# ============================================================================
# Setup
# ============================================================================

echo "========== HTCondor Preselection Wrapper =========="
echo "Start time: $(date)"
echo "Host: $(hostname)"
echo ""

SCRATCH_DIR=$(pwd) # Usually /srv
REPO_DIR="${SCRATCH_DIR}/run3-xhbb-anomaly"

echo "Scratch directory: ${SCRATCH_DIR}"
echo "Repository will be cloned to: ${REPO_DIR}"
echo ""

# Write proxy info (for debugging, to be removed later)
voms-proxy-info

# ============================================================================
# Clone repository
# ============================================================================

echo "Cloning repository..."
git clone --quiet https://github.com/mroguljic/run3-xhbb-anomaly.git
if [[ ! -d "${REPO_DIR}" ]]; then
    echo "ERROR: Failed to clone repository"
    exit 1
fi
cd "${REPO_DIR}"

# Record commit hash for reproducibility
COMMIT_HASH=$(git rev-parse HEAD)
echo "Repository cloned successfully"
echo "Commit hash: ${COMMIT_HASH}"
echo ""

# Copy manifest JSON from job's working directory to repo
MANIFEST_FILE="${@: -1}"
if [[ -f "${SCRATCH_DIR}/${MANIFEST_FILE}" ]]; then
    cp "${SCRATCH_DIR}/${MANIFEST_FILE}" "${REPO_DIR}/${MANIFEST_FILE}"
    echo "Copied manifest: ${MANIFEST_FILE}"
fi
echo ""

# ============================================================================
# Run preselection job
# ============================================================================

echo "Running preselection job with arguments:"
echo "  $@"
echo ""

# Pass all arguments to the batch job script
python3 condor/preselection_job_batch.py "$@"

JOB_STATUS=$?

# ============================================================================
# Cleanup and exit
# ============================================================================

echo ""
echo "Job completed with status: ${JOB_STATUS}"
echo "End time: $(date)"
echo "========== Wrapper Complete =========="

exit ${JOB_STATUS}
