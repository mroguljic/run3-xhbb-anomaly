#!/bin/bash
# HTCondor wrapper for template job
# Runs inside Singularity container (managed by HTCondor via +SingularityImage)
#
# This script:
#   1. Clones the repository
#   2. Copies the manifest from the job working directory
#   3. Runs template_job_batch.py with provided arguments

set -ex

echo "========== HTCondor Template Wrapper =========="
echo "Start time: $(date)"
echo "Host: $(hostname)"
echo ""

SCRATCH_DIR=$(pwd)
REPO_DIR="${SCRATCH_DIR}/run3-xhbb-anomaly"

echo "Scratch directory: ${SCRATCH_DIR}"
echo "Repository will be cloned to: ${REPO_DIR}"
echo ""

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

COMMIT_HASH=$(git rev-parse HEAD)
echo "Repository cloned at commit: ${COMMIT_HASH}"
echo ""

# Copy manifest from job working directory into repo
MANIFEST_FILE="${@: -1}"
if [[ -f "${SCRATCH_DIR}/${MANIFEST_FILE}" ]]; then
    cp "${SCRATCH_DIR}/${MANIFEST_FILE}" "${REPO_DIR}/${MANIFEST_FILE}"
    echo "Copied manifest: ${MANIFEST_FILE}"
fi
echo ""

# ============================================================================
# Run template job
# ============================================================================

echo "Running template job with arguments:"
echo "  $@"
echo ""

python3 condor/template_job_batch.py "$@"

JOB_STATUS=$?

echo ""
echo "Job completed with status: ${JOB_STATUS}"
echo "End time: $(date)"
echo "========== Template Wrapper Complete =========="

exit ${JOB_STATUS}
