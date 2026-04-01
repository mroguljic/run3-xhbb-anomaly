#!/bin/bash
# Wrapper script to run preselection_job.py inside singularity container
echo HOME: $HOME
echo X509_USER_PROXY: $X509_USER_PROXY
ls -l $X509_USER_PROXY
voms-proxy-info

echo "Running preselection_job.py with arguments: $@"

# Change to working directory (from environment variable, default to current directory)
WORKING_DIR="${WORKING_DIR:-.}"
cd "$WORKING_DIR" || exit 1
echo "Working directory: $(pwd)"

singularity exec \
    --bind "$(readlink $HOME)" \
    --bind /etc/grid-security/certificates \
    --bind /cvmfs \
    /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/jhu-tools/timber:run3/ \
    python3 condor/preselection_job.py "$@"
