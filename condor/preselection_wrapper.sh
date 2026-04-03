#!/bin/bash
# Wrapper script to run preselection_job.py inside singularity container
set -x
echo "=== Environment Variables ==="
env | sort
echo "=== End Environment ==="

WORKING_DIR="${_CONDOR_SCRATCH_DIR}"
cd $WORKING_DIR
echo "Working directory: $(pwd)"
ls -l
echo "X509_USER_PROXY: $X509_USER_PROXY"
ls -l $X509_USER_PROXY
voms-proxy-info


git clone https://github.com/mroguljic/run3-xhbb-anomaly.git
cd run3-xhbb-anomaly


exit 1; # Exit early for testing, remove this line when ready to run the actual job
echo "Running preselection_job.py with arguments: $@"

singularity exec \
    --bind "$(readlink $HOME)" \
    --bind /etc/grid-security/certificates \
    --bind /cvmfs \
    --bind /STORE \
    --bind "${_CONDOR_SCRATCH_DIR}" \
    /users/mrogul/Work/anomaly-tagging/run3-xhbb-anomaly/timber_run3.sif \
    python3 condor/preselection_job.py "$@"

    #/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/jhu-tools/timber:run3/ \
    # The singularity image in cvmfs has some issues?! Using local image for now, but should be fixed in the future
