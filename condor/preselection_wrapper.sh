#!/bin/bash
# Wrapper script to run preselection_job.py inside singularity container
set -ex
echo "=== Environment Variables ==="
env | sort
echo "=== End Environment ==="

WORKING_DIR="${_CONDOR_SCRATCH_DIR}"
cd "$WORKING_DIR"
echo "Working directory: $(pwd)"
ls -l
echo "X509_USER_PROXY: $X509_USER_PROXY"
voms-proxy-info

# Clone the repository
git clone https://github.com/mroguljic/run3-xhbb-anomaly.git
cd run3-xhbb-anomaly

# Copy transferred manifest to condor directory (transferred files arrive in $WORKING_DIR root)
# The .sub file specifies condor/manifest_qcd.json in transfer_input_files,
# but it arrives in WORKDIR as manifest_qcd.json
echo "Files in working directory"
ls "$WORKING_DIR"
cp $WORKING_DIR/manifest*.json ./condor/

# Run preselection job inside singularity container
ls -l "$WORKING_DIR/timber_run3.sif"
singularity exec \
    --bind "$(readlink $HOME)" \
    --bind /etc/grid-security/certificates \
    --bind /cvmfs \
    --bind /STORE \
    --bind "${_CONDOR_SCRATCH_DIR}" \
    $WORKING_DIR/timber_run3.sif \
    python3 condor/preselection_job.py "$@"
