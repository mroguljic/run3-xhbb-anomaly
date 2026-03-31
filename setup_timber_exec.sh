timber_exec() {
    singularity exec \
        --bind "$(readlink $HOME)" \
        --bind /etc/grid-security/certificates \
        --bind /cvmfs \
        /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/jhu-tools/timber:run3/ \
        python3 "$@"
}