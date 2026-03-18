timber_exec() {
    singularity exec \
        --bind "$(readlink $HOME)" \
        --bind /etc/grid-security/certificates \
        --bind /cvmfs \
        /cvmfs/unpacked.cern.ch/registry.hub.docker.com/mrogulji/timber:run3/ \
        python3 "$@"
}