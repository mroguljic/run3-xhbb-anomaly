FROM gitlab-registry.cern.ch/jhu-tools/timber:run3
RUN python3 -m pip install --no-cache-dir matplotlib mplhep
RUN python3 -c "import matplotlib; print('matplotlib version:', matplotlib.__version__)"

# Lund plane reweighting (lund/): fastjet provides the exclusive-kT reclustering,
# scipy is pulled in by the upstream LundReweighting plotting utilities.
RUN python3 -m pip install --no-cache-dir fastjet scipy
RUN python3 -c "import fastjet, scipy; print('fastjet version:', fastjet.__version__)"

# Base image lacks /usr/bin/which, which (heh) is called by dasgoclient
RUN dnf install -y which && dnf clean all