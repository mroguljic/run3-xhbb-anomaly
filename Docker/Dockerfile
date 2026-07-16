FROM gitlab-registry.cern.ch/jhu-tools/timber:run3
RUN python3 -m pip install --no-cache-dir matplotlib mplhep
RUN python3 -c "import matplotlib; print('matplotlib version:', matplotlib.__version__)"

# Base image lacks /usr/bin/which, which (heh) is called by dasgoclient
RUN dnf install -y which && dnf clean all