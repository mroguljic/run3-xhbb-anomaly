FROM gitlab-registry.cern.ch/jhu-tools/timber:run3
RUN python3 -m pip install --no-cache-dir matplotlib mplhep
RUN python3 -c "import matplotlib; print('matplotlib version:', matplotlib.__version__)"

# Base image lacks /usr/bin/which, which CMSSW's `scram` (invoked internally
# by the cvmfs dasgoclient wrapper) shells out to; without it dasgoclient
# still works but prints a spurious "No such file or directory" warning.
RUN printf '#!/bin/sh\ncommand -v "$1"\n' > /usr/bin/which && chmod +x /usr/bin/which