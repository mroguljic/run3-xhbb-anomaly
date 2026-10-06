"""
Condor Submission Framework Configuration
"""

from pathlib import Path
import os

REPO_ROOT = Path(__file__).resolve().parent.parent

BATCH_TARGET_EVENTS = 2000000 # Number of input events per skimming batch
TEMPLATE_BATCH_SIZE = 3. # GB of input skims per template batch (for template generation)
CAMPAIGN = "20261006"

# Jobs do not run the local checkout: the wrappers git clone GIT_REPO_URL at GIT_REF
# (branch or tag) inside the job. Set GIT_REF on a side branch to that branch's name,
# and push it before submitting - generate_submission.py warns if the remote ref is
# missing or differs from the local one.
GIT_REPO_URL = "https://gitlab.cern.ch/mrogulji/run3-xhbb-anomaly"
GIT_REF = "master"
BASE_STORE_PATH = f"/store/group/lpchbbrun3/{os.environ.get('USER')}/run3-xhbb-anomaly"

# Output paths are namespaced by year. This matters because the MC dataset keys are
# identical across years (2024 and 2025 share the same Summer24 samples, split 1:1 by
# event number in preselection.py), so a year-less path would have the two years'
# skims/templates overwrite each other.


def get_skims_dir(year: str) -> str:
    """Return the EOS store directory holding stage-1 skims for a year."""
    return f"{BASE_STORE_PATH}/{CAMPAIGN}/{year}/skims"


def get_templates_dir(year: str) -> str:
    """Return the EOS store directory holding stage-2 templates for a year."""
    return f"{BASE_STORE_PATH}/{CAMPAIGN}/{year}/templates"


def get_logs_dir(year: str) -> str:
    """Return the EOS store directory holding job logs for a year."""
    return f"{BASE_STORE_PATH}/{CAMPAIGN}/{year}/logs"

# ============================================================================
# Controller Defaults
# ============================================================================

YEARS_TO_PROCESS = ["2022", "2023", "2024", "2025"]
DEFAULT_YEAR = "2024"

AUTO_SUBMIT = False
AUTO_RESUBMIT_MISSING = False

MERGED_TEMPLATE_FILENAME = "templates_{process}.root"

# Base of the local merged-template tree; the actual files live in a per-year
# subdirectory (see get_local_merged_templates_dir).
LOCAL_MERGED_TEMPLATES_BASE_DIR = str(REPO_ROOT / "condor" / "output" / "templates" / "merged")


def get_local_merged_templates_dir(year: str) -> str:
    """Return the local directory merged templates for a year are copied into."""
    return str(Path(LOCAL_MERGED_TEMPLATES_BASE_DIR) / year)

# ============================================================================
# EOS Filesystem Operations
# ============================================================================

EOS_ROOT = "root://cmseos.fnal.gov"
XRD_ROOT = "root://cmsxrootd.fnal.gov"


def get_xrdfs_mkdir_command(store_dir: str) -> list:
    """
    Build an ``xrdfs mkdir -p`` command for creating an EOS directory.

    Uses xrdfs (XRootD client) rather than the ``eos`` CLI, since the latter
    is only installed on LPC login nodes and not inside the TIMBER/analysis
    singularity images that condor scripts run in.

    Args:
        store_dir (str): Bare /store/... directory path.

    Returns:
        list: Command argv suitable for subprocess.run.
    """
    host = EOS_ROOT.replace("root://", "").rstrip("/")
    return ["xrdfs", host, "mkdir", "-p", store_dir]


def get_xrdcp_command() -> str:
    """
    Get xrdcp command for copying files to/from XRD.
    
    Returns:
        str: Full xrdcp command with EOS root
    """
    return "xrdcp"


def get_store_xrd_path(store_path: str) -> str:
    """
    Convert a /store path to XRD URL.
    
    Args:
        store_path (str): Path like "/store/user/roguljic/run3-xhbb-anomaly/..."
    
    Returns:
        str: XRD URL like "root://cmsxrootd.fnal.gov//store/user/roguljic/..." Note that this is different from the eos path which is "root://cmseos.fnal.gov//store/user/roguljic/..." and applies to lpc-stored files only
    """
    if not store_path.startswith("/store"):
        raise ValueError(f"Expected /store path, got: {store_path}")
    return f"{XRD_ROOT}/{store_path}"


def get_store_eos_path(store_path: str) -> str:
    """
    Convert a /store path to EOS URL.
    
    Args:
        store_path (str): Path like "/store/user/roguljic/run3-xhbb-anomaly/..."
    
    Returns:
        str: EOS store URL like "root://cmseos.fnal.gov//store/user/roguljic/..." Note that this is different from the XRD path which is "root://cmsxrootd.fnal.gov//store/user/roguljic/..." and applies to lpc-stored files only
    """
    if not store_path.startswith("/store"):
        raise ValueError(f"Expected /store path, got: {store_path}")
    return f"{EOS_ROOT}/{store_path}"