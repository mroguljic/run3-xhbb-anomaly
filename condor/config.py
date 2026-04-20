"""
Condor Submission Framework Configuration
"""

BATCH_TARGET_EVENTS = 2000000 # Number of input events per skimming batch
TEMPLATE_BATCH_SIZE = 3. # GB of input skims per template batch (for template generation)
CAMPAIGN = "20260410"
BASE_STORE_PATH = "/store/user/roguljic/run3-xhbb-anomaly"

OUTPUT = {
    "skims_dir": f"{BASE_STORE_PATH}/{CAMPAIGN}/skims",
    "templates_dir": f"{BASE_STORE_PATH}/{CAMPAIGN}/templates",
    "logs_dir": f"{BASE_STORE_PATH}/{CAMPAIGN}/logs",
}

# ============================================================================
# Controller Defaults
# ============================================================================

YEARS_TO_PROCESS = ["2022", "2023", "2024"]
DEFAULT_YEAR = "2024"

AUTO_SUBMIT = False
AUTO_RESUBMIT_MISSING = False
CONTROLLER_INTERACTIVE = True

MERGED_TEMPLATE_FILENAME = "templates_{process}.root"
LOCAL_MERGED_TEMPLATES_DIR = "output/templates/merged"

# ============================================================================
# EOS Filesystem Operations
# ============================================================================

EOS_ROOT = "root://cmseos.fnal.gov"
XRD_ROOT = "root://cmsxrootd.fnal.gov"

# EOS command aliases for operations on /store
EOS_MKDIR = f"eos {EOS_ROOT} mkdir"
EOS_LS = f"eos {EOS_ROOT} ls"
EOS_CP = f"eos {EOS_ROOT} cp"

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