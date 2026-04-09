"""
Condor Submission Framework Configuration
"""

BATCH_TARGET_EVENTS = 1000000
CAMPAIGN = "20260401"
BASE_STORE_PATH = "/store/user/roguljic/run3-xhbb-anomaly"

OUTPUT = {
    "skims_dir": f"{BASE_STORE_PATH}/{CAMPAIGN}/skims",
    "templates_dir": f"{BASE_STORE_PATH}/{CAMPAIGN}/templates",
    "logs_dir": f"{BASE_STORE_PATH}/{CAMPAIGN}/logs",
}

# ============================================================================
# EOS Filesystem Operations
# ============================================================================

EOS_ROOT = "root://cmseos.fnal.gov"

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
        str: XRD URL like "root://cmseos.fnal.gov//store/user/roguljic/..."
    """
    if not store_path.startswith("/store"):
        raise ValueError(f"Expected /store path, got: {store_path}")
    return f"{EOS_ROOT}/{store_path}"
