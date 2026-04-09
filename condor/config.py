"""
Condor Submission Framework Configuration
"""

BATCH_TARGET_EVENTS = 1000000
CAMPAIGN="20260401"
BASE_STORE_PATH = "/store/user/roguljic/run3-xhbb-anomaly"

OUTPUT = {
    "skims_dir": f"{BASE_STORE_PATH}/{CAMPAIGN}/skims",
    "templates_dir": f"{BASE_STORE_PATH}/{CAMPAIGN}/templates",
    "logs_dir": f"{BASE_STORE_PATH}/{CAMPAIGN}/logs",
}
