"""
Utilities for reading ROOT histograms and converting to numpy arrays.
"""

from typing import Dict, List, Optional, Tuple
import numpy as np
import ROOT


def read_histogram_from_root(file_path: str, hist_name: str, rebin_factor: int = 1) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Read a 1D histogram from a ROOT file and convert to numpy arrays.

    Args:
        file_path: Path to ROOT file
        hist_name: Name of histogram in ROOT file
        rebin_factor: Rebinning factor (1 = no rebinning, 2 = 2x coarser, etc.)

    Returns:
        Tuple of (bin_centers, bin_contents, bin_errors, bin_edges)

    Raises:
        FileNotFoundError: If ROOT file not found
        RuntimeError: If histogram not found in file
    """
    root_file = ROOT.TFile(file_path, "READ")
    if not root_file or root_file.IsZombie():
        raise FileNotFoundError(f"Cannot open ROOT file: {file_path}")

    hist = root_file.Get(hist_name)
    if not hist:
        root_file.Close()
        raise RuntimeError(f"Histogram '{hist_name}' not found in {file_path}")

    # Apply rebinning if requested
    if rebin_factor > 1:
        hist.Rebin(rebin_factor)

    # Extract histogram data
    n_bins = hist.GetNbinsX()
    bin_edges = np.array([hist.GetBinLowEdge(i) for i in range(1, n_bins + 2)])
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
    bin_contents = np.array([hist.GetBinContent(i) for i in range(1, n_bins + 1)])
    bin_errors = np.array([hist.GetBinError(i) for i in range(1, n_bins + 1)])

    root_file.Close()
    return bin_centers, bin_contents, bin_errors, bin_edges


def read_histograms_from_files(
    file_dict: Dict[str, str],
    histogram_names: List[str],
    rebin_factors: Optional[Dict[str, int]] = None,
) -> Dict[str, Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]]]:
    """
    Read multiple histograms from multiple ROOT files with optional rebinning.

    Args:
        file_dict: Dict mapping process names to ROOT file paths
        histogram_names: List of histogram names to read
        rebin_factors: Optional dict mapping histogram names to rebin factors (default: 1)

    Returns:
        Nested dict: {process: {histogram_name: (bin_centers, bin_contents, bin_errors, bin_edges)}}

    Raises:
        RuntimeError: If any histogram is missing from any file
    """
    if rebin_factors is None:
        rebin_factors = {}

    data = {}
    missing_histograms = []

    for process, file_path in file_dict.items():
        data[process] = {}
        for hist_name in histogram_names:
            try:
                rebin = rebin_factors.get(hist_name, 1)
                bin_centers, bin_contents, bin_errors, bin_edges = read_histogram_from_root(
                    file_path, hist_name, rebin_factor=rebin
                )
                data[process][hist_name] = (bin_centers, bin_contents, bin_errors, bin_edges)
            except RuntimeError as e:
                missing_histograms.append(f"{process}: {str(e)}")

    if missing_histograms:
        error_msg = "Missing histograms:\n  " + "\n  ".join(missing_histograms)
        raise RuntimeError(error_msg)

    return data


def identify_signal_processes(process_names: List[str]) -> List[str]:
    """
    Identify signal processes from list of process names.
    Signal processes typically start with 'XToYHto' or similar anomalous patterns.

    Args:
        process_names: List of all process names

    Returns:
        List of process names identified as signal
    """
    signal_patterns = ["XToYHto", "MX", "MY"]
    signal_processes = []
    for process in process_names:
        if any(pattern in process for pattern in signal_patterns):
            signal_processes.append(process)
    return signal_processes
