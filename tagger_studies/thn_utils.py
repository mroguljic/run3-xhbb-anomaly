"""Low-level helpers for reading the joint (m_jj, m_jY, Xbb, anti-QCD) THnD
histogram (`inclusive_h_xbb_vs_y_antiqcd`, booked in
selection_and_templating.py's book_tagger_scan_histogram) from merged
per-process template files.

Axis order is fixed by book_tagger_scan_histogram: 0=m_jj, 1=m_jY, 2=Xbb,
3=anti-QCD.
"""

from typing import Tuple

import numpy as np
import ROOT

THN_HIST_NAME = "inclusive_h_xbb_vs_y_antiqcd"

MJJ_AXIS, MJY_AXIS, XBB_AXIS, ANTIQCD_AXIS = 0, 1, 2, 3


def open_thn(file_path: str, hist_name: str = THN_HIST_NAME):
    """Open a merged template file and return the THnD, keeping the file alive."""
    root_file = ROOT.TFile.Open(file_path, "READ")
    if not root_file or root_file.IsZombie():
        raise FileNotFoundError(f"Cannot open ROOT file: {file_path}")

    thn = root_file.Get(hist_name)
    if not thn:
        root_file.Close()
        raise RuntimeError(f"Histogram '{hist_name}' not found in {file_path}")

    thn._keepalive_file = root_file  # prevent the TFile from being garbage collected
    return thn


def window_yield(
    thn,
    mjj_range: Tuple[float, float],
    mjy_range: Tuple[float, float],
    xbb_min: float,
    antiqcd_min: float,
) -> float:
    """Sum bin contents of the THnD over an (m_jj, m_jY) window and a WP cut.

    mjj_range / mjy_range are [lo, hi) physical windows (GeV). xbb_min /
    antiqcd_min are lower-bound WP cuts (score > wp), open on the high end
    (up to the axis max).

    Uses an explicit nested bin loop rather than THnBase::Projection to avoid
    relying on its axis-range semantics for the projected axes.
    """
    ax_mjj = thn.GetAxis(MJJ_AXIS)
    ax_mjy = thn.GetAxis(MJY_AXIS)
    ax_xbb = thn.GetAxis(XBB_AXIS)
    ax_antiqcd = thn.GetAxis(ANTIQCD_AXIS)

    i_lo, i_hi = ax_mjj.FindBin(mjj_range[0]), ax_mjj.FindBin(mjj_range[1] - 1e-6)
    j_lo, j_hi = ax_mjy.FindBin(mjy_range[0]), ax_mjy.FindBin(mjy_range[1] - 1e-6)
    k_lo, k_hi = ax_xbb.FindBin(xbb_min), ax_xbb.GetNbins()
    l_lo, l_hi = ax_antiqcd.FindBin(antiqcd_min), ax_antiqcd.GetNbins()

    total = 0.0
    idx = np.zeros(4, dtype=np.int32)
    for i in range(i_lo, i_hi + 1):
        idx[MJJ_AXIS] = i
        for j in range(j_lo, j_hi + 1):
            idx[MJY_AXIS] = j
            for k in range(k_lo, k_hi + 1):
                idx[XBB_AXIS] = k
                for l in range(l_lo, l_hi + 1):
                    idx[ANTIQCD_AXIS] = l
                    total += thn.GetBinContent(idx)
    return total


def window_yield_grid(
    thn,
    mjj_range: Tuple[float, float],
    mjy_range: Tuple[float, float],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Sum the THnD over an (m_jj, m_jY) window, keeping Xbb/anti-QCD as a 2D
    grid of raw (per-bin, no WP cut) yields.

    Returns (grid, xbb_edges, antiqcd_edges): grid[k, l] is the yield in Xbb
    bin k+1, anti-QCD bin l+1 within the window; the edges are each axis's own
    bin edges (length n_bins + 1, suitable for pcolormesh), i.e. every WP
    threshold the histogram's binning resolves plus the axis max. A WP cut at
    bin k+1's threshold means edges[k] (the bin's lower edge). Pair with
    suffix_sum_2d to get the yield for every possible (xbb >= wp, antiqcd >= wp)
    combination in one vectorized pass, instead of re-summing the THnD once
    per WP point.
    """
    ax_mjj = thn.GetAxis(MJJ_AXIS)
    ax_mjy = thn.GetAxis(MJY_AXIS)
    ax_xbb = thn.GetAxis(XBB_AXIS)
    ax_antiqcd = thn.GetAxis(ANTIQCD_AXIS)

    i_lo, i_hi = ax_mjj.FindBin(mjj_range[0]), ax_mjj.FindBin(mjj_range[1] - 1e-6)
    j_lo, j_hi = ax_mjy.FindBin(mjy_range[0]), ax_mjy.FindBin(mjy_range[1] - 1e-6)
    n_xbb, n_antiqcd = ax_xbb.GetNbins(), ax_antiqcd.GetNbins()

    grid = np.zeros((n_xbb, n_antiqcd))
    idx = np.zeros(4, dtype=np.int32)
    for i in range(i_lo, i_hi + 1):
        idx[MJJ_AXIS] = i
        for j in range(j_lo, j_hi + 1):
            idx[MJY_AXIS] = j
            for k in range(1, n_xbb + 1):
                idx[XBB_AXIS] = k
                for l in range(1, n_antiqcd + 1):
                    idx[ANTIQCD_AXIS] = l
                    grid[k - 1, l - 1] += thn.GetBinContent(idx)

    xbb_edges = np.array([ax_xbb.GetBinLowEdge(k) for k in range(1, n_xbb + 2)])
    antiqcd_edges = np.array([ax_antiqcd.GetBinLowEdge(l) for l in range(1, n_antiqcd + 2)])
    return grid, xbb_edges, antiqcd_edges


def suffix_sum_2d(grid: np.ndarray) -> np.ndarray:
    """Reverse cumulative sum over both axes: out[k, l] = grid[k:, l:].sum().

    Turns per-bin yields into the "greater-or-equal" yield for every possible
    WP cut at once (i.e. every bin-boundary combination the THnD resolves).
    """
    return grid[::-1, ::-1].cumsum(axis=0).cumsum(axis=1)[::-1, ::-1]


def project_1d(
    thn,
    axis_index: int,
    xbb_min: float,
    antiqcd_min: float,
    other_mass_range: Tuple[float, float] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Project the THnD onto one mass axis (MJJ_AXIS or MJY_AXIS) at its full
    range, after a WP cut, optionally restricting the *other* mass axis to a
    range (e.g. the window from window_finder.find_signal_window).

    Returns (values, edges): values has length == the projected axis's Nbins,
    edges is its bin edges (length + 1).
    """
    if axis_index not in (MJJ_AXIS, MJY_AXIS):
        raise ValueError("axis_index must be MJJ_AXIS or MJY_AXIS")

    ax_mjj = thn.GetAxis(MJJ_AXIS)
    ax_mjy = thn.GetAxis(MJY_AXIS)
    ax_xbb = thn.GetAxis(XBB_AXIS)
    ax_antiqcd = thn.GetAxis(ANTIQCD_AXIS)

    proj_axis = ax_mjj if axis_index == MJJ_AXIS else ax_mjy
    other_axis_index = MJY_AXIS if axis_index == MJJ_AXIS else MJJ_AXIS
    other_axis = ax_mjy if axis_index == MJJ_AXIS else ax_mjj

    n = proj_axis.GetNbins()
    edges = np.array([proj_axis.GetBinLowEdge(b) for b in range(1, n + 2)])

    if other_mass_range is not None:
        other_lo = other_axis.FindBin(other_mass_range[0])
        other_hi = other_axis.FindBin(other_mass_range[1] - 1e-6)
    else:
        other_lo, other_hi = 1, other_axis.GetNbins()

    k_lo, k_hi = ax_xbb.FindBin(xbb_min), ax_xbb.GetNbins()
    l_lo, l_hi = ax_antiqcd.FindBin(antiqcd_min), ax_antiqcd.GetNbins()

    values = np.zeros(n)
    idx = np.zeros(4, dtype=np.int32)
    for p in range(1, n + 1):
        idx[axis_index] = p
        acc = 0.0
        for o in range(other_lo, other_hi + 1):
            idx[other_axis_index] = o
            for k in range(k_lo, k_hi + 1):
                idx[XBB_AXIS] = k
                for l in range(l_lo, l_hi + 1):
                    idx[ANTIQCD_AXIS] = l
                    acc += thn.GetBinContent(idx)
        values[p - 1] = acc

    return values, edges


def project_mjj_mjy(thn) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Collapse the Xbb/anti-QCD axes (full range, no tagger cut) into a 2D grid.

    Returns (values, mjj_edges, mjy_edges): values has shape
    (n_mjj_bins, n_mjy_bins), edges are the THnD's own bin edges in GeV.
    """
    ax_mjj = thn.GetAxis(MJJ_AXIS)
    ax_mjy = thn.GetAxis(MJY_AXIS)
    ax_xbb = thn.GetAxis(XBB_AXIS)
    ax_antiqcd = thn.GetAxis(ANTIQCD_AXIS)

    n_mjj, n_mjy = ax_mjj.GetNbins(), ax_mjy.GetNbins()
    mjj_edges = np.array([ax_mjj.GetBinLowEdge(i) for i in range(1, n_mjj + 2)])
    mjy_edges = np.array([ax_mjy.GetBinLowEdge(j) for j in range(1, n_mjy + 2)])

    values = np.zeros((n_mjj, n_mjy))
    idx = np.zeros(4, dtype=np.int32)
    for i in range(1, n_mjj + 1):
        idx[MJJ_AXIS] = i
        for j in range(1, n_mjy + 1):
            idx[MJY_AXIS] = j
            cell = 0.0
            for k in range(1, ax_xbb.GetNbins() + 1):
                idx[XBB_AXIS] = k
                for l in range(1, ax_antiqcd.GetNbins() + 1):
                    idx[ANTIQCD_AXIS] = l
                    cell += thn.GetBinContent(idx)
            values[i - 1, j - 1] = cell

    return values, mjj_edges, mjy_edges
