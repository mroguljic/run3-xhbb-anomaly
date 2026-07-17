"""Derive an (m_jj, m_jY) integration window from a signal's own kinematic
peak, independent of any tagger WP cut ("fixed-fraction contour" method).
"""

from typing import Tuple

import numpy as np

from tagger_studies.thn_utils import project_mjj_mjy


def find_signal_window(thn, target_fraction: float = 0.7) -> Tuple[float, float, float, float]:
    """Return (mjj_lo, mjj_hi, mjy_lo, mjy_hi) in GeV.

    Starts a 1-bin box at the (m_jj, m_jY) peak bin of the signal's
    tagger-cut-independent 2D projection, then greedily grows the box by one
    row/column at a time -- always extending in whichever of the 4 directions
    (left/right/up/down, clipped at the axis edges) adds the most content --
    until the box captures at least target_fraction of the total 2D integral.
    """
    values, mjj_edges, mjy_edges = project_mjj_mjy(thn)
    n_mjj, n_mjy = values.shape
    total = values.sum()
    if total <= 0:
        raise ValueError("Signal THnD has zero or negative total weight; cannot find a window")

    peak_i, peak_j = np.unravel_index(np.argmax(values), values.shape)
    i_lo, i_hi = peak_i, peak_i  # inclusive bin-index box [i_lo, i_hi] x [j_lo, j_hi]
    j_lo, j_hi = peak_j, peak_j

    captured = values[i_lo:i_hi + 1, j_lo:j_hi + 1].sum()
    target = target_fraction * total

    while captured < target and not (i_lo == 0 and i_hi == n_mjj - 1 and j_lo == 0 and j_hi == n_mjy - 1):
        candidates = []  # (added_content, direction)
        if i_lo > 0:
            candidates.append((values[i_lo - 1, j_lo:j_hi + 1].sum(), "left"))
        if i_hi < n_mjj - 1:
            candidates.append((values[i_hi + 1, j_lo:j_hi + 1].sum(), "right"))
        if j_lo > 0:
            candidates.append((values[i_lo:i_hi + 1, j_lo - 1].sum(), "down"))
        if j_hi < n_mjy - 1:
            candidates.append((values[i_lo:i_hi + 1, j_hi + 1].sum(), "up"))

        added, direction = max(candidates, key=lambda c: c[0])
        if direction == "left":
            i_lo -= 1
        elif direction == "right":
            i_hi += 1
        elif direction == "down":
            j_lo -= 1
        else:
            j_hi += 1
        captured += added

    mjj_lo, mjj_hi = mjj_edges[i_lo], mjj_edges[i_hi + 1]
    mjy_lo, mjy_hi = mjy_edges[j_lo], mjy_edges[j_hi + 1]
    return mjj_lo, mjj_hi, mjy_lo, mjy_hi
