"""Measurement of the analysis trigger efficiency and data/MC SF.

Reads the trigeff_* histogram pairs booked by selection_and_templating.py,
book_trigger_efficiency_histograms(), from merged template files, computes:
  - eff_dijet(m_jj) and eff_pnetbb_given_not_dijet(h_cand_xbb) for data and each MC process
  - the decomposed OR efficiency
    eff_dijet + (1-eff_dijet)*eff_pnetbb_given_not_dijet, closure-tested against the
    direct 2D trigeff_or measurement
  - data/MC scale factors per trigger

Usage:
  anomaly_exec trigger_studies/measure_trigger_efficiency.py \
      --templates-dir condor/output/templates/merged
"""
import argparse
import os

import sys

import numpy as np
import ROOT
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplhep as hep

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from filelists.xsecs import int_lumi

hep.style.use("CMS")

# TEfficiency prints an Info/RuntimeWarning pair on every construction, saying it cannot
# give Clopper-Pearson intervals for weighted histograms and is using the normal
# approximation instead. That is understood and accepted here (see efficiency()), so
# suppress it rather than emitting it hundreds of times.
ROOT.gErrorIgnoreLevel = ROOT.kError

CMS_LABEL_KWARGS = dict(
    text="WiP", data=True, year=2024, com=13.6,
    lumi=int_lumi["2024"] / 1000.0, lumi_format="{0:.0f}",
)

DATA_FILE = "templates_Muon2024.root"
MC_FILES = {
    "QCD": "templates_QCD.root",
    "TT": "templates_TT.root",
    "signal MX2500_MY400": "templates_MX2500_MY400.root",
}

# Used in titles/legends
DIJET_TRIGGER_SHORT = "AK8DiPFJet250_250_SD40"
PNETBB_TRIGGER_SHORT = "AK8PFJet250_SD40_PNetBB0p06"

HIST_NAMES = {
    "dijet_total": "trigeff_dijet_total",
    "dijet_pass": "trigeff_dijet_pass",
    "pnetbb_total": "trigeff_pnetbb_given_not_dijet_total",
    "pnetbb_pass": "trigeff_pnetbb_given_not_dijet_pass",
}

M_JJ_REBIN = 2
SCORE_REBIN = 50
CLOSURE_MIN_EFFECTIVE_ENTRIES = 50


def get_rebinned(f, key, rebin):
    """Fetch a trigeff histogram by key and rebin it."""
    name = HIST_NAMES[key]
    h = f.Get(name)
    if not h:
        raise RuntimeError(f"missing histogram {name} in {f.GetName()}")
    h = h.Clone(f"{name}_clone_{f.GetName()}")
    h.SetDirectory(0)
    h.Rebin(rebin)
    return h


def efficiency(total, passed):
    """Return (x_centers, eff, err_low, err_high) from TEfficiency.

    Note the requested Clopper-Pearson interval is only used for unweighted input;
    these templates are xsec-weighted, so ROOT falls back to the normal approximation.
    That is fine at the statistics here, but the intervals are not exact.
    """
    teff = ROOT.TEfficiency(passed, total)
    teff.SetStatisticOption(ROOT.TEfficiency.kFCP)
    n = total.GetNbinsX()
    x, eff, lo, hi = [], [], [], []
    for i in range(1, n + 1):
        if total.GetBinContent(i) <= 0:
            continue
        x.append(total.GetBinCenter(i))
        e = teff.GetEfficiency(i)
        eff.append(e)
        lo.append(e - teff.GetEfficiencyErrorLow(i))
        hi.append(e + teff.GetEfficiencyErrorUp(i))
    return x, eff, lo, hi


def project(h2, axis, rebin, name):
    """1D projection of a 2D trigeff_or histogram, rebinned to match the 1D measurements."""
    h = h2.ProjectionX(name) if axis == "x" else h2.ProjectionY(name)
    h.SetDirectory(0)
    h.Rebin(rebin)
    return h


def load_process(path):
    f = ROOT.TFile.Open(path)
    if not f or f.IsZombie():
        raise RuntimeError(f"cannot open {path}")
    dijet_total = get_rebinned(f, "dijet_total", M_JJ_REBIN)
    dijet_pass = get_rebinned(f, "dijet_pass", M_JJ_REBIN)
    pnetbb_total = get_rebinned(f, "pnetbb_total", SCORE_REBIN)
    pnetbb_pass = get_rebinned(f, "pnetbb_pass", SCORE_REBIN)
    or_total = f.Get("trigeff_or_total")
    or_pass = f.Get("trigeff_or_pass")
    or_total.SetDirectory(0)
    or_pass.SetDirectory(0)
    tag = os.path.basename(path).replace(".root", "")
    hists = {
        "dijet_total": dijet_total, "dijet_pass": dijet_pass,
        "pnetbb_total": pnetbb_total, "pnetbb_pass": pnetbb_pass,
        "or_total": or_total, "or_pass": or_pass,
    }
    # 1D OR efficiencies
    for var, axis, rebin in (("mjj", "x", M_JJ_REBIN), ("xbb", "y", SCORE_REBIN)):
        for which in ("total", "pass"):
            hists[f"or_{var}_{which}"] = project(
                hists[f"or_{which}"], axis, rebin, f"or_{var}_{which}_{tag}")
    f.Close()
    return hists


def efficiency_by_bin(total, passed):
    """Same as efficiency(), but keyed by bin index so it can be matched to a 2D histogram."""
    teff = ROOT.TEfficiency(passed, total)
    teff.SetStatisticOption(ROOT.TEfficiency.kFCP)
    eff_by_bin, err_by_bin = {}, {}
    for i in range(1, total.GetNbinsX() + 1):
        if total.GetBinContent(i) <= 0:
            continue
        eff_by_bin[i] = teff.GetEfficiency(i)
        err_by_bin[i] = (teff.GetEfficiencyErrorLow(i) + teff.GetEfficiencyErrorUp(i)) / 2
    return eff_by_bin, err_by_bin


def effective_entries(hist, ix, iy):
    """Effective (unweighted-equivalent) entry count of a 2D bin: (sum w)^2 / sum w^2."""
    content = hist.GetBinContent(ix, iy)
    error = hist.GetBinError(ix, iy)
    if content <= 0 or error <= 0:
        return 0.0
    return (content / error) ** 2


def closure_test(hists, label):
    """Test the factorisation behind the decomposed eff_OR against the direct 2D measurement.

    eff_OR = eff_dijet + (1-eff_dijet)*eff_pnetbb_given_not_dijet

    Checks how much the efficiency changes if the 1D dijet and pnetbb efficiencies are
    used instead of the 2D efficiency directly, i.e. whether eff_dijet depends (mostly)
    on m_jj alone and eff_pnetbb_given_not_dijet on h_cand_xbb alone. The identity above
    is exact per 2D cell; only the 1D marginalisation is an approximation.

    Deviations are reported in absolute efficiency units. Individual bins are noisy, so
    the number to read is the mean bias over bins and its uncertainty; requiring a
    minimum number of effective entries keeps that noise from dominating.
    """
    eff_a, err_a = efficiency_by_bin(hists["dijet_total"], hists["dijet_pass"])
    eff_b, err_b = efficiency_by_bin(hists["pnetbb_total"], hists["pnetbb_pass"])
    if not eff_a or not eff_b:
        print(f"  [{label}] not enough stats for closure test")
        return

    # Rebin the 2D histogram to exactly the 1D projections' binning
    or_total = hists["or_total"].Clone(f"or_total_closure_{label}")
    or_pass = hists["or_pass"].Clone(f"or_pass_closure_{label}")
    or_total.RebinX(M_JJ_REBIN)
    or_pass.RebinX(M_JJ_REBIN)
    or_total.RebinY(SCORE_REBIN)
    or_pass.RebinY(SCORE_REBIN)

    nx, ny = or_total.GetNbinsX(), or_total.GetNbinsY()
    diffs, noise, directs = [], [], []
    n_unphysical = 0
    for ix in range(1, nx + 1):
        a = ix
        if a not in eff_a:
            continue
        for iy in range(1, ny + 1):
            b = iy
            if b not in eff_b:
                continue
            n_eff = effective_entries(or_total, ix, iy)
            if n_eff < CLOSURE_MIN_EFFECTIVE_ENTRIES:
                continue

            direct = or_pass.GetBinContent(ix, iy) / or_total.GetBinContent(ix, iy)
            if not 0.0 <= direct <= 1.0:
                # Can happen with negative event weights (ttbar for example)
                n_unphysical += 1
                continue
            var_direct = max(direct * (1 - direct), 1.0 / n_eff) / n_eff

            predicted = eff_a[a] + (1 - eff_a[a]) * eff_b[b]
            # d/d(eff_a) = 1 - eff_b, d/d(eff_b) = 1 - eff_a
            var_pred = ((1 - eff_b[b]) * err_a[a]) ** 2 + ((1 - eff_a[a]) * err_b[b]) ** 2

            sigma = (var_direct + var_pred) ** 0.5
            if sigma > 0:
                directs.append(direct)
                diffs.append(direct - predicted)
                noise.append(sigma)

    if n_unphysical:
        print(f"  [{label}] WARNING: skipped {n_unphysical} bins with efficiency outside "
              f"[0, 1] (negative generator weights)")

    if not diffs:
        print(f"  [{label}] no bins with >= {CLOSURE_MIN_EFFECTIVE_ENTRIES} effective entries")
        return

    diffs, noise, directs = np.array(diffs), np.array(noise), np.array(directs)
    bias_err = diffs.std() / len(diffs) ** 0.5
    mean_eff = directs.mean()
    rel_bias = 100 * diffs.mean() / mean_eff if mean_eff > 0 else float("nan")
    print(f"  [{label}] {len(diffs)} bins with n_eff >= {CLOSURE_MIN_EFFECTIVE_ENTRIES}: "
          f"eff(2D)={mean_eff:.3f}, "
          f"bias={diffs.mean():+.4f} +/- {bias_err:.4f} ({rel_bias:+.1f}% of eff), "
          f"scatter={diffs.std():.3f} (stat. noise {noise.mean():.3f}), "
          f"max|diff|={np.abs(diffs).max():.3f}")


def plot_efficiency(ax, x, eff, lo, hi, label, **kwargs):
    yerr_lo = [e - l for e, l in zip(eff, lo)]
    yerr_hi = [h - e for e, h in zip(eff, hi)]
    ax.errorbar(x, eff, yerr=[yerr_lo, yerr_hi], label=label, fmt="o-", markersize=4, **kwargs)


def plot_trigger_efficiency_and_sf(data_hists, mc_hists, total_key, pass_key, x_label,
                                    legend_title, out_path):
    """One turn-on-curve figure (efficiency + ratio panels) for a single trigger path."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 8), sharex=True,
                                    gridspec_kw={"height_ratios": [3, 1]})
    hep.cms.label(ax=ax1, **CMS_LABEL_KWARGS)

    x_data, eff_data, lo_data, hi_data = efficiency(data_hists[total_key], data_hists[pass_key])
    plot_efficiency(ax1, x_data, eff_data, lo_data, hi_data, "Data (Muon2024)", color="black")
    err_data = [(h - l) / 2 for l, h in zip(lo_data, hi_data)]

    mc_effs = {}
    for label, hists in mc_hists.items():
        x_mc, eff_mc, lo_mc, hi_mc = efficiency(hists[total_key], hists[pass_key])
        plot_efficiency(ax1, x_mc, eff_mc, lo_mc, hi_mc, label)
        err_mc = [(h - l) / 2 for l, h in zip(lo_mc, hi_mc)]
        mc_effs[label] = (x_mc, eff_mc, err_mc)

    ax1.set_ylabel("Efficiency", fontsize=20)
    ax1.legend(fontsize=13, title=legend_title, title_fontsize=13, loc="lower right")
    ax1.set_ylim(0, 1.05)
    ax1.tick_params(labelsize=16)

    for label, (x_mc, eff_mc, err_mc) in mc_effs.items():
        sf_x, sf_y, sf_err = [], [], []
        for xd, ed, e_d in zip(x_data, eff_data, err_data):
            j = min(range(len(x_mc)), key=lambda k: abs(x_mc[k] - xd))
            if abs(x_mc[j] - xd) < 1e-6 and eff_mc[j] > 0:
                sf = ed / eff_mc[j]
                sf_x.append(xd)
                sf_y.append(sf)
                sf_err.append(sf * ((e_d / ed) ** 2 + (err_mc[j] / eff_mc[j]) ** 2) ** 0.5)
        ax2.errorbar(sf_x, sf_y, yerr=sf_err, fmt="o-", markersize=4, label=label)
    ax2.axhline(1.0, color="gray", linestyle="--", linewidth=1)
    ax2.set_ylabel("Ratio", fontsize=20)
    ax2.set_xlabel(x_label, fontsize=20)
    ax2.set_ylim(0.5, 1.5)
    ax2.tick_params(labelsize=16)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Saved {out_path}")


def plot_2d_efficiency_map(hists, colorbar_label, out_path):
    """2D map of the direct (non-decomposed) OR efficiency vs (m_jj, h_cand_xbb)."""
    total, passed = hists["or_total"].Clone(), hists["or_pass"].Clone()
    # rebin h_cand_xbb (1000 bins) down to a readable resolution; keep m_jj as-is
    total.RebinY(SCORE_REBIN)
    passed.RebinY(SCORE_REBIN)

    nx, ny = total.GetNbinsX(), total.GetNbinsY()
    eff = np.full((ny, nx), np.nan)
    for ix in range(1, nx + 1):
        for iy in range(1, ny + 1):
            t = total.GetBinContent(ix, iy)
            if t > 0:
                eff[iy - 1, ix - 1] = passed.GetBinContent(ix, iy) / t

    x_edges = [total.GetXaxis().GetBinLowEdge(i) for i in range(1, nx + 2)]
    y_edges = [total.GetYaxis().GetBinLowEdge(i) for i in range(1, ny + 2)]

    fig, ax = plt.subplots(figsize=(10, 8))
    mesh = ax.pcolormesh(x_edges, y_edges, eff, cmap="viridis", vmin=0, vmax=1)
    cbar = fig.colorbar(mesh, ax=ax)
    cbar.set_label(colorbar_label, fontsize=20)
    cbar.ax.tick_params(labelsize=16)
    ax.set_xlabel("$m_{jj}$ [GeV]", fontsize=20)
    ax.set_ylabel("h candidate Xbb", fontsize=20)
    ax.tick_params(labelsize=16)
    fig.tight_layout()
    hep.cms.label(ax=ax, **CMS_LABEL_KWARGS)
    fig.savefig(out_path, dpi=150)
    print(f"Saved {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--templates-dir", default="condor/output/templates/merged")
    parser.add_argument("--out-dir", default="trigger_studies/plots")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    data_path = os.path.join(args.templates_dir, DATA_FILE)
    print(f"Loading data: {data_path}")
    data_hists = load_process(data_path)

    mc_hists = {}
    for label, fname in MC_FILES.items():
        path = os.path.join(args.templates_dir, fname)
        if not os.path.exists(path):
            print(f"  [skip] {path} not found")
            continue
        mc_hists[label] = load_process(path)

    print("\n=== Closure test: eff_OR direct (2D) vs decomposition ===")
    print("data (Muon2024):")
    closure_test(data_hists, "data")
    for label, hists in mc_hists.items():
        closure_test(hists, label)

    plot_trigger_efficiency_and_sf(
        data_hists, mc_hists, "dijet_total", "dijet_pass", "$m_{jj}$ [GeV]",
        DIJET_TRIGGER_SHORT,
        os.path.join(args.out_dir, "dijet_efficiency_sf.png"),
    )
    plot_trigger_efficiency_and_sf(
        data_hists, mc_hists, "pnetbb_total", "pnetbb_pass", "h candidate Xbb",
        f"{PNETBB_TRIGGER_SHORT}\nand not {DIJET_TRIGGER_SHORT}",
        os.path.join(args.out_dir, "pnetbb_efficiency_sf.png"),
    )

    or_legend_title = f"{DIJET_TRIGGER_SHORT}\nOR {PNETBB_TRIGGER_SHORT}"
    plot_trigger_efficiency_and_sf(
        data_hists, mc_hists, "or_mjj_total", "or_mjj_pass", "$m_{jj}$ [GeV]",
        or_legend_title,
        os.path.join(args.out_dir, "or_efficiency_sf_mjj.png"),
    )
    plot_trigger_efficiency_and_sf(
        data_hists, mc_hists, "or_xbb_total", "or_xbb_pass", "h candidate Xbb",
        or_legend_title,
        os.path.join(args.out_dir, "or_efficiency_sf_xbb.png"),
    )

    plot_2d_efficiency_map(
        data_hists, "Data trigger efficiency",
        os.path.join(args.out_dir, "or_efficiency_2d_data.png"),
    )
    if "QCD" in mc_hists:
        plot_2d_efficiency_map(
            mc_hists["QCD"], "QCD trigger efficiency",
            os.path.join(args.out_dir, "or_efficiency_2d_qcd.png"),
        )


if __name__ == "__main__":
    main()
