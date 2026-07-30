"""
Template histogram plotter using matplotlib and mplhep.
"""

from typing import Dict, List, Optional, Tuple
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator, MaxNLocator
import mplhep as hep
from pathlib import Path

from plotting.config import (
    HISTOGRAMS_TO_PLOT,
    PROCESSES,
    PROCESS_TYPE_DEFAULTS,
    FIGURE_SIZE,
    DPI,
    LEGEND_LOC,
    LEGEND_FONTSIZE,
    XLABEL_FONTSIZE,
    YLABEL_FONTSIZE,
    SIGNAL_LINE_STYLE,
    SIGNAL_LINE_WIDTH,
    DATA_MARKER,
    DATA_MARKERSIZE,
    DATA_LINE_WIDTH,
    CMS_STYLE,
    CMS_LABEL_FONTSCALES,
    MATPLOTLIB_RCPARAMS,
    OUTPUT_PLOTS_DIR,
    RATIO_PANEL,
    RATIO_FIGURE_SIZE,
    RATIO_HEIGHT_RATIOS,
    RATIO_Y_RANGE,
    RATIO_YLABEL,
    RATIO_MC_BAND_COLOR,
    RATIO_MC_BAND_ALPHA,
    RATIO_MC_BAND_LABEL,
)
from filelists.xsecs import get_int_lumi


def get_lumi_fb(year: Optional[str]) -> Optional[float]:
    """
    Integrated luminosity in /fb for the CMS label, or None if unknown.

    xsecs.int_lumi is in /pb, while mplhep's CMS label expects /fb.
    """
    if not year:
        return None
    try:
        return get_int_lumi(year) / 1000.0
    except ValueError as e:
        print(f"  WARNING: no luminosity for the CMS label ({e})")
        return None


def plot_histogram(
    histogram_name: str,
    nice_label: str,
    histogram_data: Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]],
    output_dir: Path,
    year: Optional[str] = None,
    blind_data: bool = False,
    log_y: bool = False,
    x_range: Optional[Tuple[float, float]] = None,
    y_range: Optional[Tuple[float, float]] = None,
    ratio_panel: bool = RATIO_PANEL,
) -> None:
    """
    Create a stacked plot with background, data, and signal overlays.

    Args:
        histogram_name: Name of histogram (for output filename)
        nice_label: Nice LaTeX label for x-axis
        histogram_data: Dict mapping process keys to (bin_centers, bin_contents, bin_errors, bin_edges)
        output_dir: Directory to save plots
        year: Optional year label
        blind_data: Whether to hide data points for this histogram
        log_y: Whether to use logarithmic y-axis scale
        x_range: Optional (xmin, xmax) for x-axis limits
        y_range: Optional (ymin, ymax) for y-axis limits
        ratio_panel: Whether to add a data/MC ratio panel (skipped if data is absent/blinded)

    Raises:
        ValueError: If required process types are missing
    """
    # Set mplhep style and project-specific rcParams
    plt.style.use(hep.style.CMS)
    plt.rcParams.update(MATPLOTLIB_RCPARAMS)

    # Categorize processes by type
    background_processes = []
    data_processes = []
    signal_processes = []

    for process_key in histogram_data.keys():
        if process_key in PROCESSES:
            process_type = PROCESSES[process_key].get("type", "bkg")
            if process_type == "bkg":
                background_processes.append(process_key)
            elif process_type == "data" and not blind_data:
                data_processes.append(process_key)
            elif process_type == "signal":
                signal_processes.append(process_key)

    if not background_processes:
        raise ValueError(f"No background processes found in {list(histogram_data.keys())}")

    # A ratio panel only makes sense with unblinded data to divide by
    draw_ratio = ratio_panel and len(data_processes) > 0

    if draw_ratio:
        fig, (ax, rax) = plt.subplots(
            2,
            1,
            figsize=RATIO_FIGURE_SIZE,
            sharex=True,
            gridspec_kw={"height_ratios": RATIO_HEIGHT_RATIOS, "hspace": 0.07},
        )
    else:
        fig, ax = plt.subplots(figsize=FIGURE_SIZE)
        rax = None

    # Extract bin edges from first process
    bin_edges = histogram_data[histogram_data.keys().__iter__().__next__()][3]

    # Prepare stacked background histograms
    stack_contents = []
    stack_labels = []
    stack_colors = []

    total_bkg = np.zeros(len(bin_edges) - 1)
    total_bkg_err2 = np.zeros(len(bin_edges) - 1)

    for process_key in background_processes:
        process_config = PROCESSES[process_key]
        _, bin_contents, bin_errors, _ = histogram_data[process_key]
        stack_contents.append(bin_contents)
        stack_labels.append(process_config.get("label", process_key))
        stack_colors.append(process_config.get("color", PROCESS_TYPE_DEFAULTS["bkg"]["color"]))
        total_bkg = total_bkg + bin_contents
        total_bkg_err2 = total_bkg_err2 + bin_errors**2

    # Plot stacked backgrounds
    if stack_contents:
        ax.hist(
            [bin_edges[:-1]] * len(stack_contents),
            bins=bin_edges,
            weights=stack_contents,
            stacked=True,
            label=stack_labels,
            color=stack_colors,
            edgecolor="none",
            linewidth=0.5,
        )

    # Plot data with error bars
    for data_key in data_processes:
        data_config = PROCESSES[data_key]
        bin_centers, bin_contents, bin_errors, _ = histogram_data[data_key]
        ax.errorbar(
            bin_centers,
            bin_contents,
            yerr=bin_errors,
            fmt=DATA_MARKER,
            color=data_config.get("color", PROCESS_TYPE_DEFAULTS["data"]["color"]),
            markersize=DATA_MARKERSIZE,
            linewidth=DATA_LINE_WIDTH,
            elinewidth=DATA_LINE_WIDTH,
            capsize=3,
            label=data_config.get("label", data_key),
            zorder=10,
        )

    # Plot signal
    for signal_key in signal_processes:
        signal_config = PROCESSES[signal_key]
        _, bin_contents, _, signal_bin_edges = histogram_data[signal_key]
        ax.hist(
            signal_bin_edges[:-1],
            bins=signal_bin_edges,
            weights=bin_contents,
            histtype="step",
            linestyle=SIGNAL_LINE_STYLE,
            linewidth=SIGNAL_LINE_WIDTH,
            color=signal_config.get("color", PROCESS_TYPE_DEFAULTS["signal"]["color"]),
            label=signal_config.get("label", signal_key),
            zorder=5,
        )

    # Ratio panel: data / total background, with the MC stat uncertainty as a band around 1
    if draw_ratio:
        total_bkg_err = np.sqrt(total_bkg_err2)
        nonzero = total_bkg > 0

        rax.axhline(1.0, color="black", linestyle="--", linewidth=1.0, zorder=1)

        band_up = np.where(nonzero, 1.0 + total_bkg_err / np.where(nonzero, total_bkg, 1.0), 1.0)
        band_down = np.where(nonzero, 1.0 - total_bkg_err / np.where(nonzero, total_bkg, 1.0), 1.0)
        rax.stairs(
            band_up,
            bin_edges,
            baseline=band_down,
            fill=True,
            color=RATIO_MC_BAND_COLOR,
            alpha=RATIO_MC_BAND_ALPHA,
            linewidth=0,
            label=RATIO_MC_BAND_LABEL,
            zorder=2,
        )

        for data_key in data_processes:
            data_config = PROCESSES[data_key]
            bin_centers, bin_contents, bin_errors, _ = histogram_data[data_key]
            ratio = np.where(nonzero, bin_contents / np.where(nonzero, total_bkg, 1.0), np.nan)
            ratio_err = np.where(nonzero, bin_errors / np.where(nonzero, total_bkg, 1.0), np.nan)
            # Empty data bins carry no information here; drop them instead of piling up at 0
            visible = nonzero & (bin_contents > 0)
            rax.errorbar(
                bin_centers[visible],
                ratio[visible],
                yerr=ratio_err[visible],
                fmt=DATA_MARKER,
                color=data_config.get("color", PROCESS_TYPE_DEFAULTS["data"]["color"]),
                markersize=DATA_MARKERSIZE,
                linewidth=DATA_LINE_WIDTH,
                elinewidth=DATA_LINE_WIDTH,
                capsize=3,
                zorder=10,
            )

        rax.set_ylim(RATIO_Y_RANGE)
        # Prune the top tick so its label does not collide with the main panel
        rax.yaxis.set_major_locator(MaxNLocator(nbins=5, prune="upper"))
        rax.yaxis.set_minor_locator(AutoMinorLocator())
        rax.set_ylabel(RATIO_YLABEL, fontsize=YLABEL_FONTSIZE)
        rax.set_xlabel(nice_label, fontsize=XLABEL_FONTSIZE)
        rax.grid(False)
        rax.set_axisbelow(False)
        rax.tick_params(axis="both", which="both", direction="in", top=True, right=True)

    # Labels and formatting
    if not draw_ratio:
        ax.set_xlabel(nice_label, fontsize=XLABEL_FONTSIZE)
    ax.set_ylabel("Events", fontsize=YLABEL_FONTSIZE)

    # Apply axis ranges (shared x-axis propagates to the ratio panel)
    if x_range is not None:
        ax.set_xlim(x_range)
    if y_range is not None:
        ax.set_ylim(y_range)

    # Apply log scale if requested
    if log_y:
        ax.set_yscale("log")

    # Legend
    ax.legend(
        loc=LEGEND_LOC,
        fontsize=LEGEND_FONTSIZE,
    )

    # Grid and tick layering
    ax.grid(False)
    ax.set_axisbelow(False)
    ax.tick_params(axis="both", which="both", direction="in", top=True, right=True)

    has_data = len(data_processes) > 0
    base_fontsize = plt.rcParams["font.size"]
    hep.cms.label(
        CMS_STYLE if has_data else "",
        ax=ax,
        data=has_data,
        lumi=get_lumi_fb(year),
        lumi_format="{0:.0f}",
        year=year if year else "",
        com=13.6,
        fontsize=tuple(scale * base_fontsize for scale in CMS_LABEL_FONTSCALES),
    )

    # Layout: tight_layout would reset the gridspec spacing between the panels
    if draw_ratio:
        fig.align_ylabels((ax, rax))
    else:
        plt.tight_layout()

    # Save plots
    output_dir.mkdir(parents=True, exist_ok=True)

    # Add _log suffix if log scale is used
    log_suffix = "_log" if log_y else ""
    png_path = output_dir / f"{histogram_name}{log_suffix}.png"
    pdf_path = output_dir / f"{histogram_name}{log_suffix}.pdf"

    fig.savefig(png_path, dpi=DPI, format="png", bbox_inches="tight")
    fig.savefig(pdf_path, format="pdf", bbox_inches="tight")

    plt.close(fig)

    print(f"  Saved: {png_path}")
    print(f"  Saved: {pdf_path}")


def plot_all_histograms(
    histogram_data: Dict[str, Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]]],
    output_dir: Optional[Path] = None,
    year: Optional[str] = None,
) -> None:
    """
    Plot all configured histograms with their specified options.

    Args:
        histogram_data: Output from utils.read_histograms_from_files()
        output_dir: Directory to save plots (default: OUTPUT_PLOTS_DIR)
        year: Optional year label
    """
    if output_dir is None:
        output_dir = OUTPUT_PLOTS_DIR

    print(f"\nPlotting histograms to {output_dir}")
    print("=" * 80)

    for hist_name, hist_config in HISTOGRAMS_TO_PLOT.items():
        try:
            # Extract all configuration options with defaults
            nice_label = hist_config.get("label", hist_name)
            blind_data = hist_config.get("blind", False)
            log_y = hist_config.get("log_y", False)
            x_range = hist_config.get("x_range")
            y_range = hist_config.get("y_range")
            y_log_range = hist_config.get("y_log_range")
            ratio_panel = hist_config.get("ratio", RATIO_PANEL)

            # Extract data for this histogram from all processes
            hist_data = {}
            for process in histogram_data:
                if hist_name in histogram_data[process]:
                    hist_data[process] = histogram_data[process][hist_name]

            if not hist_data:
                print(f"  WARNING: No data found for histogram '{hist_name}'")
                continue

            print(f"\nPlotting {hist_name}...")

            # Plot linear version (always)
            plot_histogram(
                histogram_name=hist_name,
                nice_label=nice_label,
                histogram_data=hist_data,
                output_dir=output_dir,
                year=year,
                blind_data=blind_data,
                log_y=False,
                x_range=x_range,
                y_range=y_range,
                ratio_panel=ratio_panel,
            )

            # Plot log version if requested
            if log_y:
                print(f"  Also plotting {hist_name} with log y-scale...")
                plot_histogram(
                    histogram_name=hist_name,
                    nice_label=nice_label,
                    histogram_data=hist_data,
                    output_dir=output_dir,
                    year=year,
                    blind_data=blind_data,
                    log_y=True,
                    x_range=x_range,
                    y_range=y_log_range,
                    ratio_panel=ratio_panel,
                )

        except Exception as e:
            print(f"  ERROR plotting {hist_name}: {e}")
            raise

    print("\n" + "=" * 80)
    print(f"All plots saved to {output_dir}")
