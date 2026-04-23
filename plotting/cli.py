#!/usr/bin/env python3
"""
CLI for plotting template histograms.

Examples:
    python3 plotting/cli.py --year 2024 --input-dir output/templates/merged
    python3 plotting/cli.py --year 2024 --input-dir output/templates/merged --output-dir output/plots/2024
    python3 plotting/cli.py --year 2024 --input-dir output/templates/merged --processes QCD TT data
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from plotting.config import HISTOGRAMS_TO_PLOT, PROCESSES, OUTPUT_PLOTS_DIR, DEFAULT_TEMPLATE_INPUT_DIR
from plotting.utils import read_histograms_from_files
from plotting.template_plotter import plot_all_histograms


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Plot template histograms from ROOT files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 plotting/cli.py --year 2024 --input-dir output/templates/merged
  python3 plotting/cli.py --year 2024 --input-dir output/templates/merged --output-dir custom/plots
  python3 plotting/cli.py --year 2024 --input-dir output/templates/merged --processes QCD TT data
        """,
    )
    parser.add_argument(
        "--year",
        required=True,
        help="Data year for the template files",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_TEMPLATE_INPUT_DIR,
        help=f"Directory containing merged template ROOT files (default: {DEFAULT_TEMPLATE_INPUT_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for plots (default: output/plots)",
    )
    parser.add_argument(
        "--processes",
        nargs="+",
        default=None,
        help="Specific processes to plot (default: all found in input directory)",
    )
    parser.add_argument(
        "--pattern",
        default="templates_*.root",
        help="Glob pattern for template files (default: templates_*.root)",
    )
    return parser.parse_args()


def discover_template_files(
    input_dir: Path,
    pattern: str,
    requested_processes: Optional[List[str]] = None,
) -> Dict[str, str]:
    """
    Build template file mapping from config PROCESSES.

    Args:
        input_dir: Directory containing template files
        pattern: Glob pattern to match files (unused, kept for compatibility)
        requested_processes: If provided, only return these processes

    Returns:
        Dict mapping process names to file paths

    Raises:
        FileNotFoundError: If input directory doesn't exist or files not found
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    file_dict = {}
    missing_files = []

    for process_key, process_config in PROCESSES.items():
        if requested_processes and process_key not in requested_processes:
            continue

        file_path = input_dir / process_config["file"]
        if not file_path.exists():
            missing_files.append(f"{process_key}: {file_path}")
        else:
            file_dict[process_key] = str(file_path)

    if missing_files:
        error_msg = f"Missing template files:\n  " + "\n  ".join(missing_files)
        raise FileNotFoundError(error_msg)

    if requested_processes:
        not_found = set(requested_processes) - set(file_dict.keys())
        if not_found:
            raise ValueError(f"Requested processes not found in config: {', '.join(not_found)}")

    return file_dict


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()

    print("=" * 80)
    print("Template Histogram Plotter")
    print("=" * 80)
    print(f"Year:         {args.year}")
    print(f"Input dir:    {args.input_dir}")
    print(f"Output dir:   {args.output_dir or OUTPUT_PLOTS_DIR}")
    print(f"File pattern: {args.pattern}")
    if args.processes:
        print(f"Processes:    {', '.join(args.processes)}")
    print("=" * 80 + "\n")

    try:
        # Discover template files
        print(f"Searching for template files in {args.input_dir}...")
        file_dict = discover_template_files(
            input_dir=args.input_dir,
            pattern=args.pattern,
            requested_processes=args.processes,
        )
        print(f"Found {len(file_dict)} process files:")
        for process in sorted(file_dict.keys()):
            print(f"  {process}: {file_dict[process]}")

        # Read histograms
        print(f"\nReading histograms from ROOT files...")
        histogram_names = list(HISTOGRAMS_TO_PLOT.keys())
        
        # Extract rebin factors from config
        rebin_factors = {
            hist_name: hist_config.get("rebin", 1)
            for hist_name, hist_config in HISTOGRAMS_TO_PLOT.items()
        }
        
        histogram_data = read_histograms_from_files(
            file_dict=file_dict,
            histogram_names=histogram_names,
            rebin_factors=rebin_factors,
        )
        print(f"Successfully read {len(histogram_data)} processes")

        # Plot
        output_dir = args.output_dir if args.output_dir else OUTPUT_PLOTS_DIR
        plot_all_histograms(
            histogram_data=histogram_data,
            output_dir=output_dir,
            year=args.year,
        )

        return 0

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
