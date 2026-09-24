import argparse
import pathlib
from datetime import date

from ..constants import BUS_ARCGIS_IDS
from ..download import download_all_bus_data
from ..process import process_bus_events
from ..upload import upload_monthly_outputs


def backfill_bus_data(years: list = None, routes: list = None, output_dir: str = "data/output", nozip: bool = False):
    """
    Process all bus data files for specified years and routes.
    This replaces the bash script functionality.

    Args:
        years: List of years to process (default: all years in BUS_ARCGIS_IDS)
        routes: List of route IDs to process (default: common bus routes)
        output_dir: Output directory for processed data
        nozip: Whether to skip gzipping files
    """
    if years is None:
        years = [int(y) for y in BUS_ARCGIS_IDS.keys()]

    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    for year in years:
        year_dir = f"data/input/bus/{year}"
        if not pathlib.Path(year_dir).exists():
            print(f"Warning: No data found for year {year} in {year_dir}")
            continue

        print(f"Processing bus data for year {year}...")

        # Find all CSV files in the year directory
        csv_files = list(pathlib.Path(year_dir).glob("*.csv"))

        if not csv_files:
            print(f"Warning: No CSV files found in {year_dir}")
            continue

        for csv_file in csv_files:
            print(f"  Processing {csv_file.name}...")
            try:
                process_bus_events(str(csv_file), output_dir, routes, nozip)
            except Exception as e:
                print(f"    Error processing {csv_file.name}: {e}")
                continue

    print("Bus data processing completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and process monthly bus data. With no arguments, backfills every year."
    )
    parser.add_argument(
        "--year",
        action="append",
        choices=list(BUS_ARCGIS_IDS.keys()),
        help="Year to backfill (repeatable). The monthly update is usually just the current year.",
    )
    parser.add_argument("--upload", action="store_true", help="Upload changed outputs for the processed years to S3.")
    args = parser.parse_args()

    download_all_bus_data(args.year)
    backfill_bus_data([int(y) for y in args.year] if args.year else None)

    if args.upload:
        if args.year:
            upload_start, upload_end = date(int(min(args.year)), 1, 1), date(int(max(args.year)), 12, 31)
        else:
            upload_start, upload_end = None, None
        upload_monthly_outputs(modes=("bus",), start=upload_start, end=upload_end)
