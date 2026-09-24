import argparse
from datetime import date

from ..constants import ARCGIS_IDS, HISTORIC_COLUMNS_LAMP, HISTORIC_COLUMNS_PRE_LAMP
from ..download import download_historic_data, list_files_in_dir, prep_local_dir, unzip_historic_data
from ..process import process_events
from ..upload import upload_monthly_outputs
from .ferry import parse_date


def backfill_single_year(year: str, start_date: date = None, end_date: date = None):
    print(f"Backfilling year {year}")
    # download the data
    zip_file = download_historic_data(year)
    # unzip the data
    input_dir = unzip_historic_data(zip_file, f"data/input/{year}")
    # process the data
    for file in list_files_in_dir(input_dir):
        # in 2024 data moved to LAMP and the format changed
        if int(year) >= 2024:
            process_events(file, "data/output", columns=HISTORIC_COLUMNS_LAMP, start_date=start_date, end_date=end_date)
        else:
            process_events(
                file, "data/output", columns=HISTORIC_COLUMNS_PRE_LAMP, start_date=start_date, end_date=end_date
            )
    print(f"Finished backfilling year {year}")


def backfill_all_years():
    """Backfill all years of MBTA data we can"""

    prep_local_dir()

    for year in reversed(ARCGIS_IDS.keys()):
        backfill_single_year(year)


def backfill_date_range(start_date: date, end_date: date):
    """Backfill only data within [start_date, end_date], spanning whichever years are needed."""
    prep_local_dir()

    for year in range(start_date.year, end_date.year + 1):
        year_str = str(year)
        if year_str not in ARCGIS_IDS:
            print(f"Skipping year {year_str}: no ARCGIS_ID configured")
            continue
        backfill_single_year(year_str, start_date=start_date, end_date=end_date)


def backfill_years(years: list):
    """Backfill only the given years."""
    prep_local_dir()

    for year in years:
        backfill_single_year(str(year))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and process monthly rapid transit data. With no arguments, backfills every year."
    )
    parser.add_argument(
        "--year",
        action="append",
        choices=list(ARCGIS_IDS.keys()),
        help="Year to backfill (repeatable). The monthly update is usually just the current year.",
    )
    parser.add_argument("--start-date", type=parse_date, help="Only process service dates on/after this (YYYY-MM-DD).")
    parser.add_argument("--end-date", type=parse_date, help="Only process service dates on/before this (YYYY-MM-DD).")
    parser.add_argument("--upload", action="store_true", help="Upload changed outputs for the processed range to S3.")
    args = parser.parse_args()

    if args.year and (args.start_date or args.end_date):
        parser.error("Use either --year or --start-date/--end-date, not both")
    if bool(args.start_date) != bool(args.end_date):
        parser.error("--start-date and --end-date must be used together")
    if args.start_date and args.start_date > args.end_date:
        parser.error("Start date must be before or equal to end date")

    if args.year:
        backfill_years(args.year)
        upload_start, upload_end = date(int(min(args.year)), 1, 1), date(int(max(args.year)), 12, 31)
    elif args.start_date:
        backfill_date_range(args.start_date, args.end_date)
        upload_start, upload_end = args.start_date, args.end_date
    else:
        backfill_all_years()
        upload_start, upload_end = None, None

    if args.upload:
        upload_monthly_outputs(modes=("rapid",), start=upload_start, end=upload_end)
