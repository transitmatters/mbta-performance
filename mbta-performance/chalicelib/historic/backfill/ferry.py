import argparse
from datetime import datetime

import requests

from ..arcgis import FERRY_CSV_PATH, download_latest_ferry_data, ferry_update_cache
from ..constants import FERRY_ARCGIS_ID
from ..process import process_ferry, whole_months
from ..source_archive import InvalidSourceFile, archive_source_file, restore_source_file, validate_source_file
from ..upload import upload_monthly_outputs


def fetch_ferry_csv() -> str:
    """Download the ferry CSV and archive it in S3. If ArcGIS can't serve a valid one, use the archived copy instead."""
    try:
        data_edited = ferry_update_cache()
        csv_file = download_latest_ferry_data()
        validate_source_file(csv_file)
    except (requests.RequestException, InvalidSourceFile) as e:
        print(f"Failed to fetch the ferry data from ArcGIS: {e}")
        try:
            return restore_source_file(FERRY_CSV_PATH, "ferry")
        except FileNotFoundError as missing:
            raise ValueError(f"Failed to fetch the ferry data from ArcGIS, and {missing}") from e
    archive_source_file(csv_file, "ferry", item_id=FERRY_ARCGIS_ID, source_date=data_edited)
    return csv_file


def backfill_ferry_data(start_date=None, end_date=None):
    print("Processing Ferry Data")
    # Each output file is a whole month, so process whole months
    start_date, end_date = whole_months(start_date, end_date)
    if start_date:
        print(f"Start date: {start_date}")
    if end_date:
        print(f"End date: {end_date}")

    csv_file = fetch_ferry_csv()

    process_ferry(csv_file, "data/output", start_date=start_date, end_date=end_date)
    print("Finished Processing Ferry Data")


def parse_date(date_string):
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Use YYYY-MM-DD format.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process ferry data with optional date range filtering")
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="First month to process (YYYY-MM-DD; the whole month is processed). Default: all dates.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="Last month to process (YYYY-MM-DD; the whole month is processed). Default: all dates.",
    )
    parser.add_argument("--upload", action="store_true", help="Upload changed outputs for the processed range to S3.")

    args = parser.parse_args()

    # Validate date range
    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("Start date must be before or equal to end date")

    backfill_ferry_data(start_date=args.start_date, end_date=args.end_date)

    if args.upload:
        upload_monthly_outputs(modes=("ferry",), start=args.start_date, end=args.end_date)
