import argparse
from datetime import datetime
from ..process import process_ferry, whole_months
from ..arcgis import download_latest_ferry_data, ferry_update_cache
from ..upload import upload_monthly_outputs


def backfill_ferry_data(start_date=None, end_date=None):
    print("Processing Ferry Data")
    # Each output file is a whole month, so process whole months
    start_date, end_date = whole_months(start_date, end_date)
    if start_date:
        print(f"Start date: {start_date}")
    if end_date:
        print(f"End date: {end_date}")

    ferry_update_cache()
    # download the data
    csv_file = download_latest_ferry_data()

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
