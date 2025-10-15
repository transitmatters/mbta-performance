import argparse
from datetime import datetime
from ..process import process_ferry
from ..arcgis import download_latest_ferry_data, ferry_update_cache


def backfill_ferry_data(start_date=None, end_date=None):
    print("Processing Ferry Data")
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
    parser = argparse.ArgumentParser(
        description="Process ferry data with optional date range filtering"
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date for processing (YYYY-MM-DD format). If not specified, all dates are processed."
    )
    parser.add_argument(
        "--end-date", 
        type=parse_date,
        help="End date for processing (YYYY-MM-DD format). If not specified, all dates are processed."
    )
    
    args = parser.parse_args()
    
    # Validate date range
    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("Start date must be before or equal to end date")
    
    backfill_ferry_data(start_date=args.start_date, end_date=args.end_date)
