from datetime import date, timedelta

import pandas as pd

from ... import parallel
from ..ingest import fetch_pq_file_from_remote, ingest_pq_file, upload_to_s3

_parallel_upload = parallel.make_parallel(upload_to_s3)

EARLIEST_LAMP_DATA = date(2019, 9, 15)


def backfill_all_in_index():
    """Backfill all the dates in the LAMP index."""

    confirmation = input(
        "This will backfill all dates in the LAMP index from today going back to September 15th 2019. This will cost about $1 per 30 days backfilled and will take hours. Are you sure you want to proceed? (yes/no): "
    )
    if confirmation.lower() != "yes" and confirmation.lower() != "y":
        print("You must enter 'yes' to proceed. Exiting.")
        exit(1)

    # all dates that LAMP has data for, starting from 2019-09-15
    dates = pd.date_range(EARLIEST_LAMP_DATA, date.today() - timedelta(days=1), freq="d")

    # Backfill each date, most recent to oldest
    for backfill_timestamp in dates[::-1]:
        date_to_backfill = backfill_timestamp.date()
        try:
            pq_df = fetch_pq_file_from_remote(date_to_backfill)
        except ValueError as e:
            # If we can't fetch the file, we can't process it
            print(f"Failed to fetch {date_to_backfill}: {e}")
        print(f"Processing {date_to_backfill}")
        processed_daily_events = ingest_pq_file(pq_df, date_to_backfill)

        # split daily events by stop_id and parallel upload to s3
        stop_event_groups = processed_daily_events.groupby("stop_id")
        _parallel_upload(stop_event_groups, date_to_backfill)


if __name__ == "__main__":
    import logging
    import os

    # Configure logging for local execution
    # Set LOG_LEVEL=DEBUG in environment to enable debug logging
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    backfill_all_in_index()
