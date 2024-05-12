import pandas as pd
from ..ingest import fetch_pq_file_from_remote, ingest_pq_file, upload_to_s3
from ... import parallel
from datetime import datetime, timedelta


_parallel_upload = parallel.make_parallel(upload_to_s3)


def backfill_all_in_index():
    """Backfill all the dates in the LAMP index."""

    # all dates that LAMP has data for, starting from 2019-09-15
    dates = pd.date_range(datetime(2019, 9, 15).date(), datetime.today().date() - timedelta(days=1), freq="d")

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
    backfill_all_in_index()
