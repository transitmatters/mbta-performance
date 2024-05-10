import pandas as pd
from ..ingest import LAMP_INDEX_URL, fetch_pq_file_from_remote, ingest_pq_file, upload_to_s3
from ... import parallel


_parallel_upload = parallel.make_parallel(upload_to_s3)


def backfill_all_in_index():
    """Backfill all the dates in the LAMP index."""

    # Load the index
    index = pd.read_csv(LAMP_INDEX_URL)
    # Get the dates in the index
    dates = pd.to_datetime(index["service_date"]).dt.date
    # Backfill each date
    for date_to_backfill in dates.reindex(index=dates.index[::-1]):
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
