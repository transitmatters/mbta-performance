import logging
from datetime import date, timedelta

import pandas as pd

from ... import parallel
from ..bus_ingest import RTE_DIR_STOP, fetch_bus_pq_file_from_remote, ingest_bus_pq_file, upload_bus_to_s3

logger = logging.getLogger(__name__)

_parallel_upload = parallel.make_parallel(upload_bus_to_s3)

EARLIEST_BUS_LAMP_DATA = date(2020, 1, 8)


def backfill_all_bus_dates(start_date: date = EARLIEST_BUS_LAMP_DATA):
    """Backfill all dates with bus LAMP data, most recent to oldest."""

    if start_date < EARLIEST_BUS_LAMP_DATA:
        raise ValueError(f"start_date {start_date} is before earliest available data {EARLIEST_BUS_LAMP_DATA}")

    end_date = date.today() - timedelta(days=1)
    dates = pd.date_range(start_date, end_date, freq="d")
    num_days = len(dates)
    estimated_cost = num_days / 30

    confirmation = input(
        f"This will backfill {num_days} days of bus LAMP data ({start_date} to {end_date}). "
        f"Estimated cost: ~${estimated_cost:.2f} (based on ~$1 per 30 days). "
        "This will take hours. Are you sure you want to proceed? (yes/no): "
    )
    if confirmation.lower() not in ("yes", "y"):
        print("You must enter 'yes' to proceed. Exiting.")
        exit(1)

    for backfill_timestamp in dates[::-1]:
        date_to_backfill = backfill_timestamp.date()
        try:
            pq_df = fetch_bus_pq_file_from_remote(date_to_backfill)
        except ValueError as e:
            logger.warning(f"Failed to fetch {date_to_backfill}: {e}")
            continue

        logger.info(f"Processing {date_to_backfill}")
        processed = ingest_bus_pq_file(pq_df, date_to_backfill)

        group_event_groups = processed.groupby(RTE_DIR_STOP)
        logger.info(f"Uploading events for {len(group_event_groups)} route-direction-stop groups to S3")
        _parallel_upload(group_event_groups, date_to_backfill)
        logger.info(f"Finished {date_to_backfill}")


if __name__ == "__main__":
    import argparse
    import os
    from datetime import datetime

    parser = argparse.ArgumentParser(description="Backfill bus LAMP data from a start date up to yesterday.")
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=EARLIEST_BUS_LAMP_DATA,
        help=f"Start date in YYYY-MM-DD format (default: {EARLIEST_BUS_LAMP_DATA}).",
    )
    args = parser.parse_args()

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    backfill_all_bus_dates(start_date=args.start_date)
