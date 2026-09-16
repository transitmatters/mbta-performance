"""Load a handful of days of bus speed metrics into DynamoDB.

Unlike the rail LAMP backfill in chalicelib/lamp/backfill (years of history, run once),
LAMP bus data only goes back to EARLIEST_LAMP_BUS_DATA, so there isn't much to backfill.
This is for seeding a small sample -- e.g. to check DeliveredTripMetricsBus looks right in
the AWS console, or to give the dashboard something real to build against -- not a full
historical run.
"""

import logging
from datetime import date, timedelta

from ... import dynamo
from ...date import get_current_service_date
from .constants import DYNAMO_TABLE_NAME
from .ingest import generate_speed_segments

logger = logging.getLogger(__name__)


def load_sample_days(start_date: date, end_date: date, upload: bool = False) -> None:
    """Run the pipeline for each service date in [start_date, end_date] (inclusive).

    Writes daily per-route metrics to DynamoDB for every date, continuing past a single bad
    day (e.g. a date with no LAMP events) rather than aborting the whole range.
    """
    dynamo.create_table_if_not_exists(DYNAMO_TABLE_NAME, hash_key="route", range_key="date")

    current = start_date
    while current <= end_date:
        try:
            generate_speed_segments(current, upload=upload, write_to_dynamo=True)
            logger.info(f"Loaded {current}")
        except ValueError as e:
            logger.warning(f"Skipping {current}: {e}")
        current += timedelta(days=1)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    yesterday = get_current_service_date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Load a few days of bus speed metrics into DynamoDB.")
    parser.add_argument("--start-date", type=date.fromisoformat, default=yesterday - timedelta(days=4))
    parser.add_argument("--end-date", type=date.fromisoformat, default=yesterday)
    parser.add_argument("--upload", action="store_true", help="Also publish GeoParquet to S3 for each date")
    arguments = parser.parse_args()

    load_sample_days(arguments.start_date, arguments.end_date, upload=arguments.upload)
