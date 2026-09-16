"""Backfill bus speed metrics for every service date LAMP has data for.

Unlike the rail LAMP backfill in chalicelib/lamp/backfill (years of history), bus only goes
back to EARLIEST_LAMP_BUS_DATA, so the whole history is a few hundred days rather than years
-- long enough to be worth a real script, short enough to run to completion in one sitting.
"""

import logging
from datetime import date, timedelta

from ... import dynamo
from ...date import get_current_service_date
from .constants import DYNAMO_TABLE_NAME, EARLIEST_LAMP_BUS_DATA
from .ingest import generate_speed_segments

logger = logging.getLogger(__name__)


def backfill_range(start_date: date, end_date: date, upload: bool = False, write_pmtiles: bool = False) -> None:
    """Run the pipeline for each service date in [start_date, end_date] (inclusive).

    Always writes daily per-route metrics to DynamoDB. `upload` and `write_pmtiles` are
    opt-in and forwarded as-is to `generate_speed_segments` for every date, matching how
    those flags behave for a single day. Any failure on a given date -- no LAMP events, a
    transient S3/DynamoDB error, tippecanoe choking on an unusually dense day -- is logged
    and skipped rather than aborting a run covering hundreds of dates.
    """
    dynamo.create_table_if_not_exists(DYNAMO_TABLE_NAME, hash_key="route", range_key="date")

    current = start_date
    while current <= end_date:
        try:
            generate_speed_segments(current, upload=upload, write_to_dynamo=True, write_pmtiles=write_pmtiles)
            logger.info(f"Loaded {current}")
        except Exception:
            logger.exception(f"Skipping {current}")
        current += timedelta(days=1)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    yesterday = get_current_service_date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Backfill bus speed metrics for a range of service dates.")
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date.fromisoformat(EARLIEST_LAMP_BUS_DATA),
        help=f"Service date (YYYY-MM-DD), default {EARLIEST_LAMP_BUS_DATA} (earliest LAMP bus data)",
    )
    parser.add_argument(
        "--end-date", type=date.fromisoformat, default=yesterday, help="Service date, default yesterday"
    )
    parser.add_argument("--upload", action="store_true", help="Also publish GeoParquet to S3 for each date")
    parser.add_argument(
        "--write-pmtiles",
        action="store_true",
        help="Also build and publish PMTiles for each date (requires tippecanoe)",
    )
    arguments = parser.parse_args()

    backfill_range(
        arguments.start_date, arguments.end_date, upload=arguments.upload, write_pmtiles=arguments.write_pmtiles
    )
