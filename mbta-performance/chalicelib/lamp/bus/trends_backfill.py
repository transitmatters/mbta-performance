"""Backfill weekly and monthly bus speed trend tiles for every period LAMP has data for.

Mirrors backfill.py's per-date loop and `--start-date`/`--end-date` interface: bus history is
only a few hundred days, so looping and skipping a failure rather than aborting the whole run
is simple and finishes in one sitting -- see backfill.py for why bus gets a real backfill
script here rather than the years-deep chalicelib/lamp/backfill approach rail uses.

Weeks and months are keyed by (year, week)/(year, month) rather than a single running
integer (see periods.py and trends.py), so backfilling walks calendar dates -- one Monday at
a time for weeks, one first-of-month at a time for months -- deriving each period's key from
that date, rather than iterating an integer range directly.
"""

import logging
from datetime import date, timedelta

from ...date import get_current_service_date
from .periods import EARLIEST_DATE, week_key_for
from .trends import generate_monthly_speed_segments, generate_weekly_speed_segments

logger = logging.getLogger(__name__)


def backfill_weeks(
    start_date: date | None = None, end_date: date | None = None, upload: bool = False, write_pmtiles: bool = False
) -> None:
    """Run the weekly rollup for every ISO week overlapping [start_date, end_date] (inclusive).

    `start_date`/`end_date` default to EARLIEST_LAMP_BUS_DATA and yesterday. `upload` and
    `write_pmtiles` are opt-in and forwarded as-is to `generate_weekly_speed_segments` for
    every week, matching how those flags behave for a single week. Any failure on a given
    week -- no usable data, a transient S3 error, tippecanoe choking on an unusually dense
    week -- is logged and skipped rather than aborting a run covering dozens of weeks.
    """
    start_date = start_date or EARLIEST_DATE
    end_date = end_date or (get_current_service_date() - timedelta(days=1))

    monday = start_date - timedelta(days=start_date.weekday())
    while monday <= end_date:
        year, week = week_key_for(monday)
        try:
            generate_weekly_speed_segments(year, week, upload=upload, write_pmtiles=write_pmtiles)
            logger.info(f"Loaded {year}-W{week:02d}")
        except Exception:
            logger.exception(f"Skipping {year}-W{week:02d}")
        monday += timedelta(days=7)


def backfill_months(
    start_date: date | None = None, end_date: date | None = None, upload: bool = False, write_pmtiles: bool = False
) -> None:
    """Run the monthly rollup for every calendar month overlapping [start_date, end_date].

    Same defaults, opt-in flags, and per-period failure handling as `backfill_weeks`.
    """
    start_date = start_date or EARLIEST_DATE
    end_date = end_date or (get_current_service_date() - timedelta(days=1))

    year, month = start_date.year, start_date.month
    while (year, month) <= (end_date.year, end_date.month):
        try:
            generate_monthly_speed_segments(year, month, upload=upload, write_pmtiles=write_pmtiles)
            logger.info(f"Loaded {year}-{month:02d}")
        except Exception:
            logger.exception(f"Skipping {year}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    yesterday = get_current_service_date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Backfill weekly and/or monthly LAMP bus speed trend tiles.")
    parser.add_argument("--weeks", action="store_true", help="Backfill weekly trend tiles")
    parser.add_argument("--months", action="store_true", help="Backfill monthly trend tiles")
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=EARLIEST_DATE,
        help=f"Service date (YYYY-MM-DD), default {EARLIEST_DATE.isoformat()} (earliest LAMP bus data)",
    )
    parser.add_argument("--end-date", type=date.fromisoformat, default=yesterday, help="Service date, default yesterday")
    parser.add_argument("--upload", action="store_true", help="Also publish GeoParquet to S3 for each period")
    parser.add_argument(
        "--write-pmtiles",
        action="store_true",
        help="Also build and publish PMTiles for each period (requires tippecanoe)",
    )
    arguments = parser.parse_args()

    if not arguments.weeks and not arguments.months:
        parser.error("Specify --weeks and/or --months")

    if arguments.weeks:
        backfill_weeks(
            arguments.start_date, arguments.end_date, upload=arguments.upload, write_pmtiles=arguments.write_pmtiles
        )
    if arguments.months:
        backfill_months(
            arguments.start_date, arguments.end_date, upload=arguments.upload, write_pmtiles=arguments.write_pmtiles
        )
