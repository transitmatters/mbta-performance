"""Build weekly and monthly bus speed trend tiles.

Same per-segment aggregation as ingest.py, but rolled up across every service date in a
calendar week or month rather than one date at a time -- so a slow-changing pattern (a
detour that lasts a season, a route that's consistently faster off-peak) shows up as one
tile layer instead of requiring a rider to flip through individual days.

Percentiles are recomputed from every underlying traversal in the period, not averaged from
the daily p50/p90 columns already published by ingest.py -- a percentile of percentiles is
not the same number as the true percentile, so this re-runs `build_traversals_for_date` for
every date in the period and aggregates once across all of them. That costs one re-ingest per
day in the period (~25s/day per the bus README), so a week is a few minutes and a month
roughly half an hour; there is no dependency on ingest.py having already run for those dates.

Weeks and months are numbered sequentially from 1 rather than keyed by calendar date -- see
periods.py.

Every date also gets a `day_type` (business_day or weekend_or_holiday, see day_type.py) that
joins segment and time band in the aggregation key, so a week or month's output carries both
tracks side by side rather than blending a Tuesday and a Saturday into one number. It reaches
the map the same way time_band already does: as a plain tile property (pmtiles.TILE_PROPERTIES)
for the frontend to filter on, not a separate file per day type.
"""

import logging
from datetime import date, timedelta

import pandas as pd

from ...date import get_current_service_date
from .constants import S3_BUCKET
from .day_type import day_type_for
from .geoparquet import write_geoparquet
from .ingest import build_traversals_for_date
from .periods import dates_in_range, month_range, week_range
from .s3_writer import (
    upload_monthly_pmtiles,
    upload_monthly_speed_segments,
    upload_weekly_pmtiles,
    upload_weekly_speed_segments,
)
from .segments import SEGMENT_KEY, aggregate_segments, select_segment_geometry

logger = logging.getLogger(__name__)


def _build_period_result(service_dates: list[date]) -> pd.DataFrame:
    """Aggregate traversals from every given service date into one (segment, day type, time
    band) table.

    Dates with no usable data (a gap in LAMP's export, an unparseable feed) are logged and
    skipped rather than failing the whole period, matching how backfill.py treats a bad date.
    """
    all_traversals = []
    all_segments = []
    for service_date in service_dates:
        try:
            traversals, segments = build_traversals_for_date(service_date)
        except ValueError:
            logger.exception(f"Skipping {service_date} in period rollup")
            continue
        traversals["day_type"] = day_type_for(service_date)
        all_traversals.append(traversals)
        all_segments.append(segments)

    if not all_traversals:
        raise ValueError(f"No usable data for any date in {service_dates[0]}..{service_dates[-1]}")

    traversals = pd.concat(all_traversals, ignore_index=True)
    segments = pd.concat(all_segments, ignore_index=True).drop_duplicates(
        ["route_pattern_id", "from_stop_id", "to_stop_id"]
    )

    # No service_date in the group key, but day_type is: every traversal in the period
    # contributes to one percentile per (segment, day type, time band), so a business day and
    # a weekend/holiday in the same week never blend into one number.
    aggregated = aggregate_segments(traversals, extra_group_columns=("day_type",))
    chosen_geometry = select_segment_geometry(traversals, segments)

    result = aggregated.merge(chosen_geometry, on=SEGMENT_KEY, how="inner")
    logger.info(
        f"{len(result)} segment-band rows carry geometry across {len(service_dates)} dates "
        f"(from {len(aggregated)} aggregated rows)"
    )
    return result


def generate_weekly_speed_segments(
    week: int,
    output_path: str | None = None,
    upload: bool = False,
    write_pmtiles: bool = False,
) -> pd.DataFrame:
    """Build the aggregated speed-segment table across every service date in calendar week `week`.

    `week` is 1-indexed from the calendar week (Mon-Sun) containing EARLIEST_LAMP_BUS_DATA --
    see periods.py. Dates in the week that fall outside LAMP's coverage, or haven't happened
    yet, are skipped rather than erroring, so this can be run for the current in-progress
    week to get a partial-week trend.
    """
    start, end = week_range(week)
    service_dates = dates_in_range(start, end, not_after=get_current_service_date() - timedelta(days=1))
    if not service_dates:
        raise ValueError(f"Week {week} ({start}..{end}) has no service dates with available LAMP bus data yet")

    logger.info(f"Generating bus speed segments for week {week} ({service_dates[0]}..{service_dates[-1]})")
    result = _build_period_result(service_dates)

    if output_path:
        write_geoparquet(result, output_path)
    if upload:
        upload_weekly_speed_segments(result, week)
    if write_pmtiles:
        upload_weekly_pmtiles(result, week)
    return result


def generate_monthly_speed_segments(
    month: int,
    output_path: str | None = None,
    upload: bool = False,
    write_pmtiles: bool = False,
) -> pd.DataFrame:
    """Build the aggregated speed-segment table across every service date in calendar month `month`.

    `month` is 1-indexed from the calendar month containing EARLIEST_LAMP_BUS_DATA -- see
    periods.py. Dates in the month that fall outside LAMP's coverage, or haven't happened
    yet, are skipped rather than erroring.
    """
    start, end = month_range(month)
    service_dates = dates_in_range(start, end, not_after=get_current_service_date() - timedelta(days=1))
    if not service_dates:
        raise ValueError(f"Month {month} ({start}..{end}) has no service dates with available LAMP bus data yet")

    logger.info(f"Generating bus speed segments for month {month} ({service_dates[0]}..{service_dates[-1]})")
    result = _build_period_result(service_dates)

    if output_path:
        write_geoparquet(result, output_path)
    if upload:
        upload_monthly_speed_segments(result, month)
    if write_pmtiles:
        upload_monthly_pmtiles(result, month)
    return result


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Generate weekly or monthly LAMP bus speed trend tiles.")
    period = parser.add_mutually_exclusive_group(required=True)
    period.add_argument("--week", type=int, help="Week number, 1-indexed from the week of EARLIEST_LAMP_BUS_DATA")
    period.add_argument("--month", type=int, help="Month number, 1-indexed from the month of EARLIEST_LAMP_BUS_DATA")
    parser.add_argument("--out", help="Output GeoParquet path")
    parser.add_argument("--upload", action="store_true", help=f"Also publish to s3://{S3_BUCKET}")
    parser.add_argument(
        "--write-pmtiles", action="store_true", help="Also build and publish a PMTiles tileset (requires tippecanoe)"
    )
    arguments = parser.parse_args()

    if arguments.week is not None:
        generate_weekly_speed_segments(
            arguments.week, arguments.out, upload=arguments.upload, write_pmtiles=arguments.write_pmtiles
        )
    else:
        generate_monthly_speed_segments(
            arguments.month, arguments.out, upload=arguments.upload, write_pmtiles=arguments.write_pmtiles
        )
