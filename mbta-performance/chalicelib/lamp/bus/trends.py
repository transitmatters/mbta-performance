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

Weeks are keyed by ISO 8601 (year, week) and months by calendar (year, month) -- see
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
import pyarrow as pa

from ...date import get_current_service_date
from .constants import S3_BUCKET
from .day_type import day_type_for
from .geoparquet import write_geoparquet
from .ingest import build_traversals_for_date
from .leaderboard import build_leaderboard
from .periods import dates_in_range, month_key_for, month_range, recent_period_keys, week_key_for, week_range
from .s3_writer import (
    upload_monthly_leaderboard,
    upload_monthly_pmtiles,
    upload_monthly_speed_segments,
    upload_weekly_leaderboard,
    upload_weekly_pmtiles,
    upload_weekly_speed_segments,
)
from .segments import SEGMENT_KEY, aggregate_segments, select_segment_geometry
from .shared_segments import add_shared_segments

logger = logging.getLogger(__name__)

# No service_date in the group key, but day_type is: every traversal in the period contributes
# to one percentile per (segment, day type, time band), so a business day and a
# weekend/holiday in the same week never blend into one number.
PERIOD_GROUP_COLUMNS = ("day_type",)

# The scheduled jobs (`--latest-week`, `--latest-month`) build every period holding one of this
# many recent service dates. So a period is built on the morning after its last day, as before,
# and again on the two mornings after that. LAMP is sometimes late with yesterday's data, and
# those runs won't publish a period without its newest date (`require_newest_date`). One of the
# later mornings then fills in a finished period's last day without anyone rerunning the job.
# Each extra build is one week (~1.5 minutes) or one month (~5 minutes, ~8GB peak with tiles) on
# basement-dweller, plus 4 PUTs: about 45 more PUTs a month, a fraction of a cent. See the README.
LATEST_PERIOD_LOOKBACK_DAYS = 3


def _build_period_result(
    service_dates: list[date], require_newest_date: bool = False
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate traversals from every given service date into one (segment, day type, time
    band) table.

    Returns (that table, the traversals it was built from), the latter for
    shared_segments.add_shared_segments.

    Dates with no usable data (a gap in LAMP's export, an unparseable feed) are logged and
    skipped rather than failing the whole period, matching how backfill.py treats a bad date.
    The newest date is the exception when `require_newest_date` is set: then a ValueError is
    raised instead. A date LAMP hasn't published yet fails the same way, and for the scheduled
    jobs only the newest date can still be missing for that reason. Dates are read newest
    first, so that case fails in seconds without reading the rest of the period.

    pyarrow's ArrowInvalid is a ValueError too, but it means a file couldn't be parsed (one
    MBTA was midway through replacing, on 2026-11-10), not that the date has no data. So it
    fails the period like any other read error rather than being skipped.
    """
    newest = max(service_dates)
    built = {}
    skipped = []
    for service_date in sorted(service_dates, reverse=True):
        try:
            traversals, segments = build_traversals_for_date(service_date)
        except ValueError as error:
            if isinstance(error, pa.ArrowInvalid):
                raise
            if require_newest_date and service_date == newest:
                raise ValueError(
                    f"{service_date}, the newest date in {min(service_dates)}..{newest}, has no usable data "
                    "(LAMP may not have published it yet); not publishing the period without it"
                ) from error
            logger.exception(f"Skipping {service_date} in period rollup")
            skipped.append(service_date)
            continue
        traversals["day_type"] = day_type_for(service_date)
        built[service_date] = (traversals, segments)

    if not built:
        raise ValueError(f"No usable data for any date in {min(service_dates)}..{newest}")
    if skipped:
        logger.warning(
            f"{min(service_dates)}..{newest} is missing {len(skipped)} of {len(service_dates)} dates: "
            + ", ".join(str(skipped_date) for skipped_date in sorted(skipped))
        )

    # Concatenated in date order whatever order they were read in: drop_duplicates keeps the
    # first copy of each pattern's segment geometry.
    in_date_order = [built[service_date] for service_date in sorted(built)]
    traversals = pd.concat([traversals for traversals, _ in in_date_order], ignore_index=True)
    segments = pd.concat([segments for _, segments in in_date_order], ignore_index=True).drop_duplicates(
        ["route_pattern_id", "from_stop_id", "to_stop_id"]
    )

    aggregated = aggregate_segments(traversals, extra_group_columns=PERIOD_GROUP_COLUMNS)
    chosen_geometry = select_segment_geometry(traversals, segments)

    result = aggregated.merge(chosen_geometry, on=SEGMENT_KEY, how="inner")
    logger.info(
        f"{len(result)} segment-band rows carry geometry across {len(service_dates)} dates "
        f"(from {len(aggregated)} aggregated rows)"
    )
    return result, traversals


def generate_weekly_speed_segments(
    year: int,
    week: int,
    output_path: str | None = None,
    upload: bool = False,
    write_pmtiles: bool = False,
    write_leaderboard: bool = False,
    require_newest_date: bool = False,
) -> pd.DataFrame:
    """Build the aggregated speed-segment table across every service date in ISO week (year, week).

    `(year, week)` is the ISO 8601 week -- see periods.py, including the note that the week
    containing EARLIEST_LAMP_BUS_DATA (2025-12-24) is ISO year 2025, week 52. Dates in the
    week that fall outside LAMP's coverage, or haven't happened yet, are skipped rather than
    erroring, so this can be run for the current in-progress week to get a partial-week trend.

    `write_leaderboard` ranks segments slowest-first within each (day_type, time_band) slice
    (leaderboard.py) and publishes the result as JSON alongside the GeoParquet/PMTiles. Also
    opt-in and independent of `upload`/`write_pmtiles`.

    `require_newest_date` raises, before anything is published, if the week's newest date
    (yesterday, while the week is still in progress) has no usable data. Older dates without
    data are still skipped. See `_build_period_result`; the scheduled job sets it through
    `generate_latest_weeks`.
    """
    start, end = week_range(year, week)
    service_dates = dates_in_range(start, end, not_after=get_current_service_date() - timedelta(days=1))
    if not service_dates:
        raise ValueError(f"{year}-W{week:02d} ({start}..{end}) has no service dates with available LAMP bus data yet")

    logger.info(f"Generating bus speed segments for {year}-W{week:02d} ({service_dates[0]}..{service_dates[-1]})")
    result, traversals = _build_period_result(service_dates, require_newest_date=require_newest_date)

    if output_path:
        write_geoparquet(result, output_path)
    if upload:
        upload_weekly_speed_segments(result, year, week)
    if write_pmtiles:
        upload_weekly_pmtiles(add_shared_segments(result, traversals, PERIOD_GROUP_COLUMNS), year, week)
    if write_leaderboard:
        upload_weekly_leaderboard(build_leaderboard(result), year, week)
    return result


def generate_monthly_speed_segments(
    year: int,
    month: int,
    output_path: str | None = None,
    upload: bool = False,
    write_pmtiles: bool = False,
    write_leaderboard: bool = False,
    require_newest_date: bool = False,
) -> pd.DataFrame:
    """Build the aggregated speed-segment table across every service date in calendar (year, month).

    Dates in the month that fall outside LAMP's coverage, or haven't happened yet, are
    skipped rather than erroring. `write_leaderboard` and `require_newest_date` behave as in
    `generate_weekly_speed_segments`.
    """
    start, end = month_range(year, month)
    service_dates = dates_in_range(start, end, not_after=get_current_service_date() - timedelta(days=1))
    if not service_dates:
        raise ValueError(f"{year}-{month:02d} ({start}..{end}) has no service dates with available LAMP bus data yet")

    logger.info(f"Generating bus speed segments for {year}-{month:02d} ({service_dates[0]}..{service_dates[-1]})")
    result, traversals = _build_period_result(service_dates, require_newest_date=require_newest_date)

    if output_path:
        write_geoparquet(result, output_path)
    if upload:
        upload_monthly_speed_segments(result, year, month)
    if write_pmtiles:
        upload_monthly_pmtiles(add_shared_segments(result, traversals, PERIOD_GROUP_COLUMNS), year, month)
    if write_leaderboard:
        upload_monthly_leaderboard(build_leaderboard(result), year, month)
    return result


def generate_latest_weeks(upload: bool = False, write_pmtiles: bool = False, write_leaderboard: bool = False) -> None:
    """What the scheduled weekly job runs: every ISO week holding one of the last
    LATEST_PERIOD_LOOKBACK_DAYS service dates, newest first, each with `require_newest_date`.

    Every week is attempted even if one fails. Then this raises if any did, so a late LAMP
    morning fails this job along with the daily one.
    """
    _generate_latest_periods(
        week_key_for,
        generate_weekly_speed_segments,
        lambda year, week: f"{year}-W{week:02d}",
        upload=upload,
        write_pmtiles=write_pmtiles,
        write_leaderboard=write_leaderboard,
    )


def generate_latest_months(upload: bool = False, write_pmtiles: bool = False, write_leaderboard: bool = False) -> None:
    """`generate_latest_weeks` for calendar months: what the scheduled monthly job runs."""
    _generate_latest_periods(
        month_key_for,
        generate_monthly_speed_segments,
        lambda year, month: f"{year}-{month:02d}",
        upload=upload,
        write_pmtiles=write_pmtiles,
        write_leaderboard=write_leaderboard,
    )


def _generate_latest_periods(key_for, generate, label, **flags) -> None:
    yesterday = get_current_service_date() - timedelta(days=1)
    failed = []
    for year, number in recent_period_keys(key_for, yesterday, LATEST_PERIOD_LOOKBACK_DAYS):
        try:
            generate(year, number, require_newest_date=True, **flags)
        except Exception:
            logger.exception(f"Could not build {label(year, number)}")
            failed.append(label(year, number))
    if failed:
        raise RuntimeError(f"Could not build {', '.join(failed)}; see the errors above")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Generate weekly or monthly LAMP bus speed trend tiles.")
    parser.add_argument("--year", type=int, help="Calendar year for --week or --month")
    period = parser.add_mutually_exclusive_group(required=True)
    period.add_argument("--week", type=int, help="ISO 8601 week number within --year")
    period.add_argument("--month", type=int, help="Calendar month number within --year")
    # For the scheduled jobs: the period holding yesterday, which is still filling in, plus the
    # one before it for the first few mornings after it ends. See generate_latest_weeks.
    period.add_argument(
        "--latest-week",
        action="store_true",
        help=f"Each ISO week holding one of the last {LATEST_PERIOD_LOOKBACK_DAYS} service dates",
    )
    period.add_argument(
        "--latest-month",
        action="store_true",
        help=f"Each month holding one of the last {LATEST_PERIOD_LOOKBACK_DAYS} service dates",
    )
    parser.add_argument("--out", help="Output GeoParquet path")
    parser.add_argument("--upload", action="store_true", help=f"Also publish to s3://{S3_BUCKET}")
    parser.add_argument(
        "--write-pmtiles", action="store_true", help="Also build and publish a PMTiles tileset (requires tippecanoe)"
    )
    parser.add_argument(
        "--write-leaderboard", action="store_true", help="Also build and publish the slowest-segments leaderboard JSON"
    )
    arguments = parser.parse_args()
    flags = dict(
        upload=arguments.upload, write_pmtiles=arguments.write_pmtiles, write_leaderboard=arguments.write_leaderboard
    )

    if arguments.latest_week or arguments.latest_month:
        if arguments.out:
            parser.error("--out writes one period; use it with --week or --month")
        (generate_latest_weeks if arguments.latest_week else generate_latest_months)(**flags)
    elif arguments.year is None:
        parser.error("--year is required with --week or --month")
    elif arguments.week is not None:
        generate_weekly_speed_segments(arguments.year, arguments.week, arguments.out, **flags)
    else:
        generate_monthly_speed_segments(arguments.year, arguments.month, arguments.out, **flags)
