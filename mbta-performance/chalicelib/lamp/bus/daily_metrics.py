"""Aggregate per-traversal bus speed data to one row per (route, service_date) in DynamoDB.

This is a coarser, single-number-per-day companion to the per-segment GeoParquet built by
segments.py -- for the same kind of "how fast is this route" line chart the dashboard already
draws for rail from the DeliveredTripMetrics family of tables. Both directions are combined
into one row per route per day, matching how that table reports rail lines.

Unlike rail, which multiplies a fixed round-trip track length by an observed trip count (its
"routes" are two-terminus lines, so a nominal length is all it has), miles_covered and
total_time here are summed directly from the distance and time LAMP actually recorded on each
segment traversal.

Each row also carries the service date's `day_type` (day_type.py, the same MBTA-calendar split
the weekly/monthly trend tiles use) and a `time_bands` map of the same summable totals per
TIME_BANDS band, so the dashboard can filter its speed chart by time of day and business
day/weekend and still roll days up by summing. Per-band medians/means are deliberately left
out: they can't be combined across days.
"""

import logging
from decimal import Decimal

import pandas as pd

from ... import dynamo
from .constants import DYNAMO_TABLE_NAME, METERS_PER_MILE
from .segments import assign_time_band

logger = logging.getLogger(__name__)


# Totals that stay meaningful when several days' rows are added together, unlike a median or
# mean: the only fields in the per-band breakdown, and what the dashboard sums for weekly and
# monthly views.
SUMMABLE_AGGREGATION = {
    "count": ("trip_id", "nunique"),
    "n_traversals": ("total_time_seconds", "size"),
    "n_interpolated": ("is_interpolated", "sum"),
    "miles_covered": ("segment_length_m", lambda s: s.sum() / METERS_PER_MILE),
    "total_time": ("total_time_seconds", "sum"),
}

ROUTE_DAY_KEY = ["route_id", "service_date", "day_type"]


def build_daily_route_metrics(traversals: pd.DataFrame) -> pd.DataFrame:
    """Aggregate segment traversals to one row per (route, service_date).

    `traversals` is the per-(trip, segment) frame from `segments.build_traversals`, before
    it's broken down further by direction, stop pair, or time band, plus a `day_type` column
    (day_type.day_type_for of its service date).

    The top-level totals cover every traversal. `time_bands` holds the same summable totals
    per time band the traversal departed in, for bands with at least one traversal. A band's
    `count` is the trips with a traversal departing in that band, so a trip that crosses a
    band boundary counts in both, and the band counts sum to more than the day's `count`.
    """
    daily = (
        traversals.groupby(ROUTE_DAY_KEY, sort=False)
        .agg(
            **SUMMABLE_AGGREGATION,
            median_speed_mph=("speed_mph", "median"),
            mean_speed_mph=("speed_mph", "mean"),
        )
        .reset_index()
    )
    time_bands = _time_band_totals(traversals)
    daily["time_bands"] = [time_bands.get(tuple(key), {}) for key in daily[ROUTE_DAY_KEY].itertuples(index=False)]
    return daily.rename(columns={"route_id": "route", "service_date": "date"})


def _time_band_totals(traversals: pd.DataFrame) -> dict[tuple, dict[str, dict]]:
    """{(route_id, service_date, day_type): {time_band: summable totals}}.

    Uses the same band assignment as the segment map (segments.assign_time_band), so a
    traversal departing outside every band is in the top-level totals but in no band.
    """
    banded = traversals.assign(time_band=assign_time_band(traversals.depart_seconds)).dropna(subset=["time_band"])
    per_band = banded.groupby(ROUTE_DAY_KEY + ["time_band"], sort=False).agg(**SUMMABLE_AGGREGATION).reset_index()

    totals = {}
    for row in per_band.to_dict(orient="records"):
        route_day = tuple(row[column] for column in ROUTE_DAY_KEY)
        totals.setdefault(route_day, {})[row["time_band"]] = {field: row[field] for field in SUMMABLE_AGGREGATION}
    return totals


def _date_str(value) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _summable_item_fields(totals: dict) -> dict:
    """Summable totals as DynamoDB Decimals, rounded the same at the top level and per band."""
    return {
        "count": Decimal(int(totals["count"])),
        "n_traversals": Decimal(int(totals["n_traversals"])),
        "n_interpolated": Decimal(int(totals["n_interpolated"])),
        "miles_covered": Decimal(str(round(totals["miles_covered"], 3))),
        "total_time": Decimal(str(round(totals["total_time"], 1))),
    }


def prepare_dynamo_items(daily: pd.DataFrame) -> list[dict]:
    """Convert a daily-metrics frame into DynamoDB-ready items (Decimal values, ISO dates)."""
    items = []
    for row in daily.to_dict(orient="records"):
        items.append(
            {
                "route": str(row["route"]),
                "date": _date_str(row["date"]),
                **_summable_item_fields(row),
                "median_speed_mph": Decimal(str(round(row["median_speed_mph"], 2))),
                "mean_speed_mph": Decimal(str(round(row["mean_speed_mph"], 2))),
                "day_type": str(row["day_type"]),
                # SET as one whole map, so a band that drops out on a re-run doesn't linger.
                "time_bands": {str(band): _summable_item_fields(totals) for band, totals in row["time_bands"].items()},
            }
        )
    return items


def write_daily_route_metrics(traversals: pd.DataFrame) -> int:
    """Aggregate a day of traversals to per-route metrics and upsert them into DynamoDB.

    `traversals` must carry a `day_type` column -- see `build_daily_route_metrics`.

    Only these speed fields (including `day_type` and the whole `time_bands` map) are SET on
    each row; other fields on the same (route, date) row, such as the fleet stats
    data-ingestion writes, are left alone.

    Returns the number of route rows written.
    """
    daily = build_daily_route_metrics(traversals)
    items = prepare_dynamo_items(daily)
    logger.info(f"Writing {len(items)} daily route speed rows to {DYNAMO_TABLE_NAME}")
    dynamo.dynamo_update_items(items, DYNAMO_TABLE_NAME)
    return len(items)
