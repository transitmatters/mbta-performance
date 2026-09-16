"""Aggregate per-traversal bus speed data to one row per (route, service_date) in DynamoDB.

This is a coarser, single-number-per-day companion to the per-segment GeoParquet built by
segments.py -- for the same kind of "how fast is this route" line chart the dashboard already
draws for rail from the DeliveredTripMetrics family of tables. Both directions are combined
into one row per route per day, matching how that table reports rail lines.

Unlike rail, which multiplies a fixed round-trip track length by an observed trip count (its
"routes" are two-terminus lines, so a nominal length is all it has), miles_covered and
total_time here are summed directly from the distance and time LAMP actually recorded on each
segment traversal.
"""

import logging
from decimal import Decimal

import pandas as pd

from ... import dynamo
from .constants import DYNAMO_TABLE_NAME, METERS_PER_MILE

logger = logging.getLogger(__name__)


def build_daily_route_metrics(traversals: pd.DataFrame) -> pd.DataFrame:
    """Aggregate segment traversals to one row per (route, service_date).

    `traversals` is the per-(trip, segment) frame from `segments.build_traversals`, before
    it's broken down further by direction, stop pair, or time band.
    """
    grouped = traversals.groupby(["route_id", "service_date"], sort=False)
    daily = grouped.agg(
        count=("trip_id", "nunique"),
        n_traversals=("total_time_seconds", "size"),
        n_interpolated=("is_interpolated", "sum"),
        miles_covered=("segment_length_m", lambda s: s.sum() / METERS_PER_MILE),
        total_time=("total_time_seconds", "sum"),
        median_speed_mph=("speed_mph", "median"),
        mean_speed_mph=("speed_mph", "mean"),
    ).reset_index()
    return daily.rename(columns={"route_id": "route", "service_date": "date"})


def _date_str(value) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def prepare_dynamo_items(daily: pd.DataFrame) -> list[dict]:
    """Convert a daily-metrics frame into DynamoDB-ready items (Decimal values, ISO dates)."""
    items = []
    for row in daily.to_dict(orient="records"):
        items.append(
            {
                "route": str(row["route"]),
                "date": _date_str(row["date"]),
                "count": Decimal(int(row["count"])),
                "n_traversals": Decimal(int(row["n_traversals"])),
                "n_interpolated": Decimal(int(row["n_interpolated"])),
                "miles_covered": Decimal(str(round(row["miles_covered"], 3))),
                "total_time": Decimal(str(round(row["total_time"], 1))),
                "median_speed_mph": Decimal(str(round(row["median_speed_mph"], 2))),
                "mean_speed_mph": Decimal(str(round(row["mean_speed_mph"], 2))),
            }
        )
    return items


def write_daily_route_metrics(traversals: pd.DataFrame) -> int:
    """Aggregate a day of traversals to per-route metrics and batch-write them to DynamoDB.

    Returns the number of route rows written.
    """
    daily = build_daily_route_metrics(traversals)
    items = prepare_dynamo_items(daily)
    logger.info(f"Writing {len(items)} daily route speed rows to {DYNAMO_TABLE_NAME}")
    dynamo.dynamo_batch_write(items, DYNAMO_TABLE_NAME)
    return len(items)
