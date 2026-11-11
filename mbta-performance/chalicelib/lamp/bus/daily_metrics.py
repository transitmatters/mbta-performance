"""Aggregate a day of LAMP bus data to one row per (route, service_date) in DynamoDB.

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

Trip counts and headway reliability come from the raw LAMP events instead
(`build_event_route_metrics`), because traversals only exist for trips that ran: a trip LAMP
never observed has no segments, but still has a row for every scheduled stop.
"""

import logging
from decimal import Decimal

import pandas as pd

from ... import dynamo
from .constants import DYNAMO_TABLE_NAME, HEADWAY_POINT_TYPES, HEADWAY_RATIO_BUCKETS, METERS_PER_MILE
from .segments import assign_time_band, has_observed_dwell

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
    # Dwell at the stops that have one: see _with_observed_dwell. Average dwell is
    # total_dwell / n_dwell_stops, not / n_traversals.
    "total_dwell": ("observed_dwell_seconds", "sum"),
    "n_dwell_stops": ("has_observed_dwell", "sum"),
}

ROUTE_DAY_KEY = ["route_id", "service_date", "day_type"]

# Per-route totals from the raw events, all whole counts that sum across days.
TRIP_FIELDS = ("scheduled_trips", "unobserved_trips", "unobserved_waived_trips")
HEADWAY_FIELDS = tuple(bucket[0] for bucket in HEADWAY_RATIO_BUCKETS)
EVENT_FIELDS = TRIP_FIELDS + HEADWAY_FIELDS


def build_daily_route_metrics(traversals: pd.DataFrame, events: pd.DataFrame | None = None) -> pd.DataFrame:
    """Aggregate segment traversals, and optionally the raw events, to one row per (route, service_date).

    `traversals` is the per-(trip, segment) frame from `segments.build_traversals`, before
    it's broken down further by direction, stop pair, or time band, plus a `day_type` column
    (day_type.day_type_for of its service date).

    The top-level totals cover every traversal. `time_bands` holds the same summable totals
    per time band the traversal departed in, for bands with at least one traversal. A band's
    `count` is the trips with a traversal departing in that band, so a trip that crosses a
    band boundary counts in both, and the band counts sum to more than the day's `count`.

    `events`, when given, is the same day's LAMP events as read, with the same `day_type`
    column, and adds the EVENT_FIELDS from `build_event_route_metrics`. A route that appears
    only there -- every trip unobserved -- still gets a row, with no speed fields.
    """
    traversals = _with_observed_dwell(traversals)
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
    if events is not None:
        daily = daily.merge(build_event_route_metrics(events), on=ROUTE_DAY_KEY, how="outer")
    return daily.rename(columns={"route_id": "route", "service_date": "date"})


def _with_observed_dwell(traversals: pd.DataFrame) -> pd.DataFrame:
    """Mark the traversals whose dwell at their first stop was actually observed
    (segments.has_observed_dwell), which alone count toward the dwell total and the stop count."""
    has_dwell = has_observed_dwell(traversals)
    return traversals.assign(
        has_observed_dwell=has_dwell,
        observed_dwell_seconds=traversals.dwell_seconds.where(has_dwell, 0.0),
    )


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


def build_event_route_metrics(events: pd.DataFrame) -> pd.DataFrame:
    """Trip counts and headway reliability per (route_id, service_date, day_type), from raw LAMP events.

    `events` is a day of LAMP bus events as read, before interpolation, plus a `day_type`
    column. LAMP emits a row for every scheduled stop of every scheduled trip, observed or not.

    * `scheduled_trips`: distinct trips. A trip interlined across routes counts in each.
    * `unobserved_trips`: trips with no observed arrival or departure at any stop. Nearly all
      carry the placeholder vehicle_label "____", but that label also shows up on trips that
      were partly observed, so it isn't the test.
    * `unobserved_waived_trips`: unobserved trips with a `waiver_remark` on any of their rows.
    * One count per HEADWAY_RATIO_BUCKETS bucket, of the route-direction headways at
      HEADWAY_POINT_TYPES timepoints by their ratio to the scheduled headway. Rows without a
      positive scheduled headway are skipped; an actual headway of zero is bunched.
    """
    observed = events.stop_arrival_seconds.notna() | events.stop_departure_seconds.notna()
    trips = (
        events.assign(observed=observed, waived=events.waiver_remark.notna())
        .groupby(ROUTE_DAY_KEY + ["trip_id"], sort=False)
        .agg(observed=("observed", "any"), waived=("waived", "any"))
        .reset_index()
    )
    unobserved = ~trips.observed
    per_route = (
        trips.assign(unobserved=unobserved, unobserved_waived=unobserved & trips.waived)
        .groupby(ROUTE_DAY_KEY, sort=False)
        .agg(
            scheduled_trips=("trip_id", "size"),
            unobserved_trips=("unobserved", "sum"),
            unobserved_waived_trips=("unobserved_waived", "sum"),
        )
    )

    headway = events.route_direction_headway_seconds
    scheduled = events.plan_route_direction_headway_seconds
    usable = events.point_type.isin(HEADWAY_POINT_TYPES) & headway.notna() & scheduled.fillna(0).gt(0)
    ratio = (headway[usable] / scheduled[usable]).astype("float64")
    buckets = {name: _in_bucket(ratio, *edges) for name, *edges in HEADWAY_RATIO_BUCKETS}
    headways = events.loc[usable, ROUTE_DAY_KEY].assign(**buckets).groupby(ROUTE_DAY_KEY, sort=False).sum()

    # Every route with a headway also has trips, so the left join drops nothing.
    metrics = per_route.join(headways, how="left").fillna(0).astype("int64").reset_index()
    return metrics[ROUTE_DAY_KEY + list(EVENT_FIELDS)]


def _in_bucket(ratio: pd.Series, lower: float, upper: float, includes_lower: bool, includes_upper: bool) -> pd.Series:
    above = ratio >= lower if includes_lower else ratio > lower
    below = ratio <= upper if includes_upper else ratio < upper
    return above & below


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
        "total_dwell": Decimal(str(round(totals["total_dwell"], 1))),
        "n_dwell_stops": Decimal(int(totals["n_dwell_stops"])),
    }


def prepare_dynamo_items(daily: pd.DataFrame) -> list[dict]:
    """Convert a daily-metrics frame into DynamoDB-ready items (Decimal values, ISO dates).

    Speed fields are only on routes with traversals, and EVENT_FIELDS only where events were
    aggregated, so a field with nothing behind it is left off the item rather than written as
    NaN or zero over whatever the row holds.
    """
    items = []
    for row in daily.to_dict(orient="records"):
        item = {"route": str(row["route"]), "date": _date_str(row["date"]), "day_type": str(row["day_type"])}
        if pd.notna(row["n_traversals"]):
            item.update(
                _summable_item_fields(row),
                median_speed_mph=Decimal(str(round(row["median_speed_mph"], 2))),
                mean_speed_mph=Decimal(str(round(row["mean_speed_mph"], 2))),
                # SET as one whole map, so a band that drops out on a re-run doesn't linger.
                time_bands={str(band): _summable_item_fields(totals) for band, totals in row["time_bands"].items()},
            )
        for field in EVENT_FIELDS:
            if pd.notna(row.get(field)):
                item[field] = Decimal(int(row[field]))
        items.append(item)
    return items


def write_daily_route_metrics(traversals: pd.DataFrame, events: pd.DataFrame | None = None) -> int:
    """Aggregate a day of traversals (and events) to per-route metrics and upsert them into DynamoDB.

    `traversals` and `events` must carry a `day_type` column -- see `build_daily_route_metrics`.

    Only these fields (including `day_type` and the whole `time_bands` map) are SET on each
    row; other fields on the same (route, date) row, such as the fleet stats data-ingestion
    writes, are left alone.

    Returns the number of route rows written.
    """
    daily = build_daily_route_metrics(traversals, events)
    items = prepare_dynamo_items(daily)
    logger.info(f"Writing {len(items)} daily route rows to {DYNAMO_TABLE_NAME}")
    dynamo.dynamo_update_items(items, DYNAMO_TABLE_NAME)
    return len(items)
