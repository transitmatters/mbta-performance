"""How long a route's trips take between stops, against free flow, as plain JSON for the dashboard.

The segments' free-flow running times (segments.free_flow_running_times) show where buses lose
time. This shows how much a whole trip loses: for each route, direction and time band, the median
trip's running time and its free-flow running time, so the dashboard can say "the 109 took 48
minutes between stops in the PM peak, against 25 at free flow" (t-performance-dash#1493).

It's worked out trip by trip, not by summing the segment rows. A route direction's segments can
belong to several patterns (a branch, a short turn), so their sum is no one trip; and a sum of
medians is not the median trip. Each trip's running time and free flow are summed over the
segments it ran that have a free flow, so the two always cover the same stretch, and a trip is
only counted if that's most of it (MIN_ROUTE_DELAY_COVERAGE).

Published beside leaderboard.json, keyed the same way: by time band, and on the weekly and
monthly files by day type first.
"""

import json
import logging

import pandas as pd

from .constants import ALL_DAY_BAND, MIN_ROUTE_DELAY_COVERAGE, MIN_ROUTE_DELAY_TRIPS
from .segments import SEGMENT_KEY, assign_time_band, free_flow_running_times, with_time_bands

logger = logging.getLogger(__name__)

# A trip, as the traversals tell them apart: they have no tm_pullout_id (segments.TRIP_KEY), and a
# trip_id is one trip a day. The vehicle splits the rare trip handed from one bus to another
# mid-route (14 of 12,795 trip_ids on 2026-11-04) into its pieces.
TRIP_KEY = ["service_date", "trip_id", "vehicle_label"]

# What each entry carries. time_band and day_type aren't repeated: they're the keys it sits under,
# as in leaderboard.json.
ROUTE_DELAY_COLUMNS = [
    "route_id",
    "direction_id",
    "n_trips",
    "p50_running_time_seconds",
    "free_flow_running_time_seconds",
]


def trip_running_times(traversals: pd.DataFrame) -> pd.DataFrame:
    """One row per trip: its running time and free-flow running time over the segments it ran
    that have a free flow, and `coverage`, the share of its length those segments are.

    A trip takes the time band of its first departure, so a trip that starts at 3:55 PM is a
    midday trip all the way. Free flow is taken from these same traversals, every date and day
    type, as aggregate_segments takes it. A traversal with no usable running time
    (build_traversals) counts toward the trip's length but not its covered length.
    """
    traversals = with_time_bands(traversals)
    free_flow = free_flow_running_times(traversals, SEGMENT_KEY)
    timed = traversals.merge(free_flow[SEGMENT_KEY + ["free_flow_running_time_seconds"]], on=SEGMENT_KEY, how="left")
    covered = timed.running_time_seconds.notna() & timed.free_flow_running_time_seconds.notna()

    descriptive = ["route_id", "direction_id"] + (["day_type"] if "day_type" in timed.columns else [])
    by_trip = timed.groupby(TRIP_KEY, sort=False, dropna=False)
    trips = by_trip.agg(
        **{column: (column, "first") for column in descriptive},
        first_departure=("depart_seconds", "min"),
        length_m=("segment_length_m", "sum"),
    )
    covered_by_trip = timed[covered].groupby(TRIP_KEY, sort=False, dropna=False)
    trips = trips.join(
        covered_by_trip.agg(
            covered_length_m=("segment_length_m", "sum"),
            running_time_seconds=("running_time_seconds", "sum"),
            free_flow_running_time_seconds=("free_flow_running_time_seconds", "sum"),
        )
    )
    trips["coverage"] = (trips.covered_length_m / trips.length_m).fillna(0.0)
    trips["time_band"] = assign_time_band(trips.first_departure)
    return trips.drop(columns=["first_departure", "covered_length_m"]).reset_index()


def _entries(rows: pd.DataFrame) -> list[dict]:
    """One entry per route direction with enough trips, in route order, as JSON-safe records.

    Times are rounded to whole seconds. Through to_json rather than to_dict, as in leaderboard.py,
    for the numpy dtypes.
    """
    entries = (
        rows.groupby(["route_id", "direction_id"], sort=True)
        .agg(
            n_trips=("running_time_seconds", "size"),
            p50_running_time_seconds=("running_time_seconds", "median"),
            free_flow_running_time_seconds=("free_flow_running_time_seconds", "median"),
        )
        .reset_index()
    )
    entries = entries[entries.n_trips >= MIN_ROUTE_DELAY_TRIPS][ROUTE_DELAY_COLUMNS].assign(
        p50_running_time_seconds=lambda df: df.p50_running_time_seconds.round().astype("Int64"),
        free_flow_running_time_seconds=lambda df: df.free_flow_running_time_seconds.round().astype("Int64"),
    )
    return json.loads(entries.to_json(orient="records"))


def _by_band(trips: pd.DataFrame) -> dict[str, list[dict]]:
    """Entries for each time band and all_day, leaving out any with none."""
    slices = {str(band): _entries(rows) for band, rows in trips.groupby("time_band", sort=False)}
    slices[ALL_DAY_BAND] = _entries(trips)
    return {band: entries for band, entries in slices.items() if entries}


def build_route_delays(traversals: pd.DataFrame) -> dict:
    """The median trip's running time and free flow for each route direction, by time band.

    Only trips with a free flow over at least MIN_ROUTE_DELAY_COVERAGE of their length count, and
    a route direction needs MIN_ROUTE_DELAY_TRIPS of them in a band to have an entry there. Keyed
    by time band, e.g. {"pm_peak": [...]}, or, when the traversals carry day_type (the weekly and
    monthly rollups), by day type first: {"business_day": {"pm_peak": [...]}, ...}.

    The dashboard takes the minutes lost to be the difference of the two medians. Most route
    directions run one pattern at most hours, so each trip has the same free flow and that's the
    median trip's delay too. On 2026-W45 it came within 7s of it at the median, and 28s at the
    90th percentile.
    """
    trips = trip_running_times(traversals)
    counted = trips[trips.coverage >= MIN_ROUTE_DELAY_COVERAGE]

    if "day_type" in counted.columns:
        result = {str(day_type): _by_band(rows) for day_type, rows in counted.groupby("day_type", sort=False)}
    else:
        result = _by_band(counted)

    logger.info(
        f"Built route delays from {len(counted)} of {len(trips)} trips "
        f"(the rest have a free flow over less than {MIN_ROUTE_DELAY_COVERAGE:.0%} of their length)"
    )
    return result
