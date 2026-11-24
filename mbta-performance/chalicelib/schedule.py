"""Scheduled benchmarks (headways and travel times), shared by the historic and LAMP pipelines.

The historic (ArcGIS) and LAMP pipelines read their events from different places, and their GTFS
too (the MBTA's archive vs s3://tm-gtfs), but turn them into benchmarks the same way:

- Trunk headways pool every branch serving a stop (all Green Line branches at Copley); branch
  headways pool one branch (Red-A, Green-D). See routes.trunk_route_id and routes.branch_route_id.
- Scheduled headways are smoothed to their mean per trunk (or branch), direction and stop over each
  30 minutes of the service day, rounded to 10 seconds, so the dashboard's benchmark doesn't jump
  from train to train (smooth_scheduled_headways).
- Each actual trip is matched to a scheduled trip (its own, where its trip_id is in GTFS, or the one
  that left its first stop nearest in time), and takes that trip's scheduled travel time to each
  stop, visit by visit on loops (add_scheduled_travel_times).

Only where each event's scheduled headway comes from, before smoothing, differs. LAMP publishes its
own, pooled over a station's platforms the same way as its actual headways. The ArcGIS data has
none, so the historic pipeline derives them from GTFS (gtfs_scheduled_headways).

GTFS stop times passed in here need trip_id, route_id, direction_id, stop_id and arrival_time in
seconds after midnight on the service date, sorted by arrival_time. Events need event_time, either
tz-aware (LAMP) or naive Eastern wall-clock time (historic).
"""

import logging
from datetime import date

import numpy as np
import pandas as pd

from .routes import trunk_route_id

logger = logging.getLogger(__name__)

RTE_DIR_STOP = ["route_id", "direction_id", "stop_id"]
TRUNK_DIR_STOP = ["trunk_route_id", "direction_id", "stop_id"]
BRANCH_DIR_STOP = ["branch_route_id", "direction_id", "stop_id"]

BUCKET_SECONDS = 30 * 60
# Service days run past midnight. Leave room for very early or late events rather than be opinionated.
SERVICE_DAY_HOURS = 48


def seconds_after_midnight(event_time: pd.Series, service_date: date) -> pd.Series:
    """Seconds from midnight at the start of the service date: the clock GTFS arrival times use."""
    midnight = pd.Timestamp(service_date)
    if event_time.dt.tz is not None:
        midnight = midnight.tz_localize(event_time.dt.tz)
    return (event_time - midnight).dt.total_seconds()


def _half_hour_buckets(event_time: pd.Series, service_date: date) -> np.ndarray:
    """Number each event's 30-minute window of the service day, by Eastern wall-clock time from midnight.

    When clocks fall back, windows in the repeated hour start at its first (daylight time) occurrence,
    so the 1:30 window runs until 2:00 standard time. Events outside the service day get NaN.
    """
    midnight = pd.Timestamp(service_date)
    edges = pd.date_range(midnight, periods=SERVICE_DAY_HOURS * 3600 // BUCKET_SECONDS + 2, freq=f"{BUCKET_SECONDS}s")
    if event_time.dt.tz is not None:
        edges = edges.tz_localize(
            event_time.dt.tz, ambiguous=np.ones(len(edges), dtype=bool), nonexistent="shift_forward"
        )
    edges = edges.as_unit(event_time.dt.unit)
    bucket = edges.searchsorted(event_time.array, side="right") - 1
    in_day = event_time.notna().to_numpy() & (bucket >= 0) & (bucket < len(edges) - 1)
    return np.where(in_day, bucket, np.nan)


def in_service_day(event_time: pd.Series, service_date: date) -> np.ndarray:
    """Whether each event falls in one of the service day's 30-minute windows, from midnight to 48:30 after.

    The LAMP pipelines keep only these, as they always have. LAMP's bus files are dated in UTC, so they
    carry some of the previous evening's events.
    """
    return ~np.isnan(_half_hour_buckets(event_time, service_date))


def smooth_scheduled_headways(
    events: pd.DataFrame, service_date: date, pools: dict[str, list[str]] | None = None
) -> pd.DataFrame:
    """Replace each scheduled headway with its pool's mean over the same 30 minutes, to the nearest 10 seconds.

    pools maps each headway column to the keys it's pooled by. By default that's scheduled_headway
    per trunk, direction and stop: for bus and ferry routes, the trunk is the route.
    """
    if pools is None:
        pools = {"scheduled_headway": TRUNK_DIR_STOP}
    grouping = events.assign(
        trunk_route_id=trunk_route_id(events["route_id"]).array,
        _bucket=_half_hour_buckets(events["event_time"], service_date),
    )
    smoothed = {
        column: grouping.groupby([*keys, "_bucket"])[column].transform("mean").round(-1).array
        for column, keys in pools.items()
    }
    return events.assign(**smoothed)


def gtfs_scheduled_headways(events: pd.DataFrame, gtfs_stops: pd.DataFrame, service_date: date) -> pd.Series:
    """Each event's scheduled trunk headway from GTFS, aligned to events by position.

    That's the scheduled gap before the most recent scheduled arrival at the event's trunk, direction
    and stop. Branches scheduled at a shared stop in the same second count as one arrival: otherwise
    the second would get a 0s headway, and so would every event until the next arrival (#131).
    """
    scheduled = gtfs_stops.assign(
        trunk_route_id=trunk_route_id(gtfs_stops["route_id"]).array,
        arrival_time=gtfs_stops["arrival_time"].astype(float),
    )
    scheduled = scheduled.drop_duplicates([*TRUNK_DIR_STOP, "arrival_time"]).sort_values("arrival_time", kind="stable")
    scheduled["scheduled_headway"] = scheduled.groupby(TRUNK_DIR_STOP)["arrival_time"].diff()

    left = pd.DataFrame(
        {
            "trunk_route_id": trunk_route_id(events["route_id"]).array,
            "direction_id": events["direction_id"].array,
            "stop_id": events["stop_id"].array,
            "arrival_time": seconds_after_midnight(events["event_time"], service_date).to_numpy(),
            "_row": np.arange(len(events)),
        }
    ).dropna(subset=[*TRUNK_DIR_STOP, "arrival_time"])
    matched = pd.merge_asof(
        left.sort_values("arrival_time", kind="stable"),
        scheduled[[*TRUNK_DIR_STOP, "arrival_time", "scheduled_headway"]],
        on="arrival_time",
        by=TRUNK_DIR_STOP,
        direction="backward",
    )
    headways = np.full(len(events), np.nan)
    headways[matched["_row"].to_numpy()] = matched["scheduled_headway"].to_numpy(dtype="float64", na_value=np.nan)
    return pd.Series(headways, index=events.index, name="scheduled_headway")


def add_scheduled_travel_times(
    events: pd.DataFrame,
    gtfs_stops: pd.DataFrame,
    service_date: date,
    match_on: tuple[str, ...] = tuple(RTE_DIR_STOP),
    stop_on: tuple[str, ...] = ("stop_id",),
    own_trip_ids: bool = False,
    start_event_type: str | None = None,
    extra_visits_take_last: bool = False,
) -> pd.DataFrame:
    """Add scheduled_trip_id and scheduled_tt (seconds since the scheduled trip's first stop) to each event.

    Each actual trip is matched to a scheduled trip, then each event takes that trip's travel time to
    the stop found by stop_on. An event whose stop isn't on the matched trip, as after an interline
    from Green-E to Green-D, gets the median scheduled travel time to its route, direction and stop
    among trips arriving within the same 30 minutes, rounded to the second.

    - Matching: with own_trip_ids, a trip whose trip_id is in GTFS is its own scheduled trip, as bus
      LAMP trip_ids are (#135). Any other trip is matched, at its first stop, to the scheduled trip with
      the same match_on keys whose arrival there is nearest in time. That first stop is the trip's
      first event of start_event_type if it has one (a bus can arrive early for its layover and be
      nearer the previous trip), and its first event otherwise.
    - Repeat visits: a trip that passes through a stop twice (route 120 at Maverick) needs a visit
      number on both sides as the last stop_on column, counted from 0. With extra_visits_take_last,
      an event numbered past the scheduled trip's last visit to the stop takes that last visit.

    Any scheduled_tt already on events is replaced. Returns a new frame, in the same row order, with a
    fresh index. scheduled_tt has the same dtype as gtfs_stops' arrival_time.
    """
    match_on, stop_on = list(match_on), list(stop_on)
    events = events.drop(columns=["scheduled_tt", "scheduled_trip_id"], errors="ignore").reset_index(drop=True)
    scheduled = gtfs_stops.assign(
        scheduled_tt=gtfs_stops["arrival_time"] - gtfs_stops.groupby("trip_id")["arrival_time"].transform("min")
    )
    scheduled["arrival_time"] = scheduled["arrival_time"].astype(float)

    if own_trip_ids:
        events["scheduled_trip_id"] = events["trip_id"].where(events["trip_id"].isin(scheduled["trip_id"]))
    else:
        events["scheduled_trip_id"] = pd.Series(np.nan, index=events.index, dtype=object)
    unscheduled = events["scheduled_trip_id"].isna()
    if unscheduled.any():
        matched = _match_trips_by_start_time(events[unscheduled], scheduled, service_date, match_on, start_event_type)
        events.loc[unscheduled, "scheduled_trip_id"] = events.loc[unscheduled, "trip_id"].map(matched)

    lookup_keys = events[["scheduled_trip_id", *stop_on]]
    if extra_visits_take_last:
        *at_stop, visit = stop_on
        last_visit = scheduled.groupby(["trip_id", *at_stop])[visit].max()
        last_for_event = _lookup(last_visit, events[["scheduled_trip_id", *at_stop]])
        visits = events[visit].to_numpy(dtype="float64", na_value=np.nan)
        last_for_event = np.asarray(last_for_event, dtype="float64")
        lookup_keys = lookup_keys.assign(**{visit: np.where(visits > last_for_event, last_for_event, visits)})

    # Look up by trip and stop, not route: an interlined trip has a different route_id in the schedule
    on_trip = scheduled.drop_duplicates(subset=["trip_id", *stop_on]).set_index(["trip_id", *stop_on])["scheduled_tt"]
    events["scheduled_tt"] = _lookup(on_trip, lookup_keys)

    missing = events["scheduled_tt"].isna()
    if missing.any():
        # Rounded to the second: the median of an even count can land on a half second, which the
        # Int64 scheduled_tt column the LAMP pipelines write refuses.
        median_tt = (
            scheduled.assign(time_bucket=scheduled["arrival_time"] // BUCKET_SECONDS)
            .groupby([*RTE_DIR_STOP, "time_bucket"])["scheduled_tt"]
            .median()
            .round()
        )
        lookup = events.loc[missing, RTE_DIR_STOP].assign(
            time_bucket=seconds_after_midnight(events.loc[missing, "event_time"], service_date) // BUCKET_SECONDS
        )
        fallback = _lookup(median_tt, lookup).to_numpy(dtype="float64", na_value=np.nan)
        events.loc[missing, "scheduled_tt"] = fallback
        logger.debug(
            f"Filled {np.count_nonzero(~np.isnan(fallback))} of {missing.sum()} events with median scheduled_tt"
        )

    return events


def _match_trips_by_start_time(
    events: pd.DataFrame,
    scheduled: pd.DataFrame,
    service_date: date,
    match_on: list[str],
    start_event_type: str | None,
) -> pd.Series:
    """Map each trip_id to the scheduled trip, with the same match_on keys, nearest in time at its first stop."""
    starts = events.dropna(subset=[*match_on, "trip_id", "event_time"]).sort_values("event_time", kind="stable")
    if start_event_type is not None:
        starts = pd.concat([starts[starts["event_type"] == start_event_type], starts])
    starts = starts.drop_duplicates("trip_id")[[*match_on, "trip_id", "event_time"]]
    starts["arrival_time"] = seconds_after_midnight(starts["event_time"], service_date)
    matched = pd.merge_asof(
        starts.sort_values("arrival_time", kind="stable"),
        scheduled[[*match_on, "arrival_time", "trip_id"]].drop_duplicates(),
        on="arrival_time",
        direction="nearest",
        by=match_on,
        suffixes=["", "_scheduled"],
    )
    return matched.set_index("trip_id")["trip_id_scheduled"]


def _lookup(values: pd.Series, keys: pd.DataFrame) -> pd.api.extensions.ExtensionArray:
    """values, which has a unique MultiIndex, at each row of keys (missing where there's none).

    Unlike a merge, this doesn't require the keys' dtypes to match, so a day where nothing matched
    (all-NaN keys) just comes back empty.
    """
    return values.reindex(pd.MultiIndex.from_frame(keys)).array
