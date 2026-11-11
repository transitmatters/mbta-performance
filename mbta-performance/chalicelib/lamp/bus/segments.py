"""Build per-traversal bus speed segments and aggregate them by day and time band."""

import logging

import numpy as np
import pandas as pd

from .constants import (
    ALL_DAY_BAND,
    MAX_PLAUSIBLE_SPEED_MPH,
    METERS_PER_SECOND_TO_MPH,
    MOVING_PERCENTILES,
    PERCENTILES,
    TERMINAL_SECTION_SHARE,
    TIME_BANDS,
)

logger = logging.getLogger(__name__)

TRIP_KEY = ["trip_id", "tm_pullout_id"]
# Patterns that share a key can take different paths between its stops, which would mix lengths in
# one speed. Rare enough to ignore: on 2026-11-10, one key of 10,762 had pattern lengths more than
# 25m apart (route 39 from Ave Louis Pasteur, 730m and 1,101m, 5 traversals), and on 2026-11-08 none.
SEGMENT_KEY = ["route_id", "direction_id", "from_stop_id", "to_stop_id"]


def _by_trip(events: pd.DataFrame):
    """Group events by TRIP_KEY, keeping a trip whose tm_pullout_id is null.

    groupby's default dropna=True would drop such a trip from every traversal. A null trip_id
    groups too, but it isn't a trip: interpolate_missing_times skips it, and build_traversals'
    same-trip check drops it, since NaN never equals NaN.
    """
    return events.groupby(TRIP_KEY, sort=False, dropna=False)


def attach_shape_distance(events: pd.DataFrame, stop_positions: pd.DataFrame) -> pd.DataFrame:
    """Attach each event's distance along its pattern's shape.

    Joined on (route_pattern_id, stop_id, gtfs_stop_sequence) where LAMP reports a GTFS
    sequence, falling back to (route_pattern_id, stop_id). The sequence disambiguates
    patterns that visit the same stop twice, which happens on loop routes.
    """
    positions = stop_positions.rename(columns={"stop_sequence": "gtfs_stop_sequence"})

    merged = events.merge(positions, on=["route_pattern_id", "stop_id", "gtfs_stop_sequence"], how="left")

    unmatched = merged.shape_distance_m.isna()
    if unmatched.any():
        fallback_positions = positions.drop_duplicates(["route_pattern_id", "stop_id"])
        fallback_positions = fallback_positions[["route_pattern_id", "stop_id", "shape_distance_m"]]
        fallback = (
            merged.loc[unmatched, ["route_pattern_id", "stop_id"]]
            .merge(fallback_positions, on=["route_pattern_id", "stop_id"], how="left")
            .shape_distance_m.to_numpy()
        )
        merged.loc[unmatched, "shape_distance_m"] = fallback

    logger.info(
        f"Located {merged.shape_distance_m.notna().mean():.4f} of {len(merged)} bus events on their pattern shape"
    )
    return merged


def interpolate_missing_times(events: pd.DataFrame) -> pd.DataFrame:
    """Fill in when a bus passed a stop it has no recorded time for.

    LAMP emits a row for every scheduled stop, but ~7% of rows carry no observed
    arrival/departure. Where such a row is bracketed by observed rows on the same trip, the
    pass time is interpolated by distance along the shape: the bus is assumed to hold a
    constant speed across the gap. Rows that lead or trail a trip cannot be bracketed and
    are left null.
    """
    events = events.sort_values(TRIP_KEY + ["stop_sequence"]).reset_index(drop=True)

    # A stop is "observed" if LAMP recorded either an arrival or a departure there.
    observed = events.stop_arrival_seconds.notna() | events.stop_departure_seconds.notna()
    # Anchor times: when leaving a stop use its departure, when arriving use its arrival.
    depart_anchor = events.stop_departure_seconds.fillna(events.stop_arrival_seconds)
    arrive_anchor = events.stop_arrival_seconds.fillna(events.stop_departure_seconds)

    events["is_interpolated"] = False
    filled_arrival = events.stop_arrival_seconds.to_numpy(dtype="float64", copy=True)
    filled_departure = events.stop_departure_seconds.to_numpy(dtype="float64", copy=True)
    interpolated_flag = np.zeros(len(events), dtype=bool)

    distance = events.shape_distance_m.to_numpy(dtype="float64")
    observed_array = observed.to_numpy()
    depart_array = depart_anchor.to_numpy(dtype="float64")
    arrive_array = arrive_anchor.to_numpy(dtype="float64")

    for (trip_id, _), index in _by_trip(events).indices.items():
        if pd.isna(trip_id):
            continue
        index = np.sort(index)
        trip_observed = observed_array[index]
        if trip_observed.sum() < 2:
            continue
        positions = np.flatnonzero(trip_observed)
        first, last = positions[0], positions[-1]

        for offset in range(first + 1, last):
            if trip_observed[offset]:
                continue
            row = index[offset]
            if not np.isfinite(distance[row]):
                continue
            previous = positions[positions < offset][-1]
            following = positions[positions > offset][0]
            previous_row, following_row = index[previous], index[following]

            span = distance[following_row] - distance[previous_row]
            start_time = depart_array[previous_row]
            end_time = arrive_array[following_row]
            if not np.isfinite(span) or span <= 0 or not np.isfinite(start_time) or not np.isfinite(end_time):
                continue

            fraction = (distance[row] - distance[previous_row]) / span
            pass_time = start_time + fraction * (end_time - start_time)
            # The bus was not recorded stopping here, so it passes through with no dwell.
            filled_arrival[row] = pass_time
            filled_departure[row] = pass_time
            interpolated_flag[row] = True

    events["stop_arrival_seconds"] = filled_arrival
    events["stop_departure_seconds"] = filled_departure
    events["is_interpolated"] = interpolated_flag
    logger.info(f"Interpolated {int(interpolated_flag.sum())} stop times ({interpolated_flag.mean():.4f} of rows)")
    return events


def build_traversals(events: pd.DataFrame, segments: pd.DataFrame) -> pd.DataFrame:
    """One row per bus per segment: how long it took to get from one stop to the next.

    Two clocks are reported, because "how fast is the bus" has two defensible answers:

    * `moving_time_seconds` -- departure from the first stop to arrival at the second.
      This is the running speed of the vehicle in traffic, and reproduces LAMP's own
      `travel_time_seconds` column exactly.
    * `total_time_seconds` -- arrival to arrival, so it also carries the dwell spent at the
      first stop. This is the time a rider on board actually experiences, and matches the
      convention the rail benchmarks in chalicelib.benchmarks already use.

    Dwell is roughly as large as running time on MBTA bus routes (both median ~21s), so the
    two speeds differ by more than a factor of two. `speed_mph` is the rider-experienced one.

    A traversal is kept or dropped on the arrival-to-arrival clock alone. Where the moving clock
    reads zero or faster than any bus, the traversal is kept and its moving time and speed are null.
    """
    events = events.sort_values(TRIP_KEY + ["stop_sequence"])
    grouped = _by_trip(events)
    no_pullout = int(events.tm_pullout_id.isna().sum())
    if no_pullout:
        logger.info(f"{no_pullout} of {len(events)} bus events have no tm_pullout_id; kept, grouped by trip_id alone")

    traversal = pd.DataFrame(
        {
            "route_id": events.route_id,
            "direction_id": events.direction_id,
            "route_pattern_id": events.route_pattern_id,
            "service_date": events.service_date,
            "trip_id": events.trip_id,
            "vehicle_label": events.vehicle_label,
            "from_stop_id": events.stop_id,
            "to_stop_id": grouped.stop_id.shift(-1),
            "from_arrive_seconds": events.stop_arrival_seconds,
            "depart_seconds": events.stop_departure_seconds,
            "arrive_seconds": grouped.stop_arrival_seconds.shift(-1),
            "is_trip_start": grouped.cumcount() == 0,
            # The second-to-last stop's row, since a trip's last stop starts no segment.
            "is_trip_end": grouped.cumcount(ascending=False) == 1,
            "from_interpolated": events.is_interpolated,
            "to_interpolated": grouped.is_interpolated.shift(-1),
            # Guard against stitching a segment across a trip boundary.
            "next_trip": grouped.trip_id.shift(-1),
        }
    )
    traversal = traversal[traversal.next_trip == traversal.trip_id].drop(columns=["next_trip"])
    traversal = traversal.dropna(subset=["to_stop_id", "depart_seconds", "arrive_seconds"])

    traversal["moving_time_seconds"] = traversal.arrive_seconds - traversal.depart_seconds
    traversal["dwell_seconds"] = (traversal.depart_seconds - traversal.from_arrive_seconds).clip(lower=0)
    # The dwell at a trip's first stop is layover -- the bus sits at the terminal waiting for
    # its scheduled departure, often 10+ minutes. Charging that to the first segment makes it
    # look like a 0.5mph crawl. Nobody rides through it, so it is excluded, matching how the
    # rail benchmarks in chalicelib.benchmarks treat the dwell at an origin stop.
    traversal.loc[traversal.is_trip_start, "dwell_seconds"] = 0.0
    traversal["total_time_seconds"] = traversal.moving_time_seconds + traversal.dwell_seconds
    # A departure with no arrival, anywhere but a trip's first stop, leaves dwell and so total
    # time unknown. If kept, that one NaN would make its segment's percentiles NaN.
    no_arrival = traversal.total_time_seconds.isna()
    traversal = traversal[~no_arrival]

    segment_lengths = segments[["route_pattern_id", "from_stop_id", "to_stop_id", "segment_length_m"]].drop_duplicates(
        ["route_pattern_id", "from_stop_id", "to_stop_id"]
    )
    traversal = traversal.merge(segment_lengths, on=["route_pattern_id", "from_stop_id", "to_stop_id"], how="inner")

    traversal["moving_speed_mph"] = (
        traversal.segment_length_m / traversal.moving_time_seconds * METERS_PER_SECOND_TO_MPH
    )
    traversal["speed_mph"] = traversal.segment_length_m / traversal.total_time_seconds * METERS_PER_SECOND_TO_MPH
    # .eq(True) treats the shifted NaN as False without pandas' object-dtype downcast warning.
    traversal["is_interpolated"] = traversal.from_interpolated.eq(True) | traversal.to_interpolated.eq(True)

    before = len(traversal)
    # Plausibility is judged on the clock speed_mph is timed on. Judged on the moving clock instead,
    # which runs short of every segment (see "The two speeds" in the README), it dropped 22-32% of
    # traversals under 200m on 2026-11-10, against 2-8% this way. Those were mostly the fastest,
    # so about a quarter of the segments under 200m read more than 1 mph slow.
    #
    # There's no lower bound. The slowest traversals that day were buses holding mid-trip, for up
    # to an hour, often on segments where most trips hold: route 109's 7417 -> 7412 runs at a
    # median 0.64 mph. Riders on board sit through that, and a 0.5 mph floor dropped 20 of that
    # segment's 85 trips judged on the moving clock, or 37 on this one, the slowest of them.
    traversal = traversal[(traversal.total_time_seconds > 0) & (traversal.speed_mph <= MAX_PLAUSIBLE_SPEED_MPH)]
    # Kept on the clock above, but the moving clock itself is no use on these: on 2026-11-10 it
    # read zero or less on 12k of them and faster than any bus on 25k more. Nulled, they drop out
    # of the moving percentiles in _aggregate, rather than making them zero or infinite.
    unusable_moving_clock = (traversal.moving_time_seconds <= 0) | (
        traversal.moving_speed_mph > MAX_PLAUSIBLE_SPEED_MPH
    )
    traversal = traversal.assign(
        moving_time_seconds=traversal.moving_time_seconds.mask(unusable_moving_clock),
        moving_speed_mph=traversal.moving_speed_mph.mask(unusable_moving_clock),
    )
    logger.info(
        f"Built {len(traversal)} traversals ({before - len(traversal)} dropped as implausible, "
        f"{int(no_arrival.sum())} for a departure with no arrival; "
        f"{int(unusable_moving_clock.sum())} kept without a usable moving time)"
    )
    return traversal


def observed_dwell_seconds(traversals: pd.DataFrame) -> pd.Series:
    """Each traversal's dwell at its first stop, or NaN where build_traversals forced it to zero.

    That's two kinds: a trip's first stop, where the wait is layover, and an interpolated stop,
    where LAMP didn't record the bus stopping. Counting those zeros would drag a median or
    an average of dwell down.
    """
    observed = ~traversals.is_trip_start.eq(True) & ~traversals.from_interpolated.eq(True)
    return traversals.dwell_seconds.where(observed)


def assign_time_band(departure_seconds: pd.Series) -> pd.Series:
    """Label each traversal with the time band it departed in."""
    band = pd.Series(pd.NA, index=departure_seconds.index, dtype="object")
    for name, start, end in TIME_BANDS:
        band = band.mask((departure_seconds >= start) & (departure_seconds < end), name)
    return band


def with_time_bands(traversals: pd.DataFrame) -> pd.DataFrame:
    """A copy of `traversals` labelled with their time band, less any that departed outside every band."""
    traversals = traversals.assign(time_band=assign_time_band(traversals.depart_seconds))
    return traversals.dropna(subset=["time_band"])


def aggregate_segments(
    traversals: pd.DataFrame, extra_group_columns: tuple[str, ...] = ("service_date",)
) -> pd.DataFrame:
    """Aggregate traversals to one row per (segment, time band), plus any extra_group_columns.

    Defaults to also grouping by service_date, for the daily pipeline in ingest.py where
    `traversals` covers a single date. Pass `extra_group_columns=()` to roll every date in
    `traversals` together instead, for the weekly/monthly trend rollups in trends.py.

    Alongside the TIME_BANDS rows, each segment also gets an ALL_DAY_BAND row (one per
    combination of extra_group_columns, so one per day_type in trends.py) aggregated from
    every traversal that landed in any band. It's computed from the traversals themselves,
    never from the band rows: a percentile of percentiles is not the day's percentile.
    Traversals that departed outside every band are dropped before either aggregation, so
    all_day covers exactly the traversals the bands do.
    """
    traversals = with_time_bands(traversals)

    segment_key = SEGMENT_KEY + list(extra_group_columns)
    by_band = _aggregate(traversals, segment_key + ["time_band"])
    all_day = _aggregate(traversals, segment_key).assign(time_band=ALL_DAY_BAND)
    aggregated = pd.concat([by_band, all_day[by_band.columns]], ignore_index=True)

    logger.info(
        f"Aggregated to {len(by_band)} ({', '.join(segment_key + ['time_band'])}) rows "
        f"plus {len(all_day)} {ALL_DAY_BAND} rows"
    )
    return aggregated


def _aggregate(traversals: pd.DataFrame, group_key: list[str]) -> pd.DataFrame:
    """Percentile times and speeds for each group of traversals.

    median_dwell_seconds is over the dwells that were observed (observed_dwell_seconds), as
    daily_metrics' average is, and NaN where there are none, as on a segment every trip starts on.
    """
    # Copy-on-write makes this add one column, not copy the frame.
    grouped = traversals.assign(observed_dwell_seconds=observed_dwell_seconds(traversals)).groupby(
        group_key, sort=False
    )

    aggregation = {
        "n_traversals": ("total_time_seconds", "size"),
        "n_interpolated": ("is_interpolated", "sum"),
        "segment_length_m": ("segment_length_m", "median"),
        "median_dwell_seconds": ("observed_dwell_seconds", "median"),
        "trip_start_share": ("is_trip_start", "mean"),
        "trip_end_share": ("is_trip_end", "mean"),
    }
    for measure, percentiles in (("total_time_seconds", PERCENTILES), ("moving_time_seconds", MOVING_PERCENTILES)):
        for percentile in percentiles:
            aggregation[f"p{percentile}_{measure}"] = (
                measure,
                lambda series, q=percentile: _percentile(series, q),
            )

    aggregated = grouped.agg(**aggregation).reset_index()

    # Speed comes from the matching time percentile, so p50 speed is the median trip's
    # speed. Note the ordering inverts: the p90 *time* is the slow tail, hence a low speed.
    for percentile in PERCENTILES:
        aggregated[f"p{percentile}_speed_mph"] = (
            aggregated.segment_length_m / aggregated[f"p{percentile}_total_time_seconds"] * METERS_PER_SECOND_TO_MPH
        )
    for percentile in MOVING_PERCENTILES:
        aggregated[f"p{percentile}_moving_speed_mph"] = (
            aggregated.segment_length_m / aggregated[f"p{percentile}_moving_time_seconds"] * METERS_PER_SECOND_TO_MPH
        )

    return _with_terminal_sections(aggregated)


def _percentile(series: pd.Series, percentile: int) -> float:
    """np.percentile of the non-null values, or NaN if there are none.

    Only moving_time_seconds has nulls (see build_traversals). np.nanpercentile would do the same,
    but it warns on a group with no moving times at all.
    """
    values = series.to_numpy(dtype="float64")
    values = values[~np.isnan(values)]
    return float(np.percentile(values, percentile)) if values.size else np.nan


def _with_terminal_sections(aggregated: pd.DataFrame) -> pd.DataFrame:
    """Swap the trip start and end shares for what they add up to: `terminal_section`.

    "first" where most trips over a segment start on it, "last" where most end on it, and null on
    everything else. Those two segments are the only ones in a trip that read differently: on
    2026-10-28 a trip's first segment ran at a median 15 mph, because its layover isn't counted as
    dwell, and its last at 9 mph, against 12 mph for each of the segments next to those. The last
    ones are 3% of segments but 35-50% of those under 3 mph, going by two weekdays and a Sunday.

    A share rather than the pattern's own first and last stops, because a short-turning pattern
    would otherwise mark a stretch that nearly every trip runs straight through. It rarely matters
    where the line falls: only 26 of 9,948 shares fell between a quarter and three quarters. A trip
    of one segment is "first".
    """
    starts = aggregated.trip_start_share >= TERMINAL_SECTION_SHARE
    ends = aggregated.trip_end_share >= TERMINAL_SECTION_SHARE
    aggregated["terminal_section"] = None
    aggregated.loc[ends, "terminal_section"] = "last"
    aggregated.loc[starts, "terminal_section"] = "first"
    return aggregated.drop(columns=["trip_start_share", "trip_end_share"])


def select_segment_geometry(traversals: pd.DataFrame, segments: pd.DataFrame) -> pd.DataFrame:
    """Pick one geometry per (route, direction, stop pair).

    Several patterns of a route can traverse the same consecutive stop pair. Their shapes
    are near-identical there, so we keep the geometry from whichever pattern actually ran
    that segment most often on the day, which also keeps the map free of overplotted
    near-duplicate lines.
    """
    usage = (
        traversals.groupby(["route_pattern_id", "from_stop_id", "to_stop_id"], sort=False)
        .size()
        .rename("pattern_traversals")
        .reset_index()
    )
    candidates = segments.merge(usage, on=["route_pattern_id", "from_stop_id", "to_stop_id"], how="inner")
    candidates = candidates.sort_values("pattern_traversals", ascending=False)
    chosen = candidates.drop_duplicates(SEGMENT_KEY)

    logger.info(f"Selected geometry for {len(chosen)} distinct segments from {len(candidates)} pattern candidates")
    return chosen[
        SEGMENT_KEY + ["from_stop_name", "to_stop_name", "route_pattern_id", "coordinates", "pattern_traversals"]
    ].rename(columns={"route_pattern_id": "geometry_source_pattern_id"})
