"""Build per-traversal bus speed segments and aggregate them by day and time band."""

import logging

import numpy as np
import pandas as pd

from .constants import (
    MAX_PLAUSIBLE_SPEED_MPH,
    METERS_PER_SECOND_TO_MPH,
    MIN_PLAUSIBLE_SPEED_MPH,
    PERCENTILES,
    TIME_BANDS,
)

logger = logging.getLogger(__name__)

TRIP_KEY = ["trip_id", "tm_pullout_id"]
SEGMENT_KEY = ["route_id", "direction_id", "from_stop_id", "to_stop_id"]


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

    for _, index in events.groupby(TRIP_KEY, sort=False).indices.items():
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
    """
    events = events.sort_values(TRIP_KEY + ["stop_sequence"])
    grouped = events.groupby(TRIP_KEY, sort=False)

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
    # Plausibility is judged on the moving speed: the dwell-inclusive figure is legitimately
    # near zero at a long layover, but no bus covers ground at 65mph between stops.
    traversal = traversal[
        (traversal.moving_time_seconds > 0)
        & (traversal.moving_speed_mph >= MIN_PLAUSIBLE_SPEED_MPH)
        & (traversal.moving_speed_mph <= MAX_PLAUSIBLE_SPEED_MPH)
    ]
    logger.info(f"Built {len(traversal)} traversals ({before - len(traversal)} dropped as implausible)")
    return traversal


def assign_time_band(departure_seconds: pd.Series) -> pd.Series:
    """Label each traversal with the time band it departed in."""
    band = pd.Series(pd.NA, index=departure_seconds.index, dtype="object")
    for name, start, end in TIME_BANDS:
        band = band.mask((departure_seconds >= start) & (departure_seconds < end), name)
    return band


def aggregate_segments(
    traversals: pd.DataFrame, extra_group_columns: tuple[str, ...] = ("service_date",)
) -> pd.DataFrame:
    """Aggregate traversals to one row per (segment, time band), plus any extra_group_columns.

    Defaults to also grouping by service_date, for the daily pipeline in ingest.py where
    `traversals` covers a single date. Pass `extra_group_columns=()` to roll every date in
    `traversals` together instead, for the weekly/monthly trend rollups in trends.py.
    """
    traversals = traversals.copy()
    traversals["time_band"] = assign_time_band(traversals.depart_seconds)
    traversals = traversals.dropna(subset=["time_band"])

    group_key = SEGMENT_KEY + list(extra_group_columns) + ["time_band"]
    grouped = traversals.groupby(group_key, sort=False)

    aggregation = {
        "n_traversals": ("total_time_seconds", "size"),
        "n_interpolated": ("is_interpolated", "sum"),
        "segment_length_m": ("segment_length_m", "median"),
        "median_dwell_seconds": ("dwell_seconds", "median"),
    }
    for percentile in PERCENTILES:
        for measure in ("total_time_seconds", "moving_time_seconds"):
            aggregation[f"p{percentile}_{measure}"] = (
                measure,
                lambda series, q=percentile: float(np.percentile(series, q)),
            )

    aggregated = grouped.agg(**aggregation).reset_index()

    # Speed comes from the matching time percentile, so p50 speed is the median trip's
    # speed. Note the ordering inverts: the p90 *time* is the slow tail, hence a low speed.
    for percentile in PERCENTILES:
        aggregated[f"p{percentile}_speed_mph"] = (
            aggregated.segment_length_m / aggregated[f"p{percentile}_total_time_seconds"] * METERS_PER_SECOND_TO_MPH
        )
        aggregated[f"p{percentile}_moving_speed_mph"] = (
            aggregated.segment_length_m / aggregated[f"p{percentile}_moving_time_seconds"] * METERS_PER_SECOND_TO_MPH
        )

    logger.info(f"Aggregated to {len(aggregated)} ({', '.join(group_key)}) rows")
    return aggregated


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
