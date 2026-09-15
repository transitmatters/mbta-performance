"""Turn GTFS route patterns into located stops and geometric segments.

For each route pattern we take a representative trip, project that trip's stops onto the
pattern's shape, and emit both the stop positions (used to interpolate missing times) and
the consecutive-stop segments (the unit a speed is reported for).
"""

import logging
from datetime import date

import numpy as np
import pandas as pd

from .gtfs_geo import load_gtfs_slice, project_stops_onto_shape, slice_shape, to_local_xy, _cumulative_distance

logger = logging.getLogger(__name__)


class PatternGeometry:
    """Located stops and segment geometry for every route pattern on a service date."""

    def __init__(self, stop_positions: pd.DataFrame, segments: pd.DataFrame, diagnostics: dict):
        # (route_pattern_id, stop_sequence) -> distance along shape, in metres.
        self.stop_positions = stop_positions
        # One row per (route_pattern_id, from_stop_id, to_stop_id) with geometry.
        self.segments = segments
        self.diagnostics = diagnostics


def build_pattern_geometry(service_date: date, route_pattern_ids: set[str] | None = None) -> PatternGeometry:
    """Locate stops along shapes for the patterns running on a service date."""
    gtfs = load_gtfs_slice(service_date)
    trips, stop_times, stops, shapes = gtfs["trips"], gtfs["stop_times"], gtfs["stops"], gtfs["shapes"]

    trips = trips.dropna(subset=["route_pattern_id", "shape_id"])
    if route_pattern_ids is not None:
        trips = trips[trips.route_pattern_id.isin(route_pattern_ids)]

    # One representative trip per pattern is enough: by definition every trip on a pattern
    # visits the same stops in the same order.
    representatives = trips.sort_values("trip_id").drop_duplicates("route_pattern_id")
    logger.info(f"Building geometry for {len(representatives)} route patterns")

    stop_lookup = stops.set_index("stop_id")[["stop_lat", "stop_lon", "stop_name"]]
    stop_times_by_trip = {
        trip_id: group.sort_values("stop_sequence")
        for trip_id, group in stop_times[stop_times.trip_id.isin(representatives.trip_id)].groupby("trip_id")
    }
    shapes_by_id = {shape_id: group for shape_id, group in shapes.groupby("shape_id")}

    position_rows = []
    segment_rows = []
    skipped: dict[str, int] = {}
    max_offsets = []

    def skip(reason: str) -> None:
        skipped[reason] = skipped.get(reason, 0) + 1

    for representative in representatives.itertuples(index=False):
        trip_stop_times = stop_times_by_trip.get(representative.trip_id)
        shape = shapes_by_id.get(representative.shape_id)
        if trip_stop_times is None or shape is None or len(shape) < 2:
            skip("missing stop_times or shape")
            continue

        located = trip_stop_times.join(stop_lookup, on="stop_id")
        located = located.dropna(subset=["stop_lat", "stop_lon"])
        if len(located) < 2:
            skip("fewer than two locatable stops")
            continue

        shape_lonlat = np.column_stack([shape.shape_pt_lon.to_numpy(), shape.shape_pt_lat.to_numpy()])
        shape_xy = to_local_xy(shape.shape_pt_lat.to_numpy(), shape.shape_pt_lon.to_numpy())
        cumulative = _cumulative_distance(shape_xy)

        stops_xy = to_local_xy(located.stop_lat.to_numpy(), located.stop_lon.to_numpy())
        distances, offsets = project_stops_onto_shape(stops_xy, shape_xy)
        max_offsets.append(float(offsets.max()))

        pattern_id = representative.route_pattern_id
        stop_ids = located.stop_id.to_numpy()
        stop_names = located.stop_name.to_numpy()
        sequences = located.stop_sequence.to_numpy()

        for stop_id, sequence, distance in zip(stop_ids, sequences, distances):
            position_rows.append(
                {
                    "route_pattern_id": pattern_id,
                    "stop_id": stop_id,
                    "stop_sequence": int(sequence),
                    "shape_distance_m": float(distance),
                }
            )

        for i in range(len(located) - 1):
            length = float(distances[i + 1] - distances[i])
            if length <= 0:
                skip("non-advancing segment")
                continue
            coordinates = slice_shape(shape_lonlat, cumulative, distances[i], distances[i + 1])
            if len(coordinates) < 2:
                skip("degenerate geometry")
                continue
            segment_rows.append(
                {
                    "route_pattern_id": pattern_id,
                    "route_id": representative.route_id,
                    "direction_id": int(representative.direction_id),
                    "from_stop_id": stop_ids[i],
                    "to_stop_id": stop_ids[i + 1],
                    "from_stop_name": stop_names[i],
                    "to_stop_name": stop_names[i + 1],
                    "from_stop_sequence": int(sequences[i]),
                    "segment_length_m": length,
                    "coordinates": coordinates,
                }
            )

    diagnostics = {
        "patterns_requested": len(representatives),
        "patterns_with_geometry": len({row["route_pattern_id"] for row in segment_rows}),
        "skipped": skipped,
        "max_stop_offset_m": max(max_offsets) if max_offsets else None,
        "median_max_stop_offset_m": float(np.median(max_offsets)) if max_offsets else None,
    }
    logger.info(f"Pattern geometry diagnostics: {diagnostics}")

    return PatternGeometry(
        stop_positions=pd.DataFrame(position_rows),
        segments=pd.DataFrame(segment_rows),
        diagnostics=diagnostics,
    )
