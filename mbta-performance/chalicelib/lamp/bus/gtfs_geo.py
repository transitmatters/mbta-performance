"""GTFS geometry for bus speed segments.

Two jobs live here:

1. Slice the LAMP GTFS archive down to the feed that was active on a service date.
2. Locate every stop of a route pattern along that pattern's shape, so consecutive stops
   define a segment with a real road-following geometry and a real length.

Step 2 is necessary because the MBTA feed's `stop_times.shape_dist_traveled` column is
present but entirely null, and `shapes.shape_dist_traveled` is null as well. Distance
along the shape has to be derived from the geometry itself.
"""

import logging
from datetime import date

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pyproj import Transformer
from shapely.geometry import LineString
from shapely.ops import substring

from .constants import GTFS_ARCHIVE_URL_TEMPLATE
from .remote_parquet import HttpRangeFile

logger = logging.getLogger(__name__)

# NAD83 / Massachusetts Mainland, in metres -- accurate across the whole feed's service
# area, unlike an equirectangular approximation about a single reference point.
_LONLAT_CRS = "EPSG:4326"
_PROJECTED_CRS = "EPSG:26986"
_TO_XY = Transformer.from_crs(_LONLAT_CRS, _PROJECTED_CRS, always_xy=True)
_TO_LONLAT = Transformer.from_crs(_PROJECTED_CRS, _LONLAT_CRS, always_xy=True)


def to_local_xy(latitude: np.ndarray, longitude: np.ndarray) -> np.ndarray:
    """Project lat/lon to local metres. Returns an (n, 2) array of (x, y)."""
    x, y = _TO_XY.transform(longitude, latitude)
    return np.column_stack([x, y])


def to_lonlat(xy: np.ndarray) -> np.ndarray:
    """Inverse of `to_local_xy`. Returns an (n, 2) array of (longitude, latitude)."""
    longitude, latitude = _TO_LONLAT.transform(xy[:, 0], xy[:, 1])
    return np.column_stack([longitude, latitude])


def _dateint(service_date: date) -> int:
    return int(service_date.strftime("%Y%m%d"))


def read_gtfs_archive_file(filename: str, service_date: date, columns: list[str]) -> pd.DataFrame:
    """Read one GTFS archive parquet, keeping only rows active on `service_date`.

    Row groups are pre-filtered on their gtfs_active_date/gtfs_end_date statistics so we
    range-read only the groups that can contain matching rows.
    """
    url = GTFS_ARCHIVE_URL_TEMPLATE.format(year=service_date.year, filename=filename)
    dateint = _dateint(service_date)
    parquet_file = pq.ParquetFile(HttpRangeFile(url))

    schema = parquet_file.schema_arrow
    active_index = schema.get_field_index("gtfs_active_date")
    end_index = schema.get_field_index("gtfs_end_date")

    wanted = list(dict.fromkeys(columns + ["gtfs_active_date", "gtfs_end_date"]))
    groups = []
    for group in range(parquet_file.metadata.num_row_groups):
        row_group = parquet_file.metadata.row_group(group)
        active_stats = row_group.column(active_index).statistics
        end_stats = row_group.column(end_index).statistics
        # Keep the group if it *could* hold a row spanning our date.
        if active_stats is not None and active_stats.min > dateint:
            continue
        if end_stats is not None and end_stats.max < dateint:
            continue
        groups.append(group)

    logger.info(f"Reading GTFS {filename}: {len(groups)}/{parquet_file.metadata.num_row_groups} row groups")
    if not groups:
        return pd.DataFrame(columns=wanted)

    frame = parquet_file.read_row_groups(groups, columns=wanted).to_pandas()
    frame = frame[(frame.gtfs_active_date <= dateint) & (frame.gtfs_end_date >= dateint)]
    return frame.drop(columns=["gtfs_active_date", "gtfs_end_date"])


def load_gtfs_slice(service_date: date) -> dict[str, pd.DataFrame]:
    """Load the trips, stop_times, stops and shapes active on a service date."""
    logger.info(f"Loading GTFS archive slice for {service_date}")
    trips = read_gtfs_archive_file(
        "trips", service_date, ["trip_id", "route_id", "direction_id", "shape_id", "route_pattern_id"]
    )
    stop_times = read_gtfs_archive_file("stop_times", service_date, ["trip_id", "stop_id", "stop_sequence"])
    stops = read_gtfs_archive_file("stops", service_date, ["stop_id", "stop_name", "stop_lat", "stop_lon"])
    shapes = read_gtfs_archive_file(
        "shapes", service_date, ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"]
    )

    # A feed can carry more than one row per stop_id across overlapping validity windows.
    stops = stops.drop_duplicates("stop_id")
    trips = trips.drop_duplicates("trip_id")
    shapes = shapes.drop_duplicates(["shape_id", "shape_pt_sequence"]).sort_values(["shape_id", "shape_pt_sequence"])

    logger.info(
        f"GTFS slice: {len(trips)} trips, {len(stop_times)} stop_times, {len(stops)} stops, "
        f"{shapes.shape_id.nunique()} shapes"
    )
    return {"trips": trips, "stop_times": stop_times, "stops": stops, "shapes": shapes}


def _cumulative_distance(points_xy: np.ndarray) -> np.ndarray:
    """Cumulative distance in metres at each vertex of a polyline."""
    steps = np.hypot(np.diff(points_xy[:, 0]), np.diff(points_xy[:, 1]))
    return np.concatenate([[0.0], np.cumsum(steps)])


def _candidate_passes(
    point: np.ndarray,
    start: np.ndarray,
    delta: np.ndarray,
    segment_length_squared: np.ndarray,
    segment_length: np.ndarray,
    cumulative: np.ndarray,
    tolerance: float,
) -> list[tuple[float, float]]:
    """Every distinct place along the shape that passes near a stop.

    A shape that loops or doubles back runs past the same stop more than once. Each such
    pass is a candidate position for the stop; picking the right one is left to the DP in
    `project_stops_onto_shape`.
    """
    offset_from_start = point - start
    t = np.clip((offset_from_start * delta).sum(axis=1) / segment_length_squared, 0, 1)
    closest_point = start + t[:, None] * delta
    distance = np.sqrt(((point - closest_point) ** 2).sum(axis=1))

    best = float(distance.min())
    threshold = max(best * 1.5 + 5.0, tolerance)
    near = distance <= threshold
    if not near.any():
        near = distance == distance.min()

    # Split the near-segments into contiguous runs; each run is one pass of the shape.
    indices = np.flatnonzero(near)
    breaks = np.flatnonzero(np.diff(indices) > 1)
    runs = np.split(indices, breaks + 1)

    candidates = []
    for run in runs:
        local = run[int(distance[run].argmin())]
        along = cumulative[local] + t[local] * segment_length[local]
        candidates.append((float(along), float(distance[local])))
    return candidates


def project_stops_onto_shape(
    stops_xy: np.ndarray, shape_xy: np.ndarray, tolerance: float = 50.0
) -> tuple[np.ndarray, np.ndarray]:
    """Locate ordered stops along a shape, in metres from the shape's start.

    Returns (distance_along, offset), where `offset` is how far the stop sits from the
    shape. Stops must be in travel order and the returned distances strictly increase.

    On a loop or out-and-back shape a stop lies near more than one part of the geometry, so
    the nearest match is not always the right one -- and choosing greedily lets a single bad
    match drag every later stop with it. Instead every candidate pass is enumerated and a
    dynamic program picks the cheapest strictly-increasing assignment, trading off how far
    each stop sits from the shape against how much the implied along-shape spacing departs
    from the straight-line distance between the stops.
    """
    start = shape_xy[:-1]
    delta = shape_xy[1:] - start
    segment_length_squared = (delta**2).sum(axis=1)
    segment_length_squared[segment_length_squared == 0] = 1e-9
    segment_length = np.sqrt(segment_length_squared)
    cumulative = _cumulative_distance(shape_xy)

    candidates = [
        _candidate_passes(point, start, delta, segment_length_squared, segment_length, cumulative, tolerance)
        for point in stops_xy
    ]

    # Fast path: one unambiguous pass per stop, already advancing.
    if all(len(options) == 1 for options in candidates):
        distances = np.array([options[0][0] for options in candidates])
        if np.all(np.diff(distances) > 0):
            return distances, np.array([options[0][1] for options in candidates])

    straight_line = np.hypot(np.diff(stops_xy[:, 0]), np.diff(stops_xy[:, 1]))

    # DP over (stop, candidate). Cost of a candidate is its squared offset; the cost of a
    # transition is how far the along-shape advance strays from the stops' straight-line
    # separation, which is what makes a spurious 4km jump lose to the correct pass.
    best_cost = [np.array([offset**2 for _, offset in candidates[0]], dtype="float64")]
    backpointer: list[np.ndarray] = [np.full(len(candidates[0]), -1, dtype=int)]

    for stop_index in range(1, len(candidates)):
        previous_along = np.array([along for along, _ in candidates[stop_index - 1]])
        current = candidates[stop_index]
        costs = np.empty(len(current))
        pointers = np.empty(len(current), dtype=int)
        expected = straight_line[stop_index - 1]

        for option_index, (along, offset) in enumerate(current):
            advance = along - previous_along
            feasible = advance > 0
            if not feasible.any():
                # No strictly-increasing predecessor; keep the chain alive on the closest one.
                transition = np.abs(advance - expected) + 1e6
                feasible = np.ones(len(previous_along), dtype=bool)
            else:
                transition = np.where(feasible, np.abs(advance - expected), np.inf)
            total = best_cost[stop_index - 1] + transition + offset**2
            total = np.where(feasible, total, np.inf)
            choice = int(np.argmin(total))
            costs[option_index] = total[choice]
            pointers[option_index] = choice

        best_cost.append(costs)
        backpointer.append(pointers)

    chosen = [int(np.argmin(best_cost[-1]))]
    for stop_index in range(len(candidates) - 1, 0, -1):
        chosen.append(int(backpointer[stop_index][chosen[-1]]))
    chosen.reverse()

    distances = np.array([candidates[i][choice][0] for i, choice in enumerate(chosen)])
    offsets = np.array([candidates[i][choice][1] for i, choice in enumerate(chosen)])
    return distances, offsets


def cut_segment(line_xy: LineString, start_m: float, end_m: float) -> LineString | None:
    """Cut `line_xy` between two distances along its own length.

    `line_xy` must be in the same projected metres as `start_m`/`end_m` (e.g. from
    `to_local_xy`). Returns None if the cut is non-advancing or degenerate -- callers should
    reproject a real result back to lon/lat with `to_lonlat` before storing it, since GTFS
    shapes and GeoParquet output are both lon/lat.
    """
    if end_m <= start_m:
        return None
    cut = substring(line_xy, start_m, end_m)
    if not isinstance(cut, LineString) or cut.is_empty or len(cut.coords) < 2:
        return None
    return cut
