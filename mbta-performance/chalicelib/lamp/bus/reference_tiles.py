"""Build the bus stop / rapid transit station reference tileset for the speed map.

A context layer, drawn under the speed segments so a viewer can tell where in the system
they're looking: which stop a slow segment ends at, which T station is nearby. Built
separately from the segment tiles (pmtiles.py) because stops only change when the GTFS feed
does -- rebuild on a feed change or on a slow schedule, not with every daily ingest.

One PMTiles file, two layers:

- `stations`: rapid transit parent stations (subway, light rail, Mattapan), with the lines
  and routes serving each, so the frontend can colour them.
- `bus_stops`: bus stop poles, with the routes serving each.

Requires tippecanoe on PATH, same as pmtiles.py.
"""

import json
import logging
import subprocess
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from ... import s3
from ...date import get_current_service_date
from .constants import REFERENCE_PMTILES_KEY, S3_BUCKET
from .gtfs_geo import read_gtfs_archive_file
from .pmtiles import TIPPECANOE_BINARY, _require_tippecanoe

logger = logging.getLogger(__name__)

# Match the frontend's source-layer names for this tileset.
STATIONS_LAYER = "stations"
BUS_STOPS_LAYER = "bus_stops"

# GTFS route_type / vehicle_type values.
LIGHT_RAIL = 0
SUBWAY = 1
BUS = 3
RAPID_TRANSIT_TYPES = (LIGHT_RAIL, SUBWAY)

# MBTA's `listed_route` extension: 1 means "leave out of public route lists". That covers the
# ~200 Rail Replacement Bus shuttles (route_type 3, so they'd otherwise read as bus routes)
# and a few supplemental routes -- none of which belong on a public reference layer.
UNLISTED_ROUTE = 1

# GTFS location_type values.
STOP_OR_PLATFORM = 0
STATION = 1

# Stations go down to the segment tiles' MINIMUM_ZOOM so they're present in the frontend's
# zoomed-out fallback view too. ~7k bus stops would only be clutter until you're looking at
# a neighbourhood, so they start much later. Enforced per feature via tippecanoe's
# `tippecanoe.minzoom` extension, since it has no per-layer minzoom option.
STATION_MINIMUM_ZOOM = 4
BUS_STOP_MINIMUM_ZOOM = 11
# Points gain nothing from deeper tiles -- MapLibre overzooms the last level.
MAXIMUM_ZOOM = 14


def _routes_by_stop(stop_times: pd.DataFrame, trips: pd.DataFrame, routes: pd.DataFrame) -> pd.DataFrame:
    """Distinct (stop_id, route_id) pairs for listed routes, with type, sort order and line."""
    routes = routes[routes.listed_route != UNLISTED_ROUTE]
    served = stop_times[["trip_id", "stop_id"]].merge(trips[["trip_id", "route_id"]], on="trip_id")
    served = served[["stop_id", "route_id"]].drop_duplicates()
    return served.merge(routes[["route_id", "route_type", "route_sort_order", "line_id"]], on="route_id")


def _join_sorted(pairs: pd.DataFrame, key: str, column: str) -> pd.Series:
    """Comma-join `column` per `key`, in GTFS route_sort_order.

    A string rather than a list: tippecanoe would store a list as a JSON-encoded string
    anyway, and a plain comma-joined one is simpler to split or `in`-match in a MapLibre
    expression.
    """
    ordered = pairs.sort_values("route_sort_order").drop_duplicates([key, column])
    return ordered.groupby(key)[column].agg(",".join)


def select_reference_stops(
    stops: pd.DataFrame, trips: pd.DataFrame, stop_times: pd.DataFrame, routes: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pick out rapid transit stations and served bus stops from one GTFS slice.

    Returns (stations, bus_stops). Both carry stop_id, stop_name, stop_lat, stop_lon and
    `routes`; stations also carry `lines` (e.g. "Green,Red" -- Green-B through Green-E
    collapse into one "Green").

    Bus stops without any trip on a listed route are dropped: the MBTA feed keeps retired and
    seasonal poles around, and a reference layer showing a stop nothing serves would mislead.
    The trips here are everything in the feed version, not just one day's service, so a
    weekend or shutdown date doesn't hide anything.
    """
    stops = stops.drop_duplicates("stop_id")
    served = _routes_by_stop(stop_times, trips.drop_duplicates("trip_id"), routes.drop_duplicates("route_id"))

    # Parent stations carry no vehicle_type of their own in the MBTA feed; their platforms
    # do. Roll platform service up to the parent.
    rapid = served[served.route_type.isin(RAPID_TRANSIT_TYPES)]
    rapid = rapid.merge(stops[["stop_id", "parent_station"]], on="stop_id").dropna(subset=["parent_station"])
    rapid = rapid.assign(line=rapid.line_id.str.removeprefix("line-"))
    stations = stops[(stops.location_type == STATION) & stops.stop_id.isin(rapid.parent_station)].copy()
    stations["lines"] = stations.stop_id.map(_join_sorted(rapid, "parent_station", "line"))
    stations["routes"] = stations.stop_id.map(_join_sorted(rapid, "parent_station", "route_id"))

    bus = served[served.route_type == BUS]
    bus_stops = stops[(stops.location_type == STOP_OR_PLATFORM) & (stops.vehicle_type == BUS)].copy()
    bus_stops["routes"] = bus_stops.stop_id.map(_join_sorted(bus, "stop_id", "route_id"))
    bus_stops = bus_stops.dropna(subset=["routes"])

    station_columns = ["stop_id", "stop_name", "stop_lat", "stop_lon", "lines", "routes"]
    bus_columns = ["stop_id", "stop_name", "stop_lat", "stop_lon", "routes"]
    return stations[station_columns].reset_index(drop=True), bus_stops[bus_columns].reset_index(drop=True)


def load_reference_stops(service_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read the GTFS feed active on `service_date` and select its stations and bus stops."""
    stops = read_gtfs_archive_file(
        "stops",
        service_date,
        ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "vehicle_type", "parent_station"],
    )
    routes = read_gtfs_archive_file(
        "routes", service_date, ["route_id", "route_type", "route_sort_order", "line_id", "listed_route"]
    )
    trips = read_gtfs_archive_file("trips", service_date, ["trip_id", "route_id"])
    stop_times = read_gtfs_archive_file("stop_times", service_date, ["trip_id", "stop_id"])

    stations, bus_stops = select_reference_stops(stops, trips, stop_times, routes)
    logger.info(f"Reference layer for {service_date}: {len(stations)} stations, {len(bus_stops)} bus stops")
    return stations, bus_stops


def _write_geojsonseq(frame: pd.DataFrame, properties: list[str], minimum_zoom: int, path: Path) -> None:
    with path.open("w") as f:
        for row in frame.itertuples(index=False):
            feature = {
                "type": "Feature",
                "tippecanoe": {"minzoom": minimum_zoom},
                "geometry": {"type": "Point", "coordinates": [float(row.stop_lon), float(row.stop_lat)]},
                "properties": {column: getattr(row, column) for column in properties},
            }
            f.write(json.dumps(feature) + "\n")


def build_reference_pmtiles_bytes(stations: pd.DataFrame, bus_stops: pd.DataFrame) -> bytes:
    """Tile stations and bus stops into one two-layer PMTiles tileset, in memory."""
    _require_tippecanoe()
    with tempfile.TemporaryDirectory() as tmp_dir:
        stations_path = Path(tmp_dir) / "stations.geojsons"
        bus_stops_path = Path(tmp_dir) / "bus_stops.geojsons"
        output_path = Path(tmp_dir) / "stops.pmtiles"
        _write_geojsonseq(stations, ["stop_id", "stop_name", "lines", "routes"], STATION_MINIMUM_ZOOM, stations_path)
        _write_geojsonseq(bus_stops, ["stop_id", "stop_name", "routes"], BUS_STOP_MINIMUM_ZOOM, bus_stops_path)

        command = [
            TIPPECANOE_BINARY,
            "--force",
            "--quiet",
            "--output",
            str(output_path),
            "--name",
            "MBTA Stops and Stations",
            "--minimum-zoom",
            str(STATION_MINIMUM_ZOOM),
            "--maximum-zoom",
            str(MAXIMUM_ZOOM),
            # A reference layer that silently drops stops is worse than none -- keep every
            # point at every zoom it's visible at. Only ~7k points, so tiles stay small.
            "--drop-rate",
            "1",
            "--named-layer",
            f"{STATIONS_LAYER}:{stations_path}",
            "--named-layer",
            f"{BUS_STOPS_LAYER}:{bus_stops_path}",
        ]
        logger.info(f"Running tippecanoe for {len(stations)} stations and {len(bus_stops)} bus stops")
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"tippecanoe failed ({result.returncode}): {result.stderr.strip()}")
        return output_path.read_bytes()


def generate_reference_tiles(service_date: date, out: str | None = None, upload: bool = False) -> bytes:
    """Build the reference tileset from the feed active on `service_date`.

    Writes it to `out` and/or s3://S3_BUCKET/REFERENCE_PMTILES_KEY. Re-running overwrites
    the S3 object in place.
    """
    stations, bus_stops = load_reference_stops(service_date)
    data = build_reference_pmtiles_bytes(stations, bus_stops)
    if out:
        Path(out).write_bytes(data)
        logger.info(f"Wrote reference PMTiles to {out}")
    if upload:
        logger.info(
            f"Uploading reference PMTiles ({len(data) / 1048576:.2f}MB) to s3://{S3_BUCKET}/{REFERENCE_PMTILES_KEY}"
        )
        s3.upload_pmtiles(S3_BUCKET, REFERENCE_PMTILES_KEY, data)
    return data


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Generate the bus stop / T station reference PMTiles.")
    parser.add_argument(
        "--date", type=date.fromisoformat, help="Use the GTFS feed active on this date (YYYY-MM-DD), default yesterday"
    )
    parser.add_argument("--out", default="stops.pmtiles", help="Output PMTiles path")
    parser.add_argument(
        "--upload", action="store_true", help=f"Also publish to s3://{S3_BUCKET}/{REFERENCE_PMTILES_KEY}"
    )
    arguments = parser.parse_args()

    target = arguments.date or (get_current_service_date() - timedelta(days=1))
    generate_reference_tiles(target, arguments.out, upload=arguments.upload)
