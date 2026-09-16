"""Build the PMTiles vector tileset the live map reads, via tippecanoe.

GeoParquet (geoparquet.py) is for analysts -- QGIS, DuckDB spatial, GeoPandas, Felt. The
dashboard map instead reads PMTiles byte-range by byte-range straight from the browser
through MapLibre's `pmtiles://` protocol, which needs an actual tiled pyramid rather than one
big file at every zoom.

tippecanoe is a system binary, not a Python package -- install it separately (apt/brew, or
bake it into the container image this pipeline runs in). It isn't in any dependency group
here because there's nothing to add to `pyproject.toml`.
"""

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from .geoparquet import _to_geodataframe

logger = logging.getLogger(__name__)

TIPPECANOE_BINARY = "tippecanoe"

# Matches modules/busspeedmap/constants.ts's PMTILES_SOURCE_LAYER on the frontend.
LAYER_NAME = "segments"

# The frontend falls back to this zoom to find a route's geometry when the current view has
# nothing loaded (see BusSpeedMapView.tsx), on the assumption that every route is present
# there -- so MINIMUM_ZOOM must stay at or below it.
MINIMUM_ZOOM = 4
MAXIMUM_ZOOM = 16

# What modules/busspeedmap/types.ts's BusSpeedSegmentProperties reads off a tile feature.
# Selected explicitly, rather than handing tippecanoe the full GeoParquet schema, so a column
# like p90_speed_mph or moving_speed_mph can never end up on the map by accident.
TILE_PROPERTIES = [
    "route_id",
    "direction_id",
    "from_stop_name",
    "to_stop_name",
    "time_band",
    "p50_speed_mph",
    "n_traversals",
    "n_interpolated",
]


def _require_tippecanoe() -> None:
    if shutil.which(TIPPECANOE_BINARY) is None:
        raise RuntimeError(
            f"'{TIPPECANOE_BINARY}' is not on PATH. It's a system binary, not a Python "
            "dependency -- install it (apt/brew, or in the container image) before calling "
            "this."
        )


def _run_tippecanoe(geojson_path: Path, output_path: Path) -> None:
    command = [
        TIPPECANOE_BINARY,
        "--force",
        "--quiet",
        "--output",
        str(output_path),
        "--layer",
        LAYER_NAME,
        "--name",
        "Bus Speed Segments",
        "--minimum-zoom",
        str(MINIMUM_ZOOM),
        "--maximum-zoom",
        str(MAXIMUM_ZOOM),
        # Geometry is repeated per time band and direction (see the bus README), so a dense
        # area like downtown Boston has many overlapping near-duplicate lines at high zoom --
        # enough to blow past tippecanoe's default 500KB tile limit and fail outright without
        # this. Verified against a real day (51k features) that this only drops features
        # above MINIMUM_ZOOM: every route still has geometry at MINIMUM_ZOOM, which is what
        # the frontend's fallback view relies on.
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        str(geojson_path),
    ]
    logger.info(f"Running tippecanoe on {geojson_path} -> {output_path}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"tippecanoe failed ({result.returncode}): {result.stderr.strip()}")


def build_pmtiles_bytes(frame, coordinates_column: str = "coordinates") -> bytes:
    """Serialise a DataFrame with a coordinate-list column to a PMTiles tileset, in memory.

    Only `TILE_PROPERTIES` (plus geometry) reach tippecanoe -- everything else in `frame`
    (segment length, dwell, the p90 and moving-speed columns) is dropped first.
    """
    _require_tippecanoe()
    columns = [coordinates_column] + [column for column in TILE_PROPERTIES if column in frame.columns]
    geo_frame = _to_geodataframe(frame[columns], coordinates_column)

    with tempfile.TemporaryDirectory() as tmp_dir:
        geojson_path = Path(tmp_dir) / "segments.geojsons"
        pmtiles_path = Path(tmp_dir) / "segments.pmtiles"
        geo_frame.to_file(geojson_path, driver="GeoJSONSeq")
        _run_tippecanoe(geojson_path, pmtiles_path)
        return pmtiles_path.read_bytes()


def write_pmtiles(frame, path: str, coordinates_column: str = "coordinates") -> str:
    """Write a DataFrame with a coordinate-list column out as a PMTiles tileset."""
    data = build_pmtiles_bytes(frame, coordinates_column)
    Path(path).write_bytes(data)
    logger.info(f"Wrote PMTiles to {path}")
    return path
