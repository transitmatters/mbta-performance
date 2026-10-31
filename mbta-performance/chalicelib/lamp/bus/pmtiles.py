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

from .constants import ALL_DAY_BAND
from .geoparquet import _to_geodataframe

logger = logging.getLogger(__name__)

TIPPECANOE_BINARY = "tippecanoe"

# Matches modules/busspeedmap/constants.ts's PMTILES_SOURCE_LAYER on the frontend.
LAYER_NAME = "segments"

# The frontend falls back to a whole-network view (about zoom 9, depending on the map's size)
# to find a route's geometry when the current view has nothing loaded (see BusSpeedMapView.tsx),
# on the assumption that every route is present there -- so MINIMUM_ZOOM must stay below it.
MINIMUM_ZOOM = 4
MAXIMUM_ZOOM = 16

# tippecanoe's default is 500KB. --drop-densest-as-needed (see _run_tippecanoe) thins a whole
# zoom level whenever a single tile in it overflows, so the lowest zoom that still has every
# segment is always one whose densest tile only just fits. shared_segments.py's pooled rows
# make the densest tile at a zoom up to 25% bigger, since shared corridors cluster downtown;
# at 500KB that tipped the next zoom up into thinning (2026-W44's densest zoom 10 tile grew
# from 479KB to 530KB, and zoom 10 went from every segment to 15% of them). Raised by that
# 25%, every zoom that fitted before still does -- and sometimes one more now fits too.
MAXIMUM_TILE_BYTES = 625_000

# What modules/busspeedmap/types.ts's BusSpeedSegmentProperties reads off a tile feature.
# Selected explicitly, rather than handing tippecanoe the full GeoParquet schema, so a column
# like p90_speed_mph or moving_speed_mph can never end up on the map by accident.
#
# day_type (business_day / weekend_or_holiday, see day_type.py) is only present on the
# weekly/monthly trend rollups in trends.py -- a single day's file is already wholly one or
# the other, so ingest.py's daily frames never carry the column. Listed here anyway: only
# columns actually present in a given frame reach tippecanoe (see build_pmtiles_bytes), so
# this is a no-op for daily and picks the column up automatically for trends.
#
# segment_length_m and p50_total_time_seconds are the two numbers p50_speed_mph is computed
# from, shown in the map's hover popup as the segment's distance and median travel time.
#
# shared_routes marks the rows on a stop pair several routes share, so the map can draw the
# pooled row in their place (see shared_segments.py). Null everywhere else, which tippecanoe
# leaves off the feature entirely.
TILE_PROPERTIES = [
    "route_id",
    "direction_id",
    "from_stop_name",
    "to_stop_name",
    "time_band",
    "day_type",
    "p50_speed_mph",
    "n_traversals",
    "n_interpolated",
    "segment_length_m",
    "p50_total_time_seconds",
    "shared_routes",
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
        # area like downtown Boston has many overlapping near-duplicate lines -- enough to
        # blow past the tile size limit (MAXIMUM_TILE_BYTES) and fail outright without this.
        # Verified against a real day (2026-09-17, 52.8k features) that every route still has
        # geometry at MINIMUM_ZOOM, which is what the frontend's fallback view relies on. The
        # dropping is real, though: at zooms 4-10 only 8-14% of band features survive on a
        # daily file (5-7% through zoom 11 on a weekly/monthly one), since exact duplicates
        # of a geometry already kept are the "densest" features and go first. That's also
        # why all_day rows get an archive of their own -- see split_all_day.
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "--maximum-tile-bytes",
        str(MAXIMUM_TILE_BYTES),
        str(geojson_path),
    ]
    logger.info(f"Running tippecanoe on {geojson_path} -> {output_path}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"tippecanoe failed ({result.returncode}): {result.stderr.strip()}")


def split_all_day(frame):
    """Split an aggregated frame into (time band rows, ALL_DAY_BAND rows), one per archive.

    all_day rows share their geometry exactly with a band row of the same segment, so in a
    single archive --drop-densest-as-needed discards them before anything else. Measured on
    2026-09-17, one archive left no all_day feature at zooms 4-11 -- the map's default view
    is zoom 11 -- and pushed zoom 11 over the tile limit, cutting its band features by 86%.
    Built separately, the band archive is unchanged and the all_day archive keeps
    essentially every segment from zoom 8 up (~60% at zoom 4), with every route present at
    MINIMUM_ZOOM -- checked on a real day, week, and month.
    """
    is_all_day = frame.time_band == ALL_DAY_BAND
    return frame[~is_all_day], frame[is_all_day]


def _round_for_tiles(frame):
    """Round to what the dashboard shows: speed to 0.1 mph, length and time to whole metres and
    seconds, as leaderboard.py does.

    Full-precision floats make nearly every value in a tile unique, and each one a full 8-byte
    double. Measured on 2026-10-28's band archive (11.4MB before length and time were added):
    they'd add 24% as floats and 10% as integers, while rounding speed saves 12%, so with both
    the archive comes out 5% smaller than before.
    """
    return frame.assign(
        p50_speed_mph=frame.p50_speed_mph.round(1),
        segment_length_m=frame.segment_length_m.round().astype(int),
        p50_total_time_seconds=frame.p50_total_time_seconds.round().astype(int),
    )


def build_pmtiles_bytes(frame, coordinates_column: str = "coordinates") -> bytes:
    """Serialise a DataFrame with a coordinate-list column to a PMTiles tileset, in memory.

    Only `TILE_PROPERTIES` (plus geometry) reach tippecanoe -- everything else in `frame`
    (dwell, the p90 and moving-speed columns) is dropped first.
    """
    _require_tippecanoe()
    columns = [coordinates_column] + [column for column in TILE_PROPERTIES if column in frame.columns]
    geo_frame = _to_geodataframe(_round_for_tiles(frame[columns]), coordinates_column)

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
