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
#
# median_dwell_seconds and from_stop_id grow the densest tile another 13-14%, which at 625KB did
# the same to a day's lowest complete zoom: 2026-10-29's densest zoom 9 tile went from 614KB to
# 697KB, and zoom 9 from every segment to 15% of them. It's the most zoomed-out view the map has,
# so this leaves room for a busier day than that one, not just the 14%.
#
# The speed range (p10_speed_mph and p90_speed_mph) takes that densest tile to 771KB, and a week's
# zoom 10 from 600KB to 729KB, as more of a longer period's rows have the trips for one. The same
# 7% or so of room again.
MAXIMUM_TILE_BYTES = 825_000

# Median dwells pile up around 5.5s and 11s, which looks like the spacing of the vehicle positions
# behind LAMP's stop times, so a dwell is no better than this. Rounding to it rather than to the
# second also takes 1-3 points off what the column adds to an archive.
DWELL_ROUNDING_SECONDS = 5

# The fewest trips the map's "8 in 10 trips" range is shown for: it means nothing with fewer. Only
# 24% of a day's band rows have this many, so leaving it off the rest also saves most of its bytes
# where tiles are tightest.
SPEED_RANGE_MINIMUM_TRAVERSALS = 10

# What modules/busspeedmap/types.ts's BusSpeedSegmentProperties reads off a tile feature.
# Selected explicitly, rather than handing tippecanoe the full GeoParquet schema, so a column
# like moving_speed_mph can never end up on the map by accident. That one would mislead: LAMP's
# stop times leave the moving clock about 86m short of each segment, going by where it hits zero,
# so on 2026-10-28 segments of 100-150m read a median 41 mph, 63% of them over 35 mph, and 30% of
# the segments under 5 mph read over 25 mph.
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
#
# median_dwell_seconds is how long buses stood at the segment's first stop, which p50_speed_mph
# already counts, shown in the hover popups for the segment and for the stop. from_stop_id is
# how the map finds a stop's segments for that second popup; the name won't do, since a name can
# cover both sides of a street, or stops miles apart (Massachusetts Ave @ Albany St is both).
#
# p10_speed_mph and p90_speed_mph are the fastest and slowest tenth of trips, on the arrival to
# arrival clock p50_speed_mph uses, for the range around it in the hover popup. That tells a
# section that's always slow from one a few trips hold up, and, where even the fastest trips are
# slow, says it's the street rather than the traffic. Each is left off a row with too few trips.
#
# terminal_section marks the first and last segments of most trips (see
# segments._with_terminal_sections), which the map dashes.
TILE_PROPERTIES = [
    "route_id",
    "direction_id",
    "from_stop_id",
    "from_stop_name",
    "to_stop_name",
    "time_band",
    "day_type",
    "p50_speed_mph",
    "n_traversals",
    "n_interpolated",
    "segment_length_m",
    "p50_total_time_seconds",
    "median_dwell_seconds",
    "p10_speed_mph",
    "p90_speed_mph",
    "terminal_section",
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


def _round_range_speed(speed, n_traversals):
    """To 0.1 mph under 10 mph and whole mph above it, and null with too few trips to say."""
    rounded = speed.round(1).where(speed < 10, speed.round())
    return rounded.where(n_traversals >= SPEED_RANGE_MINIMUM_TRAVERSALS)


def _round_for_tiles(frame):
    """Round to what the dashboard shows: speed to 0.1 mph, length and time to whole metres and
    seconds, as leaderboard.py does, dwell to DWELL_ROUNDING_SECONDS, and the speed range to
    _round_range_speed.

    Full-precision floats make nearly every value in a tile unique, and each one a full 8-byte
    double. Measured on 2026-10-28's band archive (11.4MB before length and time were added):
    they'd add 24% as floats and 10% as integers, while rounding speed saves 12%, so with both
    the archive comes out 5% smaller than before.

    The integers are nullable (Int64), so a NaN is left off its feature rather than failing the
    whole upload. median_dwell_seconds is NaN wherever no dwell was observed.
    """
    return frame.assign(
        p50_speed_mph=frame.p50_speed_mph.round(1),
        segment_length_m=frame.segment_length_m.round().astype("Int64"),
        p50_total_time_seconds=frame.p50_total_time_seconds.round().astype("Int64"),
        median_dwell_seconds=(frame.median_dwell_seconds / DWELL_ROUNDING_SECONDS).round().astype("Int64")
        * DWELL_ROUNDING_SECONDS,
        p10_speed_mph=_round_range_speed(frame.p10_speed_mph, frame.n_traversals),
        p90_speed_mph=_round_range_speed(frame.p90_speed_mph, frame.n_traversals),
    )


def build_pmtiles_bytes(frame, coordinates_column: str = "coordinates") -> bytes:
    """Serialise a DataFrame with a coordinate-list column to a PMTiles tileset, in memory.

    Only `TILE_PROPERTIES` (plus geometry) reach tippecanoe -- everything else in `frame`
    (the moving-speed columns, say) is dropped first.
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
