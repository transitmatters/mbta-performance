"""Write GeoParquet via geopandas.

QGIS, DuckDB spatial, GeoPandas and Felt all read the result directly. This used to hand-roll
WKB encoding to keep shapely/geopandas out of the Lambda bundle; now that this pipeline runs
as a k8s cron job instead, that constraint is gone and geopandas' own writer covers it.

Spec: https://geoparquet.org/releases/v1.0.0/
"""

import io
import logging

import geopandas as gpd
from shapely.geometry import LineString

logger = logging.getLogger(__name__)

GEOMETRY_COLUMN = "geometry"

# GTFS shapes are published in WGS84 lon/lat, and downstream consumers (the map, QGIS) expect
# the same, so segment geometry stays in this CRS even though it's projected internally to
# locate stops and cut segments.
_LONLAT_CRS = "EPSG:4326"


def _to_linestring(coordinates) -> LineString | None:
    if coordinates is None or len(coordinates) < 2:
        return None
    return LineString(coordinates)


def _to_geodataframe(frame, coordinates_column: str = "coordinates") -> gpd.GeoDataFrame:
    """Replace a coordinate-list column with LineString geometry, in WGS84."""
    frame = frame.copy()
    geometry = [_to_linestring(coordinates) for coordinates in frame[coordinates_column]]
    frame = frame.drop(columns=[coordinates_column])

    geo_frame = gpd.GeoDataFrame(frame, geometry=geometry, crs=_LONLAT_CRS)

    missing = geo_frame[GEOMETRY_COLUMN].isna().sum()
    if missing:
        logger.warning(f"Dropping {missing} rows with unencodable geometry")
        geo_frame = geo_frame[geo_frame[GEOMETRY_COLUMN].notna()]
    return geo_frame


def build_geoparquet_bytes(frame, coordinates_column: str = "coordinates") -> bytes:
    """Serialise a DataFrame with a coordinate-list column to GeoParquet in memory.

    Used for uploading straight to S3 without writing a temporary file first.
    """
    buffer = io.BytesIO()
    _to_geodataframe(frame, coordinates_column).to_parquet(buffer, compression="zstd")
    return buffer.getvalue()


def write_geoparquet(frame, path: str, coordinates_column: str = "coordinates") -> str:
    """Write a DataFrame with a coordinate-list column out as GeoParquet."""
    geo_frame = _to_geodataframe(frame, coordinates_column)
    geo_frame.to_parquet(path, compression="zstd")
    logger.info(f"Wrote {len(geo_frame)} segment rows to {path}")
    return path
