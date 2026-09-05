"""Write GeoParquet without a geo stack.

GeoParquet is ordinary parquet plus a `geo` key in the file's key/value metadata and a
binary column holding WKB. Encoding LineStrings by hand keeps shapely/geopandas/pyproj out
of the Lambda bundle, and QGIS, DuckDB spatial, GeoPandas and Felt all read the result
directly.

Spec: https://geoparquet.org/releases/v1.0.0/
"""

import io
import json
import logging
import struct

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

GEOPARQUET_VERSION = "1.0.0"
GEOMETRY_COLUMN = "geometry"

_WKB_LITTLE_ENDIAN = 1
_WKB_LINESTRING = 2


def linestring_to_wkb(coordinates) -> bytes | None:
    """Encode [(lon, lat), ...] as little-endian WKB LineString."""
    if coordinates is None or len(coordinates) < 2:
        return None
    header = struct.pack("<BII", _WKB_LITTLE_ENDIAN, _WKB_LINESTRING, len(coordinates))
    body = np.asarray(coordinates, dtype="<f8").tobytes()
    return header + body


def _bounding_box_from_wkb(geometries) -> list[float]:
    """Bounding box over already-encoded WKB LineStrings, as [minx, miny, maxx, maxy]."""
    minimum_x = minimum_y = float("inf")
    maximum_x = maximum_y = float("-inf")
    for wkb in geometries:
        if not wkb:
            continue
        coordinates = np.frombuffer(wkb[9:], dtype="<f8").reshape(-1, 2)
        minimum_x = min(minimum_x, float(coordinates[:, 0].min()))
        maximum_x = max(maximum_x, float(coordinates[:, 0].max()))
        minimum_y = min(minimum_y, float(coordinates[:, 1].min()))
        maximum_y = max(maximum_y, float(coordinates[:, 1].max()))
    return [minimum_x, minimum_y, maximum_x, maximum_y]


def _to_geo_table(frame, coordinates_column: str = "coordinates") -> "pa.Table":
    """Replace a coordinate-list column with WKB and stamp GeoParquet metadata on the table.

    CRS is omitted, which the spec defines as OGC:CRS84 (longitude/latitude on WGS 84) --
    the CRS GTFS shapes are already published in.
    """
    frame = frame.copy()
    frame[GEOMETRY_COLUMN] = [linestring_to_wkb(coordinates) for coordinates in frame[coordinates_column]]
    frame = frame.drop(columns=[coordinates_column])

    missing = frame[GEOMETRY_COLUMN].isna().sum()
    if missing:
        logger.warning(f"Dropping {missing} rows with unencodable geometry")
        frame = frame[frame[GEOMETRY_COLUMN].notna()]

    table = pa.Table.from_pandas(frame, preserve_index=False)
    geo_metadata = {
        "version": GEOPARQUET_VERSION,
        "primary_column": GEOMETRY_COLUMN,
        "columns": {
            GEOMETRY_COLUMN: {
                "encoding": "WKB",
                "geometry_types": ["LineString"],
                "bbox": _bounding_box_from_wkb(frame[GEOMETRY_COLUMN]),
            }
        },
    }
    existing = table.schema.metadata or {}
    return table.replace_schema_metadata({**existing, b"geo": json.dumps(geo_metadata).encode("utf-8")})


def build_geoparquet_bytes(frame, coordinates_column: str = "coordinates") -> bytes:
    """Serialise a DataFrame with a coordinate-list column to GeoParquet in memory.

    Used for uploading straight to S3 without touching the filesystem, which matters in a
    Lambda where /tmp is small and short-lived.
    """
    buffer = io.BytesIO()
    pq.write_table(_to_geo_table(frame, coordinates_column), buffer, compression="zstd")
    return buffer.getvalue()


def write_geoparquet(frame, path: str, coordinates_column: str = "coordinates") -> str:
    """Write a DataFrame with a coordinate-list column out as GeoParquet."""
    table = _to_geo_table(frame, coordinates_column)
    pq.write_table(table, path, compression="zstd")
    logger.info(f"Wrote {table.num_rows} segment rows to {path}")
    return path
