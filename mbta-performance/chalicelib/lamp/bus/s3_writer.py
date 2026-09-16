"""Publish bus speed segments to S3.

One self-contained GeoParquet per service date holding the whole network (~4MB, ~53k rows).
This is a deliberate departure from the per-(stop, day) CSV layout the rest of the bucket
uses: a map view needs every segment at once, and fetching 10.8k objects to draw one day
would be far slower and more expensive than a single GET.
"""

import logging
from datetime import date

import pandas as pd

from ... import s3
from .constants import PMTILES_KEY_TEMPLATE, S3_BUCKET, S3_KEY_TEMPLATE
from .geoparquet import build_geoparquet_bytes
from .pmtiles import build_pmtiles_bytes

logger = logging.getLogger(__name__)


def s3_key_for(service_date: date) -> str:
    """Build the object key for a service date's segments."""
    return S3_KEY_TEMPLATE.format(YYYY=service_date.year, _M=service_date.month, _D=service_date.day)


def pmtiles_key_for(service_date: date) -> str:
    """Build the object key for a service date's PMTiles tileset."""
    return PMTILES_KEY_TEMPLATE.format(YYYY=service_date.year, _M=service_date.month, _D=service_date.day)


def upload_speed_segments(segments: pd.DataFrame, service_date: date) -> str:
    """Serialise a day of speed segments and put them in the performance bucket.

    Returns the key written. Re-running a date overwrites it in place, so a backfill or a
    corrected re-run is safe to repeat.
    """
    key = s3_key_for(service_date)
    data = build_geoparquet_bytes(segments)
    logger.info(f"Uploading {len(segments)} segment rows ({len(data) / 1048576:.2f}MB) to s3://{S3_BUCKET}/{key}")
    s3.upload_parquet(S3_BUCKET, key, data)
    return key


def upload_pmtiles(segments: pd.DataFrame, service_date: date) -> str:
    """Build a day's PMTiles tileset and put it in the performance bucket.

    Returns the key written. Requires tippecanoe on PATH -- see pmtiles.py.
    """
    key = pmtiles_key_for(service_date)
    data = build_pmtiles_bytes(segments)
    logger.info(f"Uploading PMTiles ({len(data) / 1048576:.2f}MB) to s3://{S3_BUCKET}/{key}")
    s3.upload_pmtiles(S3_BUCKET, key, data)
    return key
