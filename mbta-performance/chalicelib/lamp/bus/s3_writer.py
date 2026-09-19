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
from .constants import (
    MONTHLY_PMTILES_KEY_TEMPLATE,
    MONTHLY_S3_KEY_TEMPLATE,
    PMTILES_KEY_TEMPLATE,
    S3_BUCKET,
    S3_KEY_TEMPLATE,
    WEEKLY_PMTILES_KEY_TEMPLATE,
    WEEKLY_S3_KEY_TEMPLATE,
)
from .geoparquet import build_geoparquet_bytes
from .pmtiles import build_pmtiles_bytes

logger = logging.getLogger(__name__)


def s3_key_for(service_date: date) -> str:
    """Build the object key for a service date's segments."""
    return S3_KEY_TEMPLATE.format(YYYY=service_date.year, _M=service_date.month, _D=service_date.day)


def pmtiles_key_for(service_date: date) -> str:
    """Build the object key for a service date's PMTiles tileset."""
    return PMTILES_KEY_TEMPLATE.format(YYYY=service_date.year, _M=service_date.month, _D=service_date.day)


def weekly_s3_key_for(week: int) -> str:
    """Build the object key for a week number's segments (see periods.py)."""
    return WEEKLY_S3_KEY_TEMPLATE.format(week=week)


def weekly_pmtiles_key_for(week: int) -> str:
    """Build the object key for a week number's PMTiles tileset."""
    return WEEKLY_PMTILES_KEY_TEMPLATE.format(week=week)


def monthly_s3_key_for(month: int) -> str:
    """Build the object key for a month number's segments (see periods.py)."""
    return MONTHLY_S3_KEY_TEMPLATE.format(month=month)


def monthly_pmtiles_key_for(month: int) -> str:
    """Build the object key for a month number's PMTiles tileset."""
    return MONTHLY_PMTILES_KEY_TEMPLATE.format(month=month)


def _upload_geoparquet(segments: pd.DataFrame, key: str) -> str:
    data = build_geoparquet_bytes(segments)
    logger.info(f"Uploading {len(segments)} segment rows ({len(data) / 1048576:.2f}MB) to s3://{S3_BUCKET}/{key}")
    s3.upload_parquet(S3_BUCKET, key, data)
    return key


def _upload_pmtiles(segments: pd.DataFrame, key: str) -> str:
    data = build_pmtiles_bytes(segments)
    logger.info(f"Uploading PMTiles ({len(data) / 1048576:.2f}MB) to s3://{S3_BUCKET}/{key}")
    s3.upload_pmtiles(S3_BUCKET, key, data)
    return key


def upload_speed_segments(segments: pd.DataFrame, service_date: date) -> str:
    """Serialise a day of speed segments and put them in the performance bucket.

    Returns the key written. Re-running a date overwrites it in place, so a backfill or a
    corrected re-run is safe to repeat.
    """
    return _upload_geoparquet(segments, s3_key_for(service_date))


def upload_pmtiles(segments: pd.DataFrame, service_date: date) -> str:
    """Build a day's PMTiles tileset and put it in the performance bucket.

    Returns the key written. Requires tippecanoe on PATH -- see pmtiles.py.
    """
    return _upload_pmtiles(segments, pmtiles_key_for(service_date))


def upload_weekly_speed_segments(segments: pd.DataFrame, week: int) -> str:
    """Serialise a week's rolled-up speed segments and put them in the performance bucket.

    Returns the key written. Re-running a week overwrites it in place.
    """
    return _upload_geoparquet(segments, weekly_s3_key_for(week))


def upload_weekly_pmtiles(segments: pd.DataFrame, week: int) -> str:
    """Build a week's PMTiles tileset and put it in the performance bucket.

    Returns the key written. Requires tippecanoe on PATH -- see pmtiles.py.
    """
    return _upload_pmtiles(segments, weekly_pmtiles_key_for(week))


def upload_monthly_speed_segments(segments: pd.DataFrame, month: int) -> str:
    """Serialise a month's rolled-up speed segments and put them in the performance bucket.

    Returns the key written. Re-running a month overwrites it in place.
    """
    return _upload_geoparquet(segments, monthly_s3_key_for(month))


def upload_monthly_pmtiles(segments: pd.DataFrame, month: int) -> str:
    """Build a month's PMTiles tileset and put it in the performance bucket.

    Returns the key written. Requires tippecanoe on PATH -- see pmtiles.py.
    """
    return _upload_pmtiles(segments, monthly_pmtiles_key_for(month))
