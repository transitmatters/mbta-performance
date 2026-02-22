import logging
from datetime import date
from tempfile import TemporaryDirectory
from typing import Iterable

import boto3
import pandas as pd
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import StopTime, Trip
from sqlalchemy import or_

logger = logging.getLogger(__name__)

# information to fetch from GTFS
MAX_QUERY_DEPTH = 900  # actually 1000


def fetch_stop_times_from_gtfs(
    trip_ids: Iterable[str], service_date: date, local_archive_path: str | None = None
) -> pd.DataFrame:
    """Fetch scheduled stop time information from GTFS."""
    logger.info(f"Fetching GTFS stop times for {len(trip_ids)} trips on {service_date}")
    s3 = boto3.resource("s3")
    if not local_archive_path:
        local_archive_path = TemporaryDirectory().name
    mbta_gtfs = MbtaGtfsArchive(
        local_archive_path=local_archive_path,
        s3_bucket=s3.Bucket("tm-gtfs"),
    )
    feed = mbta_gtfs.get_feed_for_date(service_date)
    logger.info(f"GTFS feed key: {feed.key}")

    logger.info("Downloading or building GTFS feed...")
    try:
        feed.download_or_build()
    except Exception as e:
        logger.exception(f"Failed to download or build GTFS feed {feed.key}: {e}")
        raise
    logger.info("GTFS feed ready")

    session = feed.create_sqlite_session()
    exists_remotely = feed.exists_remotely()

    gtfs_stops = []
    num_batches = (len(trip_ids) + MAX_QUERY_DEPTH - 1) // MAX_QUERY_DEPTH
    logger.debug(f"Querying GTFS in {num_batches} batches")
    for start in range(0, len(trip_ids), MAX_QUERY_DEPTH):
        gtfs_stops.append(
            pd.read_sql(
                session.query(
                    StopTime.trip_id, StopTime.stop_id, StopTime.arrival_time, Trip.route_id, Trip.direction_id
                )
                .filter(or_(StopTime.trip_id == tid for tid in trip_ids[start : start + MAX_QUERY_DEPTH]))  # noqa: E203
                .join(Trip, Trip.trip_id == StopTime.trip_id)
                .statement,
                session.bind,
                dtype_backend="numpy_nullable",
                dtype={"direction_id": "int16"},
            )
        )

    if not exists_remotely:
        logger.info(f"Uploading GTFS feed {feed.key} to S3...")
        try:
            feed.upload_to_s3()
        except Exception as e:
            logger.exception(f"Failed to upload GTFS feed {feed.key} to S3: {e}")
            raise
        logger.info(f"GTFS feed {feed.key} uploaded to S3")

    result = pd.concat(gtfs_stops)
    logger.info(f"Fetched {len(result)} GTFS stop times")
    return result


def ensure_gtfs_bundle_for_date(service_date: date) -> None:
    """Ensure the GTFS SQLite bundle for the given date is built and uploaded to S3."""
    logger.info(f"Checking GTFS bundle for {service_date}...")
    s3 = boto3.resource("s3")
    with TemporaryDirectory() as tmpdir:
        mbta_gtfs = MbtaGtfsArchive(
            local_archive_path=tmpdir,
            s3_bucket=s3.Bucket("tm-gtfs"),
        )
        feed = mbta_gtfs.get_feed_for_date(service_date)
        logger.info(f"GTFS feed key: {feed.key}")
        if feed.exists_remotely():
            logger.info("GTFS feed already in S3, nothing to do.")
            return
        logger.info("GTFS feed not in S3. Downloading and building...")
        feed.download_or_build()
        logger.info("Uploading GTFS feed to S3...")
        feed.upload_to_s3()
        logger.info("GTFS feed uploaded successfully.")
