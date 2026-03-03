import logging
import os
from datetime import date
from tempfile import TemporaryDirectory
from typing import Iterable

import boto3
import pandas as pd
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import StopTime, Trip
from sqlalchemy import or_

logger = logging.getLogger(__name__)

BOOSTED_TIMEOUT = 900  # AWS Lambda maximum timeout in seconds
NORMAL_TIMEOUT = 250  # Normal configured timeout for LAMP ingest functions


def _set_lambda_timeout(timeout_seconds: int) -> None:
    """Update this Lambda function's configured timeout for future invocations.

    Has no effect on the current invocation — only applies to the next one.
    Safe to call from local dev (no-op when AWS_LAMBDA_FUNCTION_NAME is unset).
    """
    function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    if not function_name:
        return
    try:
        client = boto3.client("lambda")
        current = client.get_function_configuration(FunctionName=function_name)["Timeout"]
        if current != timeout_seconds:
            logger.info(f"Updating Lambda timeout: {current}s → {timeout_seconds}s")
            client.update_function_configuration(FunctionName=function_name, Timeout=timeout_seconds)
    except Exception:
        logger.exception("Failed to update Lambda timeout — continuing anyway")


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

    # Check S3 before downloading so we can boost the Lambda timeout if a
    # new bundle needs to be built and uploaded (which can take several minutes).
    exists_remotely = feed.exists_remotely()
    if not exists_remotely:
        logger.warning(f"GTFS feed {feed.key} not in S3 — boosting Lambda timeout for next invocation")
        _set_lambda_timeout(BOOSTED_TIMEOUT)

    logger.info("Downloading or building GTFS feed...")
    try:
        feed.download_or_build()
    except Exception as e:
        logger.exception(f"Failed to download or build GTFS feed {feed.key}: {e}")
        raise
    logger.info("GTFS feed ready")

    session = feed.create_sqlite_session()

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
        # Upload succeeded — restore normal timeout for future invocations
        _set_lambda_timeout(NORMAL_TIMEOUT)

    result = pd.concat(gtfs_stops)
    logger.info(f"Fetched {len(result)} GTFS stop times")
    return result
