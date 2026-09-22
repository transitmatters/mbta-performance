import json
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

# data-ingestion owns building GTFS bundles. Its on-demand worker reads this
# queue -- see transitmatters/data-ingestion ingestor/chalicelib/gtfs/enqueue.py.
GTFS_BUILD_QUEUE = "gtfs-ingest-keys"


def _enqueue_gtfs_build(feed_key: str) -> None:
    """Ask data-ingestion to build a feed we need but do not have.

    Best effort: a failure to enqueue must not mask the RuntimeError the caller
    is about to raise, which is the actionable signal.
    """
    try:
        queue = boto3.resource("sqs").get_queue_by_name(QueueName=GTFS_BUILD_QUEUE)
        queue.send_message(MessageBody=json.dumps({"feed_key": feed_key}))
        logger.info(f"Enqueued a build for GTFS feed {feed_key} on {GTFS_BUILD_QUEUE}")
    except Exception as e:  # pragma: no cover - best effort
        logger.error(f"Could not enqueue a build for GTFS feed {feed_key}: {e}")


def fetch_stop_times_from_gtfs(
    trip_ids: Iterable[str],
    service_date: date,
    local_archive_path: str | None = None,
    allow_build: bool = False,
) -> pd.DataFrame:
    """Fetch scheduled stop time information from GTFS.

    Args:
        allow_build: Whether this caller may build a missing feed itself. False in
            Lambda, where a build cannot fit; True for the backfill scripts, which
            run on a laptop with the disk and time to do it.
    """
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

    if feed.exists_locally():
        logger.info(f"GTFS feed {feed.key} already present locally")
    elif feed.exists_remotely():
        logger.info(f"Downloading GTFS feed {feed.key} from S3...")
        try:
            feed.download_from_s3()
        except Exception as e:
            logger.exception(f"Failed to download GTFS feed {feed.key}: {e}")
            raise
    elif allow_build:
        # Backfill on a laptop: build it, and publish it so the next caller --
        # including Lambda -- does not have to.
        logger.info(f"GTFS feed {feed.key} is missing; building locally")
        feed.build_locally()
        logger.info(f"Uploading GTFS feed {feed.key} to S3...")
        feed.upload_to_s3()
    else:
        # In Lambda a full build is ~200s and ~1GB of disk against a 60s timeout,
        # so download_or_build() could only ever time out -- and it did, every 30
        # minutes for 5 hours on 2026-09-15, when the MBTA published feed 20260907
        # (and later retracted it). Fail in ~2s and let data-ingestion's worker
        # build it; a later scheduled run picks it up.
        _enqueue_gtfs_build(feed.key)
        raise RuntimeError(
            f"GTFS feed {feed.key} is not in s3://tm-gtfs; enqueued a build on "
            f"{GTFS_BUILD_QUEUE}. Cannot build it here -- that needs ~200s and ~1GB "
            f"of disk against a 60s timeout."
        )
    logger.info("GTFS feed ready")

    session = feed.create_sqlite_session()

    gtfs_stops = []
    num_batches = (len(trip_ids) + MAX_QUERY_DEPTH - 1) // MAX_QUERY_DEPTH
    logger.debug(f"Querying GTFS in {num_batches} batches")
    for start in range(0, len(trip_ids), MAX_QUERY_DEPTH):
        gtfs_stops.append(
            pd.read_sql(
                session.query(
                    StopTime.trip_id,
                    StopTime.stop_id,
                    StopTime.arrival_time,
                    StopTime.stop_sequence,
                    Trip.route_id,
                    Trip.direction_id,
                )
                .filter(or_(StopTime.trip_id == tid for tid in trip_ids[start : start + MAX_QUERY_DEPTH]))  # noqa: E203
                .join(Trip, Trip.trip_id == StopTime.trip_id)
                .statement,
                session.bind,
                dtype_backend="numpy_nullable",
                dtype={"direction_id": "int16"},
            )
        )

    result = pd.concat(gtfs_stops)
    logger.info(f"Fetched {len(result)} GTFS stop times")
    return result
