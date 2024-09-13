from datetime import date
from tempfile import TemporaryDirectory
import pandas as pd
from typing import Iterable
import boto3

from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import StopTime, Trip
from sqlalchemy import or_

# information to fetch from GTFS
MAX_QUERY_DEPTH = 900  # actually 1000


def fetch_stop_times_from_gtfs(trip_ids: Iterable[str], service_date: date) -> pd.DataFrame:
    """Fetch scheduled stop time information from GTFS."""
    s3 = boto3.resource("s3")
    mbta_gtfs = MbtaGtfsArchive(
        local_archive_path=TemporaryDirectory().name,
        s3_bucket=s3.Bucket("tm-gtfs"),
    )
    feed = mbta_gtfs.get_feed_for_date(service_date)
    feed.download_or_build()
    session = feed.create_sqlite_session()
    exists_remotely = feed.exists_remotely()

    gtfs_stops = []
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
        print(f"[{feed.key}] Uploading GTFS feed to S3")
        feed.upload_to_s3()
    return pd.concat(gtfs_stops)
