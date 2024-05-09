from datetime import date
import io
import os
import pandas as pd
import requests
from typing import Iterable, Tuple

from .constants import LAMP_COLUMNS, S3_COLUMNS
from ..date import format_dateint, get_current_service_date
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import StopTime, Trip
from sqlalchemy import or_

from .. import parallel
from .. import s3


LAMP_INDEX_URL = "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/index.csv"
RAPID_DAILY_URL_TEMPLATE = "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/{YYYY_MM_DD}-subway-on-time-performance-v1.parquet"
S3_BUCKET = "tm-mbta-performance"
# month and day are not zero-padded
S3_KEY_TEMPLATE = "Events-lamp/daily-data/{stop_id}/Year={YYYY}/Month={_M}/Day={_D}/events.csv"

COLUMN_RENAME_MAP = {
    "headway_trunk_seconds": "headway_seconds",
    "scheduled_headway_trunk": "scheduled_headway",
    "scheduled_travel_time": "scheduled_tt",
}

# if a trip_id begins with NONREV-, it is not revenue producing and thus not something we want to benchmark
# if an event has a trip_id begins with ADDED-, then a downstream process was unable to determine the scheduled trip
# that the vehicle is currently on (this can be due to AVL glitches, trip diversions, test train trips, etc.)
TRIP_IDS_TO_DROP = ("NONREV-", "ADDED-")

# information to fetch from GTFS
TEMP_GTFS_LOCAL_PREFIX = ".temp/gtfs-feeds/"
MAX_QUERY_DEPTH = 900  # actually 1000
# defining these columns in particular becasue we use them everywhere
RTE_DIR_STOP = ["route_id", "direction_id", "stop_id"]


def _local_save(s3_key, stop_events):
    """TODO remove this temp code, it saves the output files locally!"""
    import os

    s3_key = ".temp/" + s3_key
    if not os.path.exists(os.path.dirname(s3_key)):
        os.makedirs(os.path.dirname(s3_key))
    stop_events.to_csv(s3_key)


def _process_arrival_departure_times(pq_df: pd.DataFrame) -> pd.DataFrame:
    """Process and collate arrivals and departures for a timetable of events.

    This does two things:
        1. Convert Epoch Unix timestamps to Eastern Time-datetimes
        2. Set departures' stop id's to that of their previous stops (see below example)
            Before (Green-D):
                STOP_SEQUENCE   | STOP          | ARRIVAL     | DEPARTURE
                360             | place-newto   | 4:55:59 AM  | (blah)
                370             | place-chhil   | 4:59:36 AM  | 4:56:41 AM
                380             | place-rsmnl   | (blah)      | 5:00:30 AM

            It's more interpretable to move up the departures so they act as follows:

            After (Green-D):
                STOP_SEQUENCE   | STOP          | ARRIVAL     | DEPARTURE
                360             | place-newto   | 4:55:59 AM  | 4:56:41 AM
                370             | place-chhil   | 4:59:36 AM  | 5:00:30 AM
                380             | place-rsmnl   | (blah)      | (blah)
    """
    pq_df["dep_time"] = pd.to_datetime(pq_df["move_timestamp"], unit="s", utc=True).dt.tz_convert("US/Eastern")
    pq_df["arr_time"] = pd.to_datetime(pq_df["stop_timestamp"], unit="s", utc=True).dt.tz_convert("US/Eastern")
    pq_df = pq_df.sort_values(by=["stop_sequence"])

    # explode departure and arrival times
    arr_df = pq_df[pq_df["arr_time"].notna()]
    arr_df = arr_df.assign(event_type="ARR").rename(columns={"arr_time": "event_time"})
    arr_df = arr_df[S3_COLUMNS]

    dep_df = pq_df[pq_df["dep_time"].notna()]
    dep_df = dep_df.assign(event_type="DEP").rename(columns={"dep_time": "event_time"})

    # these departures are from the the previous stop! so set them to the previous stop id
    # find the stop id for the departure whose sequence number precences the recorded one
    # stop sequences don't necessarily increment by 1 or with a reliable pattern
    dep_df = pd.merge_asof(
        dep_df,
        arr_df,
        on=["stop_sequence"],
        by=[
            "service_date",  # comment out for faster performance
            "route_id",
            "trip_id",
            "vehicle_id",
            "direction_id",
        ],
        direction="backward",
        suffixes=("_curr", "_prev"),
        allow_exact_matches=False,  # don't want to match on itself
    )

    # use current infomation for almost everything...
    renamed_cols = {key + "_curr": key for key in S3_COLUMNS if key != "stop_id"}
    # ...but use the PREVIOUS stop_id
    renamed_cols["stop_id_prev"] = "stop_id"
    dep_df = dep_df.rename(columns=renamed_cols)[S3_COLUMNS]

    # stitch together arrivals and departures
    return pd.concat([arr_df, dep_df])


def fetch_pq_file_from_remote(service_date: date) -> pd.DataFrame:
    """Fetch a parquet file from LAMP for a given service date."""
    # TODO(check if file exists in index, throw if it doesn't)
    url = RAPID_DAILY_URL_TEMPLATE.format(YYYY_MM_DD=service_date.strftime("%Y-%m-%d"))
    result = requests.get(url)

    if result.status_code != 200:
        raise ValueError(f"Failed to fetch LAMP parquet file from {url}. Status code: {result.status_code}")

    return pd.read_parquet(
        io.BytesIO(result.content),
        columns=LAMP_COLUMNS,
        engine="pyarrow",
        # NB: Even through parquet files are compressed with columnar metadata, pandas will sometimes override them
        # if the columns contain nulls. This is important as the move/stop times are nullable int64 epoch timestamps,
        # which will overflow if read in as floats.
        # https://pandas.pydata.org/docs/user_guide/integer_na.html#nullable-integer-data-type
        dtype_backend="numpy_nullable",
    )


def fetch_stop_times_from_gtfs(trip_ids: Iterable[str], service_date: date) -> pd.DataFrame:
    """Fetch scheduled stop time information from GTFS."""
    mbta_gtfs = MbtaGtfsArchive(TEMP_GTFS_LOCAL_PREFIX)
    feed = mbta_gtfs.get_feed_for_date(service_date)
    feed.download_or_build()
    session = feed.create_sqlite_session()

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
    return pd.concat(gtfs_stops)


def _recalculate_fields_from_gtfs(pq_df: pd.DataFrame, service_date: date):
    """Enrich LAMP data with GTFS data for some schedule information."""
    trip_ids = pq_df["trip_id"].unique()
    gtfs_stops = fetch_stop_times_from_gtfs(trip_ids, service_date)
    gtfs_stops = gtfs_stops.sort_values(by="arrival_time")

    # remove the reported travel times, because we use a slightly different metric.
    pq_df = pq_df.drop(columns=["scheduled_tt"])

    # we could do this groupby/min/merge in sql, but let's keep our computations in
    # pandas to stay consistent across services
    trip_start_times = gtfs_stops.groupby("trip_id").arrival_time.transform("min")
    gtfs_stops["scheduled_tt"] = gtfs_stops["arrival_time"] - trip_start_times
    gtfs_stops["arrival_time"] = gtfs_stops["arrival_time"].astype(float)

    # assign each actual trip a scheduled trip_id, based on when it started the route
    route_starts = pq_df.loc[pq_df.groupby("trip_id").event_time.idxmin()]
    route_starts["arrival_time"] = (
        route_starts.event_time - pd.Timestamp(service_date).tz_localize("US/Eastern")
    ).dt.total_seconds()

    trip_id_map = pd.merge_asof(
        route_starts.sort_values(by="arrival_time"),
        gtfs_stops[RTE_DIR_STOP + ["arrival_time", "trip_id"]],
        on="arrival_time",
        direction="nearest",
        by=RTE_DIR_STOP,
        suffixes=["", "_scheduled"],
    )
    trip_id_map = trip_id_map.drop_duplicates("trip_id").set_index("trip_id").trip_id_scheduled

    # use the scheduled trip matching to get the scheduled traveltime
    pq_df["scheduled_trip_id"] = pq_df.trip_id.map(trip_id_map)
    pq_df = pd.merge(
        pq_df,
        gtfs_stops[RTE_DIR_STOP + ["trip_id", "scheduled_tt"]],
        how="left",
        left_on=RTE_DIR_STOP + ["scheduled_trip_id"],
        right_on=RTE_DIR_STOP + ["trip_id"],
        suffixes=["", "_gtfs"],
    )
    return pq_df[S3_COLUMNS]


def ingest_pq_file(pq_df: pd.DataFrame, service_date: date) -> pd.DataFrame:
    """Process and tranform columns for the full day's events."""
    pq_df["direction_id"] = pq_df["direction_id"].astype("int16")
    pq_df["service_date"] = pq_df["service_date"].apply(format_dateint)
    # use trunk headway metrics as default, and add branch metrics when it makes sense.
    # TODO: verify and recalculate headway metrics if necessary!
    pq_df = pq_df.rename(columns=COLUMN_RENAME_MAP)
    # drop non-revenue producing events
    pq_df = pq_df[~pq_df["trip_id"].str.startswith(TRIP_IDS_TO_DROP)]

    processed_daily_events = _process_arrival_departure_times(pq_df)
    processed_daily_events = processed_daily_events[processed_daily_events["stop_id"].notna()]
    processed_daily_events = _recalculate_fields_from_gtfs(processed_daily_events, service_date)

    return processed_daily_events.sort_values(by=["event_time"])


def upload_to_s3(stop_id_and_events: Tuple[str, pd.DataFrame], service_date: date) -> None:
    """Upload events to s3 as a .csv file.

    Args:
        stop_id_and_events: tuple of a stop id, and all the arrival/departure events that occured at this
            stop over the course of a day (so far)
        service day: service date corresponding to the events.
    """
    # unpack from iterable
    stop_id, stop_events = stop_id_and_events

    # Upload to s3 as csv
    s3_key = S3_KEY_TEMPLATE.format(stop_id=stop_id, YYYY=service_date.year, _M=service_date.month, _D=service_date.day)
    # _local_save(s3_key, stop_events)
    s3.upload_df_as_csv(S3_BUCKET, s3_key, stop_events)
    return [stop_id]


_parallel_upload = parallel.make_parallel(upload_to_s3)


def ingest_today_lamp_data():
    """Ingest and upload today's LAMP data."""
    service_date = get_current_service_date()
    try:
        pq_df = fetch_pq_file_from_remote(service_date)
    except ValueError as e:
        # If we can't fetch the file, we can't process it
        print(e)
        return
    processed_daily_events = ingest_pq_file(pq_df, service_date)

    # split daily events by stop_id and parallel upload to s3
    stop_event_groups = processed_daily_events.groupby("stop_id")
    _parallel_upload(stop_event_groups, service_date)


if __name__ == "__main__":
    if not os.path.exists(os.path.dirname(TEMP_GTFS_LOCAL_PREFIX)):
        os.makedirs(TEMP_GTFS_LOCAL_PREFIX)
    ingest_today_lamp_data()
