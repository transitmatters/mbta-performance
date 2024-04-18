from datetime import date
import io
from typing import Tuple
import requests
import pandas as pd

from .date import format_dateint, get_current_service_date
from .. import parallel
from .. import s3


LAMP_INDEX_URL = "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/index.csv"
RAPID_DAILY_URL_TEMPLATE = "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/{YYYY_MM_DD}-subway-on-time-performance-v1.parquet"
S3_BUCKET = "tm-mbta-performance"
# month and day are not zero-padded
S3_KEY_TEMPLATE = "Events-lamp/daily-data/{stop_id}/Year={YYYY}/Month={_M}/Day={_D}/events.csv"


# LAMP columns to fetch from parquet files
INPUT_COLUMNS = [
    "service_date",
    "route_id",
    "trip_id",
    "stop_id",
    "direction_id",
    "stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "move_timestamp",  # departure time from the previous station
    "stop_timestamp",  # arrival time at the current station
    # BENCHMARKING COLUMNS
    "travel_time_seconds",
    "dwell_time_seconds",
    "headway_trunk_seconds",
    "headway_branch_seconds",
    "scheduled_travel_time",
    "scheduled_headway_trunk",
    "scheduled_headway_branch",
]

COLUMN_RENAME_MAP = {
    "headway_trunk_seconds": "headway_seconds",
    "scheduled_headway_trunk": "scheduled_headway",
}

# columns that should be output to s3 events.csv
OUTPUT_COLUMNS = [
    "service_date",
    "route_id",
    "trip_id",
    "direction_id",
    "stop_id",
    "stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "event_type",
    "event_time",
    "travel_time_seconds",
    "dwell_time_seconds",
    "headway_seconds",
    "headway_branch_seconds",
    "scheduled_travel_time",
    "scheduled_headway",
    "scheduled_headway_branch",
]


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
    arr_df = arr_df[OUTPUT_COLUMNS]

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
    dep_df = dep_df.rename(
        columns={
            "event_time_curr": "event_time",  # use CURRENT time...
            "stop_id_prev": "stop_id",  # ...but PREVIOUS stop id...
            "event_type_curr": "event_type",  # keep DEPARTURE label...
            "vehicle_label_curr": "vehicle_label",
            # Keep all current benchmarking data...
            "travel_time_seconds_curr": "travel_time_seconds",
            "dwell_time_seconds_curr": "dwell_time_seconds",
            "headway_seconds_curr": "headway_seconds",
            "headway_branch_seconds_curr": "headway_branch_seconds",
            "scheduled_travel_time_curr": "scheduled_travel_time",
            "scheduled_headway_curr": "scheduled_headway",
            "scheduled_headway_branch_curr": "scheduled_headway_branch",
        }
    )[OUTPUT_COLUMNS]

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
        columns=INPUT_COLUMNS,
        engine="pyarrow",
        # NB: Even through parquet files are compressed with columnar metadata, pandas will sometimes override them
        # if the columns contain nulls. This is important as the move/stop times are nullable int64 epoch timestamps,
        # which will overflow if read in as floats.
        # https://pandas.pydata.org/docs/user_guide/integer_na.html#nullable-integer-data-type
        dtype_backend="numpy_nullable",
    )


def ingest_pq_file(pq_df: pd.DataFrame) -> pd.DataFrame:
    """Process and tranform columns for the full day's events."""
    pq_df["direction_id"] = pq_df["direction_id"].astype("int16")
    pq_df["service_date"] = pq_df["service_date"].apply(format_dateint)
    # use trunk headway metrics as default, and add branch metrics when it makes sense.
    # TODO: verify and recalculate headway metrics if necessary!
    pq_df = pq_df.rename(columns=COLUMN_RENAME_MAP)

    processed_daily_events = _process_arrival_departure_times(pq_df)
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
    processed_daily_events = ingest_pq_file(pq_df)

    # split daily events by stop_id and parallel upload to s3
    stop_event_groups = processed_daily_events.groupby("stop_id")
    _parallel_upload(stop_event_groups, service_date)


if __name__ == "__main__":
    ingest_today_lamp_data()
