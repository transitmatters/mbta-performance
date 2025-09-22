import pandas as pd
import pathlib
from .constants import HISTORIC_COLUMNS_PRE_LAMP as HISTORIC_COLUMNS
from .constants import (
    CSV_FIELDS,
    arrival_field_mapping,
    departure_field_mapping,
    station_mapping,
    unofficial_ferry_labels_map,
    inbound_outbound,
)
from .gtfs_archive import add_gtfs_headways
from ..date import service_date as get_service_date
from datetime import datetime


def process_events(input_csv: str, outdir: str, nozip: bool = False, columns: list = HISTORIC_COLUMNS):
    df = pd.read_csv(
        input_csv,
        usecols=columns,
        parse_dates=["service_date"],
        dtype={
            "route_id": "str",
            "trip_id": "str",
            "stop_id": "str",
            "vehicle_id": "str",
            "vehicle_label": "str",
            "event_time": "int",
        },
    )

    df["event_time"] = df["service_date"] + pd.to_timedelta(df["event_time_sec"], unit="s")
    df.drop(columns=["event_time_sec"], inplace=True)

    if "sync_stop_sequence" in df.columns:
        df.rename(columns={"sync_stop_sequence": "stop_sequence"}, inplace=True)

    try:
        df = add_gtfs_headways(df)
    except IndexError:
        # failure to add gtfs benchmarks
        pass

    # Write to disk
    to_disk(df, outdir, nozip)


def to_disk(df: pd.DataFrame, outdir, nozip=False):
    """
    For each service_date/stop_id/direction/route group, we write the events to disk.
    """
    service_date_month = pd.Grouper(key="service_date", freq="1ME")
    grouped = df.groupby([service_date_month, "stop_id"])

    for name, events in grouped:
        service_date, stop_id = name

        fname = pathlib.Path(
            outdir,
            "Events",
            "monthly-data",
            str(stop_id),
            f"Year={service_date.year}",
            f"Month={service_date.month}",
            "events.csv.gz",
        )
        fname.parent.mkdir(parents=True, exist_ok=True)
        # set mtime to 0 in gzip header for determinism (so we can re-gen old routes, and rsync to s3 will ignore)
        events.to_csv(fname, index=False, compression={"method": "gzip", "mtime": 0} if not nozip else None)


def process_ferry(
    path_to_csv_file: str,
    outdir: str,
    nozip: bool = False,
):
    # read data, convert to datetime
    df = pd.read_csv(path_to_csv_file, low_memory=False)
    df.dropna(
        subset=["travel_direction", "trip_id", "departure_terminal", "mbta_sched_arrival", "mbta_sched_departure"]
    )

    # Convert to datetime first, then apply timezone localization
    df["mbta_sched_arrival"] = pd.to_datetime(df["mbta_sched_arrival"], errors="coerce").dt.tz_convert(tz="US/Eastern")
    df["mbta_sched_departure"] = pd.to_datetime(df["mbta_sched_departure"], errors="coerce").dt.tz_convert(
        tz="US/Eastern"
    )

    # Calculate Travel time in Minutes - only for rows that have both arrival and departure times
    # This should be calculated per trip, not per event
    arrival_time = df["mbta_sched_arrival"]
    departure_time = df["mbta_sched_departure"]

    # Only calculate travel time where both times are valid
    time_diff = arrival_time - departure_time
    df["scheduled_tt"] = time_diff.dt.total_seconds() / 60

    # Remove negative travel times (data quality issue)
    df.loc[df["scheduled_tt"] < 0, "scheduled_tt"] = None

    # Convert To Boston/From Boston to Inbound/Outbound Values
    df["travel_direction"] = df["travel_direction"].replace(inbound_outbound).infer_objects(copy=False)
    # Convert direction_id to integer to ensure outputs are integers
    df["travel_direction"] = df["travel_direction"].astype("Int64")
    # Replace terminal values with GTFS Approved Values
    df["departure_terminal"] = df["departure_terminal"].replace(station_mapping)
    df["arrival_terminal"] = df["arrival_terminal"].replace(station_mapping)
    # Replace Route_ids based on mapping
    df["route_id"] = df["route_id"].replace(unofficial_ferry_labels_map)

    # Subset dataframe to just arrival and departure event data - create copies to avoid warnings
    arrival_events = df[arrival_field_mapping.keys()].copy()
    departure_events = df[departure_field_mapping.keys()].copy()

    arrival_events.rename(columns=arrival_field_mapping, inplace=True)
    departure_events.rename(columns=departure_field_mapping, inplace=True)

    # Add missing columns with default values
    for events_df in [arrival_events, departure_events]:
        events_df["stop_sequence"] = None
        events_df["vehicle_label"] = None
        events_df["vehicle_consist"] = None

    # Add event_type to distinguish between arrivals and departures
    arrival_events.loc[:, "event_type"] = "ARR"
    departure_events.loc[:, "event_type"] = "DEP"

    # Convert event_time to datetime, handling mixed formats
    arrival_events.loc[:, "event_time"] = pd.to_datetime(arrival_events["event_time"], format="mixed", errors="coerce")
    departure_events.loc[:, "event_time"] = pd.to_datetime(
        departure_events["event_time"], format="mixed", errors="coerce"
    )

    arrival_events = arrival_events[CSV_FIELDS]
    departure_events = departure_events[CSV_FIELDS]
    df = pd.concat([arrival_events, departure_events])

    # Convert service_date to datetime for proper grouping in to_disk()
    # First convert to datetime, then apply service_date logic, then back to datetime
    df.loc[:, "service_date"] = pd.to_datetime(df["service_date"], errors="coerce").apply(
        lambda x: pd.to_datetime(get_service_date(x)) if pd.notna(x) else x
    )

    # Load route constants and add stop sequence information
    # route_dicts = load_constants()
    # events = add_stop_sequence_to_dataframe(events, route_dicts)

    to_disk(df, outdir, nozip)


def load_bus_data(input_csv: str, routes: list = None):
    """
    Loads in the below format and makes some adjustments for processing.
    - Filter only points with actual trip data
    - Trim leading 0s from route_id
    - Select only route_ids in `routes`
    - Set scheduled/actual times to be on service_date, not 1900-01-01
    - Map direction_id (Outbound -> 0, Inbound -> 1)

    "service_date", "route_id", "direction",  "half_trip_id", "stop_id",  "time_point_id",  "time_point_order", "point_type",   "standard_type",  "scheduled",            "actual",               "scheduled_headway",  "headway"
    2020-01-15,     "01",       "Inbound",    46374001,       67,         "maput",                2,            "Midpoint",     "Schedule",       1900-01-01 05:08:00,    1900-01-01 05:09:07,      -5,                   NA,NA
    2020-01-15,     "01",       "Inbound",    46374001,       110,        "hhgat",                1,            "Startpoint",   "Schedule",       1900-01-01 05:05:00,    1900-01-01 05:04:34,      26,                   NA,NA
    2020-01-15,     "01",       "Inbound",    46374001,       72,         "cntsq",                3,            "Midpoint",     "Schedule",       1900-01-01 05:11:00,    1900-01-01 05:12:01,      -22,                    NA,NA
    2020-01-15,     "01",       "Inbound",    46374001,       75,         "mit",                  4,            "Midpoint",     "Schedule",       1900-01-01 05:14:00,    1900-01-01 05:14:58,      -25,                    NA,NA
    2020-01-15,     "01",       "Inbound",    46374001,       79,         "hynes",                5,            "Midpoint",     "Schedule",       1900-01-01 05:18:00,    1900-01-01 05:18:45,      32,                   NA,NA
    2020-01-15,     "01",       "Inbound",    46374001,       187,        "masta",                6,            "Midpoint",     "Schedule",       1900-01-01 05:20:00,    1900-01-01 05:21:04,      -33,                    NA,NA
    2020-01-15,     "01",       "Inbound",    46374045,       110,        "hhgat",                1,            "Startpoint",   "Headway",        1900-01-01 05:20:00,    1900-01-01 05:20:45,      NA,                   900,971
    """
    df = pd.read_csv(input_csv, dtype={"service_date": str, "route_id": str, "direction": str, "stop_id": str})
    df.rename(
        columns={
            # This set of transformations covers prior-year bus data.
            "ServiceDate": "service_date",
            "Route": "route_id",
            "Direction": "direction_id",
            "HalfTripId": "half_trip_id",
            "Stop": "stop_id",
            "stop_name": "time_point_id",
            "stop_sequence": "time_point_order",
            "Timepoint": "time_point_id",
            "TimepointOrder": "time_point_order",
            "PointType": "point_type",
            "StandardType": "standard_type",
            "Scheduled": "scheduled",
            "Actual": "actual",
            "ScheduledHeadway": "scheduled_headway",
            "Headway": "headway",
            "direction": "direction_id",
        },
        inplace=True,
    )

    # We need to keep both "Headway" AND "Schedule": both can have timepoint data.
    df = df.loc[df.actual.notnull()]

    df.route_id = df.route_id.str.lstrip("0")
    if routes:
        df = df.loc[df.route_id.isin(routes)]
    df.stop_id = df.stop_id.astype(str)

    # Convert dates
    df.scheduled = pd.to_datetime(df.scheduled).dt.tz_localize(None)
    df.service_date = pd.to_datetime(df.service_date).dt.tz_localize(None)
    df.actual = pd.to_datetime(df.actual).dt.tz_localize(None)

    OFFSET = datetime(1900, 1, 1, 0, 0, 0)
    df.scheduled = df.service_date + (df.scheduled - OFFSET)
    df.actual = df.service_date + (df.actual - OFFSET)

    df.direction_id = df.direction_id.map({"Outbound": 0, "Inbound": 1})

    return df


def process_bus_events(input_csv: str, outdir: str, routes: list = None, nozip: bool = False):
    """
    Process bus events from CSV file and save to disk.
    - Load and clean bus data
    - Transform to match rapid transit format
    - Add GTFS headways
    - Generate ARR/DEP events
    - Save to monthly bus data structure
    """
    data = load_bus_data(input_csv, routes)
    events = process_bus_events_data(data)
    to_disk_bus(events, outdir, nozip=nozip)


def process_bus_events_data(df: pd.DataFrame):
    """
    Take the tidied input data and rearrange the columns to match rapidtransit format.
    - Rename columns (trip_id, stop_sequence, event_time)
    - Remove extra columns
    - Add empty vehicle columns
    - Calculate event_type column with ARR and DEP entries
    """
    CSV_HEADER = [
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
        "scheduled_headway",
        "scheduled_tt",
    ]

    df = df.rename(columns={"half_trip_id": "trip_id", "time_point_order": "stop_sequence", "actual": "event_time"})
    df = df.drop(
        columns=["time_point_id", "standard_type", "scheduled", "scheduled_headway", "headway"], errors="ignore"
    )

    df = add_gtfs_headways(df)

    df["vehicle_id"] = ""
    df["vehicle_label"] = ""
    df["event_type"] = df.point_type.map({"Startpoint": ["DEP"], "Midpoint": ["ARR", "DEP"], "Endpoint": ["ARR"]})
    df = df.explode("event_type")
    df = df[CSV_HEADER]  # reorder

    return df


def to_disk_bus(df: pd.DataFrame, outdir: str, nozip: bool = False):
    """
    For each service_date/stop_id/direction/route group, we write the events to disk.
    Uses monthly bus data structure: monthly-bus-data/{route_id}-{direction_id}-{stop_id}/Year={year}/Month={month}/events.csv.gz
    """
    monthly_service_date = pd.Grouper(key="service_date", freq="1ME")
    grouped = df.groupby([monthly_service_date, "stop_id", "direction_id", "route_id"])

    for name, events in grouped:
        service_date, stop_id, direction_id, route_id = name

        fname = pathlib.Path(
            outdir,
            "Events",
            "monthly-bus-data",
            f"{route_id}-{direction_id}-{stop_id}",
            f"Year={service_date.year}",
            f"Month={service_date.month}",
            "events.csv.gz",
        )
        fname.parent.mkdir(parents=True, exist_ok=True)
        # set mtime to 0 in gzip header for determinism (so we can re-gen old routes, and rsync to s3 will ignore)
        events.to_csv(fname, index=False, compression={"method": "gzip", "mtime": 0} if not nozip else None)
