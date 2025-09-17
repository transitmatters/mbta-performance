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

    # Calculate Travel time in Minutes
    time_diff = pd.to_datetime(df["mbta_sched_arrival"]) - pd.to_datetime(df["mbta_sched_departure"])
    df["scheduled_tt"] = time_diff.dt.total_seconds() / 60

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
