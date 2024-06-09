import pandas as pd
import pathlib
from .constants import HISTORIC_COLUMNS_PRE_LAMP as HISTORIC_COLUMNS
from .gtfs_archive import add_gtfs_headways


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
