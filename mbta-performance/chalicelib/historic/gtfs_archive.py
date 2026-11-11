import datetime
import os
import numpy as np
import pandas as pd
import pathlib
import shutil
import urllib.request

from ..date import to_dateint
from ..routes import branch_route_id
from ..schedule import (
    RTE_DIR_STOP,
    add_scheduled_travel_times,
    gtfs_scheduled_headways,
    smooth_scheduled_headways,
)

# Set GTFS_DATA_DIR to share downloaded archives between repos when running locally
MAIN_DIR = pathlib.Path(os.environ.get("GTFS_DATA_DIR", "./data")).expanduser() / "gtfs_archives"
MAIN_DIR.mkdir(parents=True, exist_ok=True)

ARCHIVES = pd.read_csv("https://cdn.mbta.com/archive/archived_feeds.txt")

# Files read_gtfs needs; an archive dir missing any of them is treated as a partial download
REQUIRED_ARCHIVE_FILES = ["calendar.txt", "calendar_dates.txt", "stop_times.txt", "trips.txt"]


def get_gtfs_archive(dateint: int):
    """
    Determine which GTFS archive corresponds to the date.
    Returns that archive folder, downloading if it doesn't yet exist.
    """
    matches = ARCHIVES[(ARCHIVES.feed_start_date <= dateint) & (ARCHIVES.feed_end_date >= dateint)]
    archive_url = matches.iloc[0].archive_url

    archive_name = pathlib.Path(archive_url).stem
    archive_dir = MAIN_DIR / archive_name

    if archive_dir.exists():
        if all((archive_dir / f).exists() for f in REQUIRED_ARCHIVE_FILES):
            print(f"Archive for {dateint} already exists: {archive_name}")
            return archive_dir
        # A partial extraction from an interrupted run; without this check that date silently loses its events
        print(f"Archive for {dateint} is incomplete, re-downloading: {archive_name}")
        shutil.rmtree(archive_dir)

    # else we have to download it
    print(f"Downloading archive for {dateint}: {archive_url}")
    zipfile, _ = urllib.request.urlretrieve(archive_url)
    # Extract to a temp dir and rename, so an interrupted extraction never looks like a complete archive.
    # The pid suffix keeps repos sharing GTFS_DATA_DIR from extracting into the same temp dir at once.
    partial_dir = MAIN_DIR / f".{archive_name}.partial-{os.getpid()}"
    shutil.rmtree(partial_dir, ignore_errors=True)
    shutil.unpack_archive(zipfile, extract_dir=partial_dir, format="zip")
    # remove temporary zipfile
    urllib.request.urlcleanup()
    try:
        partial_dir.rename(archive_dir)
    except OSError:
        # another process sharing GTFS_DATA_DIR extracted the same archive first
        if not archive_dir.exists():
            raise
        shutil.rmtree(partial_dir, ignore_errors=True)

    return archive_dir


def get_services(date: datetime.date, archive_dir: pathlib.Path):
    """
    Read calendar.txt to determine which services ran on the given date.
    Also, incorporate exceptions from calendar_dates.txt for holidays, etc.
    """
    dateint = to_dateint(date)
    day_of_week = date.strftime("%A").lower()

    cal = pd.read_csv(archive_dir / "calendar.txt")
    current_services = cal[(cal.start_date <= dateint) & (cal.end_date >= dateint)]
    services = current_services[current_services[day_of_week] == 1].service_id.tolist()

    exceptions = pd.read_csv(archive_dir / "calendar_dates.txt")
    exceptions = exceptions[exceptions.date == dateint]
    additions = exceptions[exceptions.exception_type == 1].service_id.tolist()
    subtractions = exceptions[exceptions.exception_type == 2].service_id.tolist()

    services = (set(services) - set(subtractions)) | set(additions)
    return list(services)


def read_gtfs(date: datetime.date):
    """
    Given a date, this function will:
    - Find the appropriate gtfs archive (downloading if necessary)
    - Determine which services ran on that date
    - Return two dataframes containing just the trips and stop_times that ran on that date
    - Return None if the archive is incomplete (missing required files)
    """
    dateint = to_dateint(date)

    archive_dir = get_gtfs_archive(dateint)

    # Check if required files exist
    trips_file = archive_dir / "trips.txt"
    stop_times_file = archive_dir / "stop_times.txt"

    if not trips_file.exists():
        print(f"Warning: Missing trips.txt in GTFS archive for {date} ({archive_dir}). Skipping this date.")
        return None, None

    if not stop_times_file.exists():
        print(f"Warning: Missing stop_times.txt in GTFS archive for {date} ({archive_dir}). Skipping this date.")
        return None, None

    services = get_services(date, archive_dir)

    # specify dtypes to avoid warnings
    try:
        trips = pd.read_csv(
            trips_file,
            dtype={
                "route_id": str,
                "service_id": str,
                "trip_id": str,
                "trip_headsign": str,
                "trip_short_name": str,
                "block_id": str,
            },
        )
        trips = trips[trips.service_id.isin(services)]

        stops = pd.read_csv(
            stop_times_file,
            dtype={"trip_id": str, "stop_id": str, "stop_headsign": str, "checkpoint_id": str},
        )
        stops = stops[stops.trip_id.isin(trips.trip_id)]
        stops.arrival_time = pd.to_timedelta(stops.arrival_time)
        stops.departure_time = pd.to_timedelta(stops.departure_time)

        return trips, stops
    except Exception as e:
        print(f"Error reading GTFS files for {date} ({archive_dir}): {e}. Skipping this date.")
        return None, None


def scheduled_stop_times(trips: pd.DataFrame, stops: pd.DataFrame, route_ids: pd.Series) -> pd.DataFrame:
    """The day's scheduled stop times on these routes, in the form chalicelib/schedule.py takes.

    arrival_time becomes seconds after midnight, and rows are sorted by it.
    """
    trip_info = trips.loc[trips.route_id.isin(route_ids), ["trip_id", "route_id", "direction_id"]]
    stop_times = stops[["trip_id", "stop_id", "arrival_time"]].merge(trip_info, on="trip_id")
    stop_times["arrival_time"] = stop_times["arrival_time"].dt.total_seconds()
    stop_times = stop_times.dropna(subset=["route_id", "direction_id", "stop_id", "arrival_time"])
    return stop_times.sort_values("arrival_time", kind="stable").reset_index(drop=True)


def add_gtfs_headways(events_df: pd.DataFrame) -> pd.DataFrame:
    """Add scheduled_headway and scheduled_tt to historic events, from that day's GTFS archive.

    These are built the same way as the LAMP pipelines' (see chalicelib/schedule.py). The ArcGIS data
    has no scheduled headways of its own, so each event's comes from GTFS before being smoothed.
    Events on days without usable GTFS, or without a scheduled match, keep empty scheduled values.
    """
    results = []

    # GTFS changes so often that each day needs its own archive
    for service_date, days_events in events_df.groupby("service_date"):
        day = service_date.date()
        all_trips, all_stops = read_gtfs(day)

        if all_trips is None or all_stops is None:
            # Keep the day's events, just without scheduled headways/travel times, rather than dropping them
            print(f"Incomplete GTFS data for {day}; keeping its events without scheduled values")
            results.append(days_events.assign(scheduled_headway=np.nan, scheduled_tt=np.nan))
            continue

        gtfs_stops = scheduled_stop_times(all_trips, all_stops, days_events.route_id)
        events = days_events.assign(branch_route_id=branch_route_id(days_events).array)
        gtfs_stops = gtfs_stops.assign(branch_route_id=branch_route_id(gtfs_stops).array)

        events["scheduled_headway"] = gtfs_scheduled_headways(events, gtfs_stops, day).array
        events = add_scheduled_travel_times(
            events, gtfs_stops, day, match_on=(*RTE_DIR_STOP, "branch_route_id"), stop_on=("stop_id",)
        )
        events = smooth_scheduled_headways(events, day)

        results.append(
            events.drop(columns=["branch_route_id", "scheduled_trip_id"]).sort_values("event_time", kind="stable")
        )

    return pd.concat(results)
