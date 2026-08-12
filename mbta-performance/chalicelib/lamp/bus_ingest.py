import io
import logging
from datetime import date
from typing import Tuple

import pandas as pd
import requests

from .. import parallel, s3
from ..date import EASTERN_TIME, get_current_service_date
from ..gtfs import fetch_stop_times_from_gtfs
from .bus_constants import (
    BUS_COLUMN_RENAME_MAP,
    BUS_DAILY_URL_TEMPLATE,
    BUS_LAMP_COLUMNS,
    BUS_S3_BUCKET,
    BUS_S3_COLUMNS,
    BUS_S3_KEY_TEMPLATE,
)

logger = logging.getLogger(__name__)

RTE_DIR_STOP = ["route_id", "direction_id", "stop_id"]


def fetch_bus_pq_file_from_remote(service_date: date) -> pd.DataFrame:
    """Fetch a bus parquet file from LAMP for a given service date."""
    url = BUS_DAILY_URL_TEMPLATE.format(YYYYMMDD=service_date.strftime("%Y%m%d"))
    logger.info(f"Fetching bus LAMP parquet file from {url}")
    result = requests.get(url)

    if result.status_code != 200:
        logger.error(f"Failed to fetch bus LAMP parquet file from {url}. Status code: {result.status_code}")
        raise ValueError(f"Failed to fetch bus LAMP parquet file from {url}. Status code: {result.status_code}")

    logger.info(f"Successfully fetched bus LAMP parquet file ({len(result.content)} bytes)")
    df = pd.read_parquet(
        io.BytesIO(result.content),
        columns=BUS_LAMP_COLUMNS,
        engine="pyarrow",
        dtype_backend="numpy_nullable",
    )
    logger.info(f"Parsed parquet file: {len(df)} rows")
    return df


def _process_bus_arrival_departure_times(df: pd.DataFrame) -> pd.DataFrame:
    """Split bus events into separate ARR and DEP rows.

    Bus LAMP data already provides stop_arrival_dt and stop_departure_dt
    as UTC-aware timestamps, along with previous_stop_id. We just need to:
    1. Convert to Eastern Time
    2. Create separate ARR/DEP event rows
    3. For DEP events, use previous_stop_id as the stop_id
    """
    logger.debug(f"Processing arrival/departure times for {len(df)} rows")

    # Arrivals: use stop_arrival_dt and the current stop_id
    arr_df = df[df["stop_arrival_dt"].notna()].copy()
    arr_df["event_type"] = "ARR"
    arr_df["event_time"] = arr_df["stop_arrival_dt"].dt.tz_convert(EASTERN_TIME)
    arr_df = arr_df[BUS_S3_COLUMNS]

    # Departures: use stop_departure_dt and previous_stop_id
    dep_df = df[df["stop_departure_dt"].notna() & df["previous_stop_id"].notna()].copy()
    dep_df["event_type"] = "DEP"
    dep_df["event_time"] = dep_df["stop_departure_dt"].dt.tz_convert(EASTERN_TIME)
    dep_df["stop_id"] = dep_df["previous_stop_id"]
    dep_df = dep_df[BUS_S3_COLUMNS]

    result = pd.concat([arr_df, dep_df])
    logger.debug(f"Processed: {len(arr_df)} arrivals, {len(dep_df)} departures")
    return result


def _recalculate_bus_fields_from_gtfs(
    pq_df: pd.DataFrame, service_date: date, local_archive_path: str | None = None
) -> pd.DataFrame:
    """Enrich bus LAMP data with GTFS scheduled travel times.

    Replaces LAMP's plan_travel_time_seconds with a scheduled_tt computed from
    GTFS (arrival_time - trip start time), matching each LAMP trip to its
    nearest scheduled GTFS trip by route/direction/stop.
    """
    trip_ids = pq_df["trip_id"].unique()
    logger.info(f"Enriching bus LAMP data with GTFS for {len(trip_ids)} unique trips on {service_date}")
    gtfs_stops = fetch_stop_times_from_gtfs(trip_ids, service_date, local_archive_path=local_archive_path)
    logger.debug(f"Fetched {len(gtfs_stops)} GTFS stop times")
    gtfs_stops = gtfs_stops.sort_values(by="arrival_time")

    # Normalize merge-key dtypes so pd.merge_asof's strict by= check doesn't reject string vs. object.
    for col in ("route_id", "stop_id", "trip_id"):
        pq_df[col] = pq_df[col].astype("string")
        gtfs_stops[col] = gtfs_stops[col].astype("string")
    pq_df["direction_id"] = pq_df["direction_id"].astype("int16")
    gtfs_stops["direction_id"] = gtfs_stops["direction_id"].astype("int16")

    pq_df = pq_df.drop(columns=["scheduled_tt"])

    trip_start_times = gtfs_stops.groupby("trip_id").arrival_time.transform("min")
    gtfs_stops["scheduled_tt"] = gtfs_stops["arrival_time"] - trip_start_times
    gtfs_stops["arrival_time"] = gtfs_stops["arrival_time"].astype(float)

    route_starts = pq_df.loc[pq_df.groupby("trip_id").event_time.idxmin()]
    route_starts["arrival_time"] = (
        route_starts.event_time - pd.Timestamp(service_date).tz_localize(EASTERN_TIME)
    ).dt.total_seconds()

    trip_id_map = pd.merge_asof(
        route_starts.sort_values(by="arrival_time"),
        gtfs_stops[RTE_DIR_STOP + ["arrival_time", "trip_id"]].drop_duplicates(),
        on="arrival_time",
        direction="nearest",
        by=RTE_DIR_STOP,
        suffixes=["", "_scheduled"],
    )
    trip_id_map = trip_id_map.drop_duplicates("trip_id").set_index("trip_id").trip_id_scheduled

    pq_df["scheduled_trip_id"] = pq_df.trip_id.map(trip_id_map)
    pq_df = pd.merge(
        pq_df,
        gtfs_stops[["trip_id", "stop_id", "scheduled_tt"]],
        how="left",
        left_on=["scheduled_trip_id", "stop_id"],
        right_on=["trip_id", "stop_id"],
        suffixes=["", "_gtfs"],
    )

    missing_tt_mask = pq_df["scheduled_tt"].isna()
    if missing_tt_mask.any():
        logger.debug(f"Attempting fallback scheduled_tt matching for {missing_tt_mask.sum()} events")
        gtfs_stops["time_bucket"] = (gtfs_stops["arrival_time"] // 1800).astype(int)
        bucketed_median_tt = gtfs_stops.groupby(RTE_DIR_STOP + ["time_bucket"])["scheduled_tt"].median()

        event_seconds = (
            pq_df.loc[missing_tt_mask, "event_time"] - pd.Timestamp(service_date).tz_localize(EASTERN_TIME)
        ).dt.total_seconds()
        event_buckets = (event_seconds // 1800).astype(int)

        lookup_keys = pq_df.loc[missing_tt_mask, RTE_DIR_STOP].copy()
        lookup_keys["time_bucket"] = event_buckets.values
        fallback_tt = lookup_keys.apply(lambda row: bucketed_median_tt.get(tuple(row)), axis=1)

        pq_df.loc[missing_tt_mask, "scheduled_tt"] = fallback_tt.values
        filled_count = (~fallback_tt.isna()).sum()
        logger.debug(f"Fallback matching filled {filled_count} events with bucketed median scheduled_tt")

    unmatched_trips = pq_df["scheduled_trip_id"].isna().sum()
    if unmatched_trips > 0:
        logger.warning(f"{unmatched_trips} bus events could not be matched to a scheduled trip")
    return pq_df[BUS_S3_COLUMNS]


def _average_bus_scheduled_headways(pq_df: pd.DataFrame, service_date: date) -> pd.DataFrame:
    """Bucket scheduled bus headways into 30-minute buckets to smooth the benchmark."""
    logger.debug(f"Calculating average scheduled bus headways for {len(pq_df)} events")
    start_time = pd.Timestamp(service_date.year, service_date.month, service_date.day)
    end_time = start_time + pd.Timedelta(hours=48)
    buckets = pd.date_range(start_time, end_time, freq="30min")

    _enriched = []
    for bucket in buckets:
        bucket_start = pd.to_datetime(bucket, unit="s").tz_localize(
            EASTERN_TIME, ambiguous=True, nonexistent="shift_forward"
        )
        bucket_end = pd.to_datetime(bucket + pd.Timedelta(minutes=30), unit="s").tz_localize(
            EASTERN_TIME, ambiguous=True, nonexistent="shift_forward"
        )
        filtered = pq_df[(pq_df["event_time"] >= bucket_start) & (pq_df["event_time"] < bucket_end)]

        average_scheduled_headway = filtered.groupby(RTE_DIR_STOP)["scheduled_headway"].mean()
        average_scheduled_headway = average_scheduled_headway.round(-1)

        enriched = filtered.merge(average_scheduled_headway, how="left", on=RTE_DIR_STOP, suffixes=["_lamp", ""])
        _enriched.append(enriched)
    return pd.concat(_enriched)[BUS_S3_COLUMNS]


def ingest_bus_pq_file(df: pd.DataFrame, service_date: date, local_archive_path: str | None = None) -> pd.DataFrame:
    """Process and transform columns for a full day's bus events."""
    logger.info(f"Processing {len(df)} raw bus events for service date {service_date}")

    rows_before = len(df)
    df = df[df["direction_id"].notna()]
    rows_dropped = rows_before - len(df)
    if rows_dropped > 0:
        logger.warning(f"Dropped {rows_dropped} rows with null direction_id")

    df["direction_id"] = df["direction_id"].astype("int16")
    df["service_date"] = df["service_date"].astype(str)
    df = df.rename(columns=BUS_COLUMN_RENAME_MAP)

    logger.info("Processing arrival/departure times")
    processed = _process_bus_arrival_departure_times(df)
    events_before = len(processed)
    processed = processed[processed["stop_id"].notna()]
    events_dropped = events_before - len(processed)
    if events_dropped > 0:
        logger.warning(f"Dropped {events_dropped} events with null stop_id")

    logger.info("Recalculating fields from GTFS")
    processed = _recalculate_bus_fields_from_gtfs(processed, service_date, local_archive_path)

    logger.info("Averaging scheduled headways")
    processed = _average_bus_scheduled_headways(processed, service_date)

    logger.info(f"Processing complete: {len(processed)} events ready for upload")
    return processed.sort_values(by=["event_time"])


def upload_bus_to_s3(group_key_and_events: Tuple[tuple, pd.DataFrame], service_date: date) -> None:
    """Upload bus events to S3, grouped by route-direction-stop."""
    (route_id, direction_id, stop_id), stop_events = group_key_and_events

    s3_key = BUS_S3_KEY_TEMPLATE.format(
        route_id=route_id,
        direction_id=direction_id,
        stop_id=stop_id,
        YYYY=service_date.year,
        _M=service_date.month,
        _D=service_date.day,
    )
    logger.debug(f"Uploading {len(stop_events)} events for {route_id}-{direction_id}-{stop_id}")
    try:
        s3.upload_df_as_csv(BUS_S3_BUCKET, s3_key, stop_events)
    except Exception as e:
        logger.error(f"Failed to upload bus events for {route_id}-{direction_id}-{stop_id}: {e}")
        raise
    return [(route_id, direction_id, stop_id)]


_parallel_upload = parallel.make_parallel(upload_bus_to_s3)


def ingest_bus_data(service_date: date, local_archive_path: str | None = None):
    """Ingest and upload bus LAMP data for a given service date."""
    logger.info(f"Starting bus LAMP data ingestion for service date {service_date}")
    try:
        df = fetch_bus_pq_file_from_remote(service_date)
    except ValueError as e:
        logger.error(f"Failed to fetch bus data for {service_date}: {e}")
        return
    except Exception as e:
        logger.exception(f"Unexpected error fetching bus data for {service_date}: {e}")
        raise

    try:
        processed = ingest_bus_pq_file(df, service_date, local_archive_path=local_archive_path)
    except Exception as e:
        logger.exception(f"Error processing bus data for {service_date}: {e}")
        raise

    # Group by route-direction-stop and parallel upload to S3
    group_event_groups = processed.groupby(RTE_DIR_STOP)
    num_groups = len(group_event_groups)
    logger.info(f"Uploading events for {num_groups} route-direction-stop groups to S3")
    try:
        _parallel_upload(group_event_groups, service_date)
    except Exception as e:
        logger.exception(f"Error uploading bus data for {service_date}: {e}")
        raise
    logger.info(f"Bus LAMP data ingestion complete for service date {service_date}")


def ingest_today_bus_data():
    """Ingest and upload today's bus LAMP data."""
    service_date = get_current_service_date()
    logger.info(f"Ingesting today's bus data (service date: {service_date})")
    ingest_bus_data(service_date)


def ingest_yesterday_bus_data():
    """Ingest and upload yesterday's bus LAMP data."""
    service_date = get_current_service_date() - pd.Timedelta(days=1)
    logger.info(f"Ingesting yesterday's bus data (service date: {service_date})")
    ingest_bus_data(service_date)


if __name__ == "__main__":
    import os

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    ingest_today_bus_data()
