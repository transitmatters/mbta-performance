import io
import logging
import os
from datetime import date
from typing import Tuple

import pandas as pd
import requests

from .. import parallel, s3
from ..date import EASTERN_TIME, format_dateint, get_current_service_date
from ..gtfs import fetch_stop_times_from_gtfs
from .constants import (
    LAMP_COLUMNS,
    RED_LINE_ASHMONT_STOPS,
    RED_LINE_BRAINTREE_STOPS,
    S3_COLUMNS,
    STOP_ID_NUMERIC_MAP,
)

logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)

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
# but only if it happens before December 2023 as it's an unreliable indicator of actual revenue service.
# All trips after that are properly ignored by the LAMP system, and don't appear in the dataset anymore.
TRIP_IDS_TO_DROP = ("NONREV-",)

# defining these columns in particular becasue we use them everywhere
RTE_DIR_STOP = ["route_id", "direction_id", "stop_id"]


def _derive_gtfs_branch_route_id(gtfs_stops: pd.DataFrame) -> pd.DataFrame:
    """Derive branch_route_id for GTFS trips based on their stops.

    For Red Line trips, we determine the branch (Ashmont/Braintree) by looking
    at which branch-specific stops the trip visits. This is needed because GTFS
    uses route_id="Red" for both branches, unlike Green Line which already has
    distinct route_ids (Green-B, Green-C, etc.).

    For all other lines, we use route_id as branch_route_id since it already
    provides the necessary distinction for matching.
    """

    def get_branch_for_trip(trip_stops: pd.DataFrame) -> str:
        route_id = trip_stops["route_id"].iloc[0]

        # Red Line needs special handling - derive branch from stops
        if route_id == "Red":
            stop_ids = set(trip_stops["stop_id"].astype(str))
            has_ashmont = bool(stop_ids & RED_LINE_ASHMONT_STOPS)
            has_braintree = bool(stop_ids & RED_LINE_BRAINTREE_STOPS)

            if has_ashmont and not has_braintree:
                return "Red-A"
            elif has_braintree and not has_ashmont:
                return "Red-B"
            # Trunk-only trip - fallback to route_id
            return route_id

        # For all other lines (including Green-B, Green-C, etc.), use route_id
        return route_id

    # Group by trip_id and determine branch for each trip
    trip_branches = gtfs_stops.groupby("trip_id", group_keys=False).apply(get_branch_for_trip, include_groups=False)
    trip_branches.name = "branch_route_id"

    # Merge branch info back to gtfs_stops
    gtfs_stops = gtfs_stops.merge(trip_branches, on="trip_id", how="left")
    return gtfs_stops


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
    logger.debug(f"Processing arrival/departure times for {len(pq_df)} rows")
    pq_df["dep_time"] = pd.to_datetime(pq_df["move_timestamp"], unit="s", utc=True).dt.tz_convert(EASTERN_TIME)
    pq_df["arr_time"] = pd.to_datetime(pq_df["stop_timestamp"], unit="s", utc=True).dt.tz_convert(EASTERN_TIME)
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
    result = pd.concat([arr_df, dep_df])
    logger.debug(f"Processed arrival/departure times: {len(arr_df)} arrivals, {len(dep_df)} departures")
    return result


def fetch_pq_file_from_remote(service_date: date) -> pd.DataFrame:
    """Fetch a parquet file from LAMP for a given service date."""
    # TODO(check if file exists in index, throw if it doesn't)
    url = RAPID_DAILY_URL_TEMPLATE.format(YYYY_MM_DD=service_date.strftime("%Y-%m-%d"))
    logger.info(f"Fetching LAMP parquet file from {url}")
    result = requests.get(url)

    if result.status_code != 200:
        logger.error(f"Failed to fetch LAMP parquet file from {url}. Status code: {result.status_code}")
        raise ValueError(f"Failed to fetch LAMP parquet file from {url}. Status code: {result.status_code}")

    logger.info(f"Successfully fetched LAMP parquet file ({len(result.content)} bytes)")
    df = pd.read_parquet(
        io.BytesIO(result.content),
        columns=LAMP_COLUMNS,
        engine="pyarrow",
        # NB: Even through parquet files are compressed with columnar metadata, pandas will sometimes override them
        # if the columns contain nulls. This is important as the move/stop times are nullable int64 epoch timestamps,
        # which will overflow if read in as floats.
        # https://pandas.pydata.org/docs/user_guide/integer_na.html#nullable-integer-data-type
        dtype_backend="numpy_nullable",
    )
    logger.info(f"Parsed parquet file: {len(df)} rows")
    return df


def _derive_lamp_branch_route_id(pq_df: pd.DataFrame) -> pd.DataFrame:
    """Derive branch_route_id for LAMP trips based on their stops.

    For Red Line trips, we determine the branch (Ashmont/Braintree) by looking
    at which branch-specific stops the trip visits. This is needed because LAMP
    uses route_id="Red" for both branches.

    For all other lines, we use route_id as branch_route_id since it already
    provides the necessary distinction (e.g., Green-B, Green-C, etc.).
    """

    def get_branch_for_trip(trip_events: pd.DataFrame) -> str:
        route_id = trip_events["route_id"].iloc[0]

        # Red Line needs special handling - derive branch from stops
        if route_id == "Red":
            stop_ids = set(trip_events["stop_id"].astype(str))
            has_ashmont = bool(stop_ids & RED_LINE_ASHMONT_STOPS)
            has_braintree = bool(stop_ids & RED_LINE_BRAINTREE_STOPS)

            if has_ashmont and not has_braintree:
                return "Red-A"
            elif has_braintree and not has_ashmont:
                return "Red-B"
            # Trunk-only trip - fallback to route_id
            return route_id

        # For all other lines (including Green-B, Green-C, etc.), use route_id
        return route_id

    # Group by trip_id and determine branch for each trip
    trip_branches = pq_df.groupby("trip_id", group_keys=False).apply(get_branch_for_trip, include_groups=False)
    trip_branches.name = "branch_route_id"

    # Merge branch info back to pq_df
    pq_df = pq_df.merge(trip_branches, on="trip_id", how="left")
    return pq_df


def _recalculate_fields_from_gtfs(pq_df: pd.DataFrame, service_date: date):
    """Enrich LAMP data with GTFS data for some schedule information."""
    trip_ids = pq_df["trip_id"].unique()
    logger.info(f"Enriching LAMP data with GTFS for {len(trip_ids)} unique trips on {service_date}")
    gtfs_stops = fetch_stop_times_from_gtfs(trip_ids, service_date)
    logger.debug(f"Fetched {len(gtfs_stops)} GTFS stop times")
    gtfs_stops = gtfs_stops.sort_values(by="arrival_time")

    # remove the reported travel times, because we use a slightly different metric.
    pq_df = pq_df.drop(columns=["scheduled_tt"])

    # Derive branch_route_id for both LAMP and GTFS data to enable branch-aware matching
    # This is critical for Red Line trips originating on the trunk and ending on Ashmont/Braintree
    logger.debug("Deriving branch_route_id for LAMP trips")
    pq_df = _derive_lamp_branch_route_id(pq_df)
    logger.debug("Deriving branch_route_id for GTFS trips")
    gtfs_stops = _derive_gtfs_branch_route_id(gtfs_stops)

    # we could do this groupby/min/merge in sql, but let's keep our computations in
    # pandas to stay consistent across services
    trip_start_times = gtfs_stops.groupby("trip_id").arrival_time.transform("min")
    gtfs_stops["scheduled_tt"] = gtfs_stops["arrival_time"] - trip_start_times
    gtfs_stops["arrival_time"] = gtfs_stops["arrival_time"].astype(float)

    # Use branch-aware matching for all routes
    # For Red Line, branch_route_id distinguishes Ashmont (Red-A) from Braintree (Red-B)
    # For other lines, branch_route_id equals route_id (e.g., Green-B, Orange, Blue)
    match_columns = RTE_DIR_STOP + ["branch_route_id"]

    # assign each actual trip a scheduled trip_id, based on when it started the route
    route_starts = pq_df.loc[pq_df.groupby("trip_id").event_time.idxmin()]
    route_starts["arrival_time"] = (
        route_starts.event_time - pd.Timestamp(service_date).tz_localize(EASTERN_TIME)
    ).dt.total_seconds()

    trip_id_map = pd.merge_asof(
        route_starts.sort_values(by="arrival_time"),
        gtfs_stops[match_columns + ["arrival_time", "trip_id"]].drop_duplicates(),
        on="arrival_time",
        direction="nearest",
        by=match_columns,
        suffixes=["", "_scheduled"],
    )
    trip_id_map = trip_id_map.drop_duplicates("trip_id").set_index("trip_id").trip_id_scheduled

    # use the scheduled trip matching to get the scheduled traveltime
    pq_df["scheduled_trip_id"] = pq_df.trip_id.map(trip_id_map)
    # For scheduled_tt lookup, only match on scheduled_trip_id and stop_id
    # Don't match on route_id/branch_route_id because interlined trips (e.g., Green-E→Green-D)
    # have different route_ids in LAMP vs GTFS for the same scheduled trip
    pq_df = pd.merge(
        pq_df,
        gtfs_stops[["trip_id", "stop_id", "scheduled_tt"]],
        how="left",
        left_on=["scheduled_trip_id", "stop_id"],
        right_on=["trip_id", "stop_id"],
        suffixes=["", "_gtfs"],
    )

    # For interlined trips (e.g., Green-E→Green-D), the matched GTFS trip may not contain
    # stops from the second part of the journey. For these events, use the median
    # scheduled_tt for that route/direction/stop in 30-minute buckets to provide a smoothed benchmark.
    missing_tt_mask = pq_df["scheduled_tt"].isna()
    if missing_tt_mask.any():
        logger.debug(f"Attempting fallback scheduled_tt matching for {missing_tt_mask.sum()} events")

        # Bucket GTFS data into 30-minute windows and calculate median scheduled_tt
        # per route/direction/stop per bucket (same smoothing approach as headways)
        gtfs_stops["time_bucket"] = (gtfs_stops["arrival_time"] // 1800).astype(int)  # 1800 seconds = 30 min
        bucketed_median_tt = gtfs_stops.groupby(RTE_DIR_STOP + ["time_bucket"])["scheduled_tt"].median()

        # Calculate time bucket for missing events
        event_seconds = (
            pq_df.loc[missing_tt_mask, "event_time"] - pd.Timestamp(service_date).tz_localize(EASTERN_TIME)
        ).dt.total_seconds()
        event_buckets = (event_seconds // 1800).astype(int)

        # Look up median scheduled_tt for each missing event's route/direction/stop/bucket
        lookup_keys = pq_df.loc[missing_tt_mask, RTE_DIR_STOP].copy()
        lookup_keys["time_bucket"] = event_buckets.values
        fallback_tt = lookup_keys.apply(lambda row: bucketed_median_tt.get(tuple(row)), axis=1)

        pq_df.loc[missing_tt_mask, "scheduled_tt"] = fallback_tt.values
        filled_count = (~fallback_tt.isna()).sum()
        logger.debug(f"Fallback matching filled {filled_count} events with bucketed median scheduled_tt")

    unmatched_trips = pq_df["scheduled_trip_id"].isna().sum()
    if unmatched_trips > 0:
        logger.warning(f"{unmatched_trips} events could not be matched to a scheduled trip")
    logger.debug(f"GTFS enrichment complete: {len(pq_df)} events")
    return pq_df[S3_COLUMNS]


def _average_scheduled_headways(pq_df: pd.DataFrame, service_date: date) -> pd.DataFrame:
    """Bucket scheduled headways into 30 minute buckets.

    We do this so as to smooth the benchmark headways for the data dashboard.
    TODO: do this with branch headways as well
    TODO: group green line branches together in trunk headways
    """
    logger.debug(f"Calculating average scheduled headways for {len(pq_df)} events")
    # service date starts at 5:30am, but there are enough very early/late departures that we dont want to be opinionated
    start_time = pd.Timestamp(service_date.year, service_date.month, service_date.day)
    end_time = start_time + pd.Timedelta(hours=48)
    buckets = pd.date_range(start_time, end_time, freq="30min")

    _enriched_trips = []
    for bucket in buckets:
        bucket_start = pd.to_datetime(bucket, unit="s").tz_localize(
            EASTERN_TIME, ambiguous=True, nonexistent="shift_forward"
        )
        bucket_end = pd.to_datetime(bucket + pd.Timedelta(minutes=30), unit="s").tz_localize(
            EASTERN_TIME, ambiguous=True, nonexistent="shift_forward"
        )
        filtered_trips = pq_df[(pq_df["event_time"] >= bucket_start) & (pq_df["event_time"] < bucket_end)]

        # Get the average headway per route
        average_scheduled_headway = filtered_trips.groupby(RTE_DIR_STOP)["scheduled_headway"].mean()
        average_scheduled_headway = average_scheduled_headway.round(-1)  # Round to the nearest 10seconds

        enriched_trip = filtered_trips.merge(
            average_scheduled_headway, how="left", on=RTE_DIR_STOP, suffixes=["_lamp", ""]
        )
        _enriched_trips.append(enriched_trip)
    return pd.concat(_enriched_trips)[S3_COLUMNS]


def ingest_pq_file(pq_df: pd.DataFrame, service_date: date) -> pd.DataFrame:
    """Process and tranform columns for the full day's events."""
    logger.info(f"Processing {len(pq_df)} raw events for service date {service_date}")
    pq_df["direction_id"] = pq_df["direction_id"].astype("int16")
    pq_df["service_date"] = pq_df["service_date"].apply(format_dateint)
    # use trunk headway metrics as default, and add branch metrics when it makes sense.
    # TODO: verify and recalculate headway metrics if necessary!
    pq_df = pq_df.rename(columns=COLUMN_RENAME_MAP)
    # Live data will sometimes report an aliased version of stop_id different
    # from that which GTFS reports in its schedule. Replace for better schedule matching.
    pq_df["stop_id"] = pq_df["stop_id"].replace(STOP_ID_NUMERIC_MAP)
    # drop non-revenue producing events
    cutoff_date = format_dateint(20231130)
    initial_count = len(pq_df)
    pq_df = pq_df[~((pq_df["trip_id"].str.startswith(TRIP_IDS_TO_DROP)) & (pq_df["service_date"] < cutoff_date))]
    dropped_nonrev = initial_count - len(pq_df)
    if dropped_nonrev > 0:
        logger.debug(f"Dropped {dropped_nonrev} non-revenue events")

    logger.info("Processing arrival/departure times")
    processed_daily_events = _process_arrival_departure_times(pq_df)
    events_before_filter = len(processed_daily_events)
    processed_daily_events = processed_daily_events[processed_daily_events["stop_id"].notna()]
    events_dropped = events_before_filter - len(processed_daily_events)
    if events_dropped > 0:
        logger.warning(f"Dropped {events_dropped} events with null stop_id")

    logger.info("Recalculating fields from GTFS")
    processed_daily_events = _recalculate_fields_from_gtfs(processed_daily_events, service_date)

    logger.info("Averaging scheduled headways")
    processed_daily_events = _average_scheduled_headways(processed_daily_events, service_date)

    logger.info(f"Processing complete: {len(processed_daily_events)} events ready for upload")
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
    logger.debug(f"Uploading {len(stop_events)} events for stop {stop_id} to s3://{S3_BUCKET}/{s3_key}")
    # _local_save(s3_key, stop_events)
    try:
        s3.upload_df_as_csv(S3_BUCKET, s3_key, stop_events)
    except Exception as e:
        logger.error(f"Failed to upload events for stop {stop_id} to S3: {e}")
        raise
    return [stop_id]


_parallel_upload = parallel.make_parallel(upload_to_s3)


def ingest_lamp_data(service_date: date):
    logger.info(f"Starting LAMP data ingestion for service date {service_date}")
    try:
        pq_df = fetch_pq_file_from_remote(service_date)
    except ValueError as e:
        # If we can't fetch the file, we can't process it
        logger.error(f"Failed to fetch LAMP data for {service_date}: {e}")
        return
    except Exception as e:
        logger.exception(f"Unexpected error fetching LAMP data for {service_date}: {e}")
        raise

    try:
        processed_daily_events = ingest_pq_file(pq_df, service_date)
    except Exception as e:
        logger.exception(f"Error processing LAMP data for {service_date}: {e}")
        raise

    # split daily events by stop_id and parallel upload to s3
    stop_event_groups = processed_daily_events.groupby("stop_id")
    num_stops = len(stop_event_groups)
    logger.info(f"Uploading events for {num_stops} stops to S3")
    try:
        _parallel_upload(stop_event_groups, service_date)
    except Exception as e:
        logger.exception(f"Error uploading LAMP data for {service_date}: {e}")
        raise
    logger.info(f"LAMP data ingestion complete for service date {service_date}")


def ingest_today_lamp_data():
    """Ingest and upload today's LAMP data."""
    service_date = get_current_service_date()
    logger.info(f"Ingesting today's LAMP data (service date: {service_date})")
    ingest_lamp_data(service_date)


def ingest_yesterday_lamp_data():
    """Ingest and upload yesterday's LAMP data."""
    service_date = get_current_service_date() - pd.Timedelta(days=1)
    logger.info(f"Ingesting yesterday's LAMP data (service date: {service_date})")
    ingest_lamp_data(service_date)


if __name__ == "__main__":
    import os

    # Configure logging for local execution
    # Set LOG_LEVEL=DEBUG in environment to enable debug logging
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    ingest_today_lamp_data()
