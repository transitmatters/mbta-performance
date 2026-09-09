"""Ingest MBTA alert history from the LAMP real-time alerts parquet archive.

MBTA's LAMP project publishes a single, ever-growing parquet file containing every
version of every alert MBTA has ever issued. This module reads it and reshapes it
into the same daily `Alerts/v3/{date}.json.gz`-style files that `data-ingestion`'s
live v3 API poller (`ingestor/chalicelib/alerts.py::save_v3_alerts`) has always
written, so every existing consumer -- t-performance-dash's `s3_alerts.py`, and the
alert-delay aggregation built on top of it -- keeps working unchanged. We write to a
separate `Alerts/lamp/` prefix so the switchover is a routing change on the read
side, not a destructive rewrite.

Two properties of the source file drive most of the logic here:

1. It is append-only and versioned. Every time an alert is revised, LAMP appends a
   new row set (keyed by `last_modified_timestamp`) rather than updating in place.
   When an alert *closes*, its final version drops `active_period` entirely and
   sets `closed_timestamp` instead -- so "the latest version of an alert" must mean
   "the latest version that still has an active_period," not the literal last row.
   Taking a naive max(last_modified_timestamp) per alert silently discards ~99% of
   alerts (their true active periods live in an earlier version).

2. Rows are a cross product of (alert version x active_period x informed_entity),
   so a single alert can have thousands of rows. All fan-in below is done as
   whole-frame `drop_duplicates()` passes rather than per-alert `groupby().apply()`
   -- the latter takes minutes across the ~400k distinct alerts in the file, the
   former takes seconds.

`effect_detail` (not `effect`) is what corresponds to the MBTA v3 API's `effect`
attribute (e.g. DELAY, DETOUR, SHUTTLE) -- LAMP's own `effect` column is the
coarser GTFS-RT enum (OTHER_EFFECT, NO_SERVICE, ...) and is not used here.

All timestamps are read from the `*_timestamp` (epoch seconds) columns rather than
the parallel `*_datetime` (naive Eastern local) columns -- converting from an
unambiguous epoch avoids the twice-yearly DST ambiguity of localizing a naive
timestamp by hand.
"""

import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Iterator, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from .. import parallel, s3
from ..date import EASTERN_TIME

logger = logging.getLogger(__name__)

LAMP_ALERTS_URL = "https://performancedata.mbta.com/lamp/tableau/alerts/LAMP_RT_ALERTS.parquet"
S3_BUCKET = "tm-mbta-performance"
S3_KEY_TEMPLATE = "Alerts/lamp/{YYYY_MM_DD}.json.gz"
LOCAL_PARQUET_PATH = "/tmp/lamp_alerts.parquet"

# The scheduled job only rebuilds a rolling window around today -- comfortably
# inside Lambda memory/time limits and self-healing day to day. A full rebuild of
# all history is a separate local script: chalicelib/lamp/backfill/alerts.py.
LOOKBACK_DAYS = 30
LOOKAHEAD_DAYS = 90

# Alert service dates roll over at 3am ET, matching chalicelib/date.py::service_date
# and data-ingestion's get_current_service_date (the producer of Alerts/v3/).
SERVICE_DATE_ROLLOVER_HOUR = 3

# We skip `description_text.translation.text` and `recurrence_text.translation.text`:
# together they account for ~170MB of the file's ~480MB uncompressed, and nothing
# downstream (t-performance-dash's data_funcs.alerts) reads either field.
ALERT_COLUMNS = [
    "id",
    "cause",
    "effect_detail",  # the MBTA v3 API's "effect" -- see module docstring
    "severity",
    "alert_lifecycle",
    "header_text.translation.text",
    "service_effect_text.translation.text",
    "created_timestamp",
    "last_modified_timestamp",
    "closed_timestamp",
    "active_period.start_timestamp",
    "active_period.end_timestamp",
    "informed_entity.route_id",
    "informed_entity.route_type",
    "informed_entity.direction_id",
    "informed_entity.stop_id",
    "informed_entity.activities",
]

LAST_SEEN_COLUMNS = ["id", "last_modified_timestamp", "closed_timestamp"]

PERIOD_COLUMNS = ["id", "active_period.start_timestamp", "active_period.end_timestamp", "eff_end"]
ENTITY_COLUMNS = [
    "id",
    "informed_entity.route_id",
    "informed_entity.stop_id",
    "informed_entity.route_type",
    "informed_entity.direction_id",
    "informed_entity.activities",
]


def fetch_alerts_parquet(dest: str = LOCAL_PARQUET_PATH) -> str:
    """Stream the LAMP alerts parquet (~130MB) to local disk.

    Streamed rather than buffered in memory -- there's no need to hold a second
    130MB copy in RAM alongside the eventual pandas frames.
    """
    logger.info(f"Downloading LAMP alerts parquet from {LAMP_ALERTS_URL}")
    with requests.get(LAMP_ALERTS_URL, stream=True) as response:
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    return dest


def last_seen_by_alert(path: str) -> pd.DataFrame:
    """Per alert id, the most recent last_modified_timestamp and closed_timestamp (if ever set).

    This has to be computed across the *whole* file independent of the
    active-period filtering in `read_latest_versions`: a closed alert's
    closed_timestamp only appears on its final version, and that version's
    active_period is null, so it would otherwise be filtered out before we ever
    see it.
    """
    table = pq.read_table(path, columns=LAST_SEEN_COLUMNS)
    df = table.to_pandas()
    return df.groupby("id").agg(
        last_modified_timestamp=("last_modified_timestamp", "max"),
        closed_timestamp=("closed_timestamp", "max"),
    )


def _select_winning_versions(path: str) -> pa.Table:
    """(id, last_modified_timestamp) identifying the latest active-period-bearing
    version of every alert in the file -- computed globally, independent of any
    window. This has to be a separate global pass: picking "latest" only among
    rows that already happen to overlap a window would return a stale, superseded
    active_period whenever an alert's true latest version was rescheduled to fall
    outside that window.
    """
    table = pq.read_table(path, columns=["id", "last_modified_timestamp", "active_period.start_timestamp"])
    table = table.filter(pc.is_valid(table["active_period.start_timestamp"]))
    winners = (
        table.select(["id", "last_modified_timestamp"]).group_by("id").aggregate([("last_modified_timestamp", "max")])
    )
    return winners.select(["id", "last_modified_timestamp_max"]).rename_columns(["id", "last_modified_timestamp"])


def read_latest_versions(path: str, window: tuple, last_seen: pd.DataFrame) -> pd.DataFrame:
    """Read the latest active-period-bearing version of every alert overlapping `window`.

    Filtering happens in Arrow, row-group batch by batch, before anything becomes a
    pandas object: converting the full file to pandas costs several GB of RSS,
    while window-filtering first keeps peak memory around 1GB.

    Each alert's effective end (used only for bucketing into day files, not for the
    "end" field we emit) is: its active_period.end if present, else the last time
    MBTA marked it closed, else the last time it was modified at all. We do not
    extend an unclosed alert's effective end to "today" -- most such alerts are
    years-old and orphaned rather than genuinely ongoing, and today's alerts are
    covered by the live v3 poller regardless (see t-performance-dash's same-day
    fallback), so there's no accuracy gained and a real risk of a stale alert
    reappearing in every day file forever.
    """
    window_start = datetime.combine(window[0], datetime.min.time(), EASTERN_TIME).timestamp()
    window_end = datetime.combine(window[1] + timedelta(days=1), datetime.min.time(), EASTERN_TIME).timestamp()

    winners = _select_winning_versions(path)
    last_seen_table = pa.Table.from_pandas(last_seen.reset_index(), preserve_index=False).rename_columns(
        ["id", "_all_versions_last_modified", "_all_versions_closed"]
    )

    parquet_file = pq.ParquetFile(path)
    batches = []
    for record_batch in parquet_file.iter_batches(columns=ALERT_COLUMNS, batch_size=250_000):
        batch = pa.Table.from_batches([record_batch])
        batch = batch.filter(pc.is_valid(batch["active_period.start_timestamp"]))
        if batch.num_rows == 0:
            continue
        # keep only rows belonging to each alert's one winning (latest AP-bearing) version
        batch = batch.join(winners, keys=["id", "last_modified_timestamp"], join_type="left semi")
        if batch.num_rows == 0:
            continue
        batch = batch.join(last_seen_table, keys="id", join_type="left outer")
        eff_end = pc.coalesce(
            batch["active_period.end_timestamp"], batch["_all_versions_closed"], batch["_all_versions_last_modified"]
        )
        batch = batch.append_column("eff_end", eff_end)
        batch = batch.filter(
            (pc.field("active_period.start_timestamp") < window_end) & (pc.field("eff_end") >= window_start)
        )
        if batch.num_rows:
            batches.append(batch.drop_columns(["_all_versions_last_modified", "_all_versions_closed"]))

    if not batches:
        return pd.DataFrame(columns=[*ALERT_COLUMNS, "eff_end"])

    return pa.concat_tables(batches).to_pandas()


def _iso(epoch_seconds) -> Optional[str]:
    """Epoch seconds -> Eastern-local ISO 8601, e.g. '2026-09-19T03:00:00-04:00'."""
    if epoch_seconds is None or pd.isna(epoch_seconds):
        return None
    return datetime.fromtimestamp(int(epoch_seconds), EASTERN_TIME).isoformat()


def _clean(value):
    """NaN -> None, so it serializes as JSON null instead of leaking as `NaN`."""
    return None if pd.isna(value) else value


def service_dates_for(start_ts: float, end_ts: float) -> Iterator[date]:
    """Every service date (3am ET rollover) that the closed interval [start_ts, end_ts] touches."""
    start_day = (
        datetime.fromtimestamp(int(start_ts), EASTERN_TIME) - timedelta(hours=SERVICE_DATE_ROLLOVER_HOUR)
    ).date()
    end_day = (datetime.fromtimestamp(int(end_ts), EASTERN_TIME) - timedelta(hours=SERVICE_DATE_ROLLOVER_HOUR)).date()
    day = start_day
    while day <= end_day:
        yield day
        day += timedelta(days=1)


def build_v3_alerts(df: pd.DataFrame, window: Optional[tuple] = None) -> tuple:
    """Reshape latest-version rows into v3-JSON:API-shaped alert objects, plus the
    day -> [alert_id] index used to write the daily files.

    `window`, if given, clips which days are written. An alert's full active period
    can extend well outside the caller's window (e.g. a still-open alert that
    started months before a 30-day lookback) -- its data is unaffected, but we only
    want the rolling daily job touching day files it actually owns, not silently
    overwriting historical files a backfill already populated.

    Returns (alerts: dict[str, dict], day_index: dict[date, list[str]]).
    """
    attrs = df.drop_duplicates(subset=["id"]).set_index("id")

    periods_by_id = defaultdict(list)
    day_index = defaultdict(list)
    for alert_id, start, end, eff_end in df[PERIOD_COLUMNS].drop_duplicates().itertuples(index=False):
        periods_by_id[alert_id].append({"start": _iso(start), "end": _iso(end)})
        for day in service_dates_for(start, eff_end if not pd.isna(eff_end) else start):
            if window and not (window[0] <= day <= window[1]):
                continue
            day_index[day].append(str(alert_id))

    entities_by_id = defaultdict(list)
    for alert_id, route, stop, route_type, direction_id, activities in (
        df[ENTITY_COLUMNS].drop_duplicates().itertuples(index=False)
    ):
        # Match the v3 API's own convention of omitting absent keys, rather than
        # emitting `"route": null` -- t-performance-dash's routes_for_alert does
        # `if "route" in informed_entity: routes.add(...)`, and a present-but-null
        # key would add a bare `None` into that route set.
        entity = {}
        if not pd.isna(route):
            entity["route"] = route
        if not pd.isna(stop):
            entity["stop"] = stop
        if not pd.isna(route_type):
            entity["route_type"] = int(route_type)
        if not pd.isna(direction_id):
            entity["direction_id"] = int(direction_id)
        entity["activities"] = activities.split("|") if isinstance(activities, str) else []
        entities_by_id[alert_id].append(entity)

    alerts = {}
    for alert_id, row in attrs.iterrows():
        key = str(alert_id)
        alerts[key] = {
            "id": key,
            "type": "alert",
            "attributes": {
                "effect": _clean(row["effect_detail"]),
                "cause": _clean(row["cause"]),
                "severity": None if pd.isna(row["severity"]) else int(row["severity"]),
                "lifecycle": _clean(row["alert_lifecycle"]),
                "header": _clean(row["header_text.translation.text"]),
                "short_header": None,
                "service_effect": _clean(row["service_effect_text.translation.text"]),
                "created_at": _iso(row["created_timestamp"]),
                "updated_at": _iso(row["last_modified_timestamp"]),
                "active_period": periods_by_id[alert_id],
                "informed_entity": entities_by_id[alert_id],
            },
        }
    return alerts, day_index


def upload_day_alerts(day: date, alerts: dict, day_index: dict) -> list:
    """Write one day's alerts to S3. `day` is the value make_parallel multiplexes on."""
    ids = day_index[day]
    payload = json.dumps({alert_id: alerts[alert_id] for alert_id in ids}).encode("utf8")
    s3_key = S3_KEY_TEMPLATE.format(YYYY_MM_DD=str(day))
    logger.debug(f"Uploading {len(ids)} alerts for {day} to s3://{S3_BUCKET}/{s3_key}")
    try:
        s3.upload(S3_BUCKET, s3_key, payload, compress=True)
    except Exception as e:
        logger.error(f"Failed to upload alerts for {day} to S3: {e}")
        raise
    return [day]


_parallel_upload_days = parallel.make_parallel(upload_day_alerts)


def ingest_lamp_alerts(lookback_days: int = LOOKBACK_DAYS, lookahead_days: int = LOOKAHEAD_DAYS) -> None:
    """Rebuild the rolling window of `Alerts/lamp/{date}.json.gz` day files.

    Runs daily and re-derives the window from scratch each time (rather than
    tracking incremental state) so retroactive corrections in the LAMP source data
    propagate automatically.
    """
    today = date.today()
    window = (today - timedelta(days=lookback_days), today + timedelta(days=lookahead_days))
    logger.info(f"Ingesting LAMP alerts for window {window[0]} to {window[1]}")

    path = fetch_alerts_parquet()
    try:
        last_seen = last_seen_by_alert(path)
        df = read_latest_versions(path, window, last_seen)
        alerts, day_index = build_v3_alerts(df, window=window)
        logger.info(f"Rebuilding {len(day_index)} day files covering {len(alerts)} alerts")
        _parallel_upload_days(day_index.keys(), alerts, day_index)
    finally:
        if os.path.exists(path):
            os.remove(path)
    logger.info("LAMP alerts ingestion complete")
