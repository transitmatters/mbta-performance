"""Run a daily "yesterday" job, and retry any recent day that never finished.

The rail and bus yesterday jobs each run once a day. Before this, a run that failed (a LAMP 5xx, a
GTFS feed that wasn't built yet, a parquet schema change) lost that day: nothing ran it again.

Each run writes a small marker per service date it finishes, and checks the last LOOKBACK_DAYS for
dates without one. It does yesterday first, then the rest oldest first, so the day closest to
leaving the window goes next. Days that fail are logged, the others still run, and the run raises
at the end so the Lambda reports a failure. That fires the Datadog error metrics and EventBridge's
async retries, which only redo the days still missing a marker.

Cost per job: LOOKBACK_DAYS HEADs and one marker PUT a day, about 210 HEADs and 30 PUTs a month
(under $0.001). A retried day costs what its original run would have.
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from .. import s3
from ..date import get_current_service_date

logger = logging.getLogger(__name__)

S3_BUCKET = "tm-mbta-performance"
# One marker per finished service date, e.g. Events-lamp/ingest-markers/daily-data/2026-11-10.json.
# Kept outside the data prefixes so nothing that lists those mistakes a marker for a stop.
MARKER_KEY_TEMPLATE = "Events-lamp/ingest-markers/{dataset}/{service_date}.json"

# How far back each run looks for unfinished days. A missing GTFS feed is usually built within
# minutes and LAMP outages are short, so most misses are caught the next day. A week leaves time
# for a fix that needs a code change. Anything older needs the backfill scripts.
LOOKBACK_DAYS = 7


def marker_key(dataset: str, service_date: date) -> str:
    return MARKER_KEY_TEMPLATE.format(dataset=dataset, service_date=service_date.isoformat())


def is_ingested(dataset: str, service_date: date) -> bool:
    return s3.head_etag(S3_BUCKET, marker_key(dataset, service_date)) is not None


def mark_ingested(dataset: str, service_date: date, files: int) -> None:
    body = {
        "service_date": service_date.isoformat(),
        "files": files,
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    s3.upload(S3_BUCKET, marker_key(dataset, service_date), json.dumps(body).encode("utf8"), compress=False)


def days_to_ingest(dataset: str, today: date, lookback_days: int = LOOKBACK_DAYS) -> list[date]:
    """The service dates in the window that have no marker, in the order they should run."""
    yesterday = today - timedelta(days=1)
    older = [today - timedelta(days=n) for n in range(lookback_days, 1, -1)]
    return [day for day in [yesterday, *older] if not is_ingested(dataset, day)]


def ingest_recent_days(
    dataset: str,
    ingest_day: Callable[[date], int],
    seconds_per_day: float,
    context=None,
    lookback_days: int = LOOKBACK_DAYS,
) -> None:
    """Run ingest_day for yesterday and any earlier day in the window without a marker.

    Args:
        dataset: the Events-lamp prefix the job writes, used to name its markers.
        ingest_day: ingests one service date and returns the number of files it wrote.
        seconds_per_day: how much time must be left before another day is started.
        context: the Lambda context. When given, days that won't fit before the timeout are left
            for the next run instead of being cut off partway through.
    """
    pending = days_to_ingest(dataset, get_current_service_date(), lookback_days)
    if not pending:
        logger.info(f"{dataset}: every day in the last {lookback_days} is already ingested")
        return
    logger.info(f"{dataset}: ingesting {', '.join(map(str, pending))}")

    failed = []
    last_error = None
    for i, service_date in enumerate(pending):
        if context is not None and context.get_remaining_time_in_millis() < seconds_per_day * 1000:
            logger.warning(f"{dataset}: not enough time left for {', '.join(map(str, pending[i:]))}; next run")
            break
        try:
            files = ingest_day(service_date)
            mark_ingested(dataset, service_date, files)
        except Exception as e:
            logger.error(f"{dataset}: {service_date} failed: {e!r}")
            failed.append(service_date)
            last_error = e

    if failed:
        raise RuntimeError(
            f"{dataset}: failed to ingest {', '.join(map(str, failed))}. Later runs retry each day "
            f"for {lookback_days} days; after that, use the backfill script."
        ) from last_error
