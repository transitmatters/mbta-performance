import logging
import os
import sys
from datetime import date, timedelta
from typing import Optional

from .. import alerts

logger = logging.getLogger(__name__)

# LAMP's alert archive is effectively empty before this date (fewer than 20 new
# alerts a day; ~2.5k-8k a month from here on). t-performance-dash should not
# route dates before this to the LAMP source.
EARLIEST_LAMP_ALERTS_DATA = date(2019, 9, 15)


def backfill_all_alerts(start: Optional[date] = None, end: Optional[date] = None):
    """Rebuild every historical Alerts/lamp/{date}.json.gz day file from `start`
    (default EARLIEST_LAMP_ALERTS_DATA) through `end` (default yesterday -- today
    is left to the live v3 poller, see alerts.LOOKBACK_DAYS).

    Unlike the scheduled Lambda job (which only rebuilds a rolling window), this
    processes the entire LAMP alerts archive. It's a one-time/occasional local
    script, not something that runs on a schedule or in Lambda -- reading the full
    history into pandas in one shot costs several GB of RSS.

    We keep that memory bounded by chunking the read into one-year windows (the
    download and the two global, window-independent passes -- last_seen_by_alert
    and select_winning_versions -- still happen once) at the cost of re-scanning
    the full parquet's per-row columns once per chunk -- an acceptable trade for a
    script that runs rarely and doesn't need to fit in a Lambda's memory ceiling.
    """
    start = max(start or EARLIEST_LAMP_ALERTS_DATA, EARLIEST_LAMP_ALERTS_DATA)
    end = end or date.today() - timedelta(days=1)
    confirmation = input(
        f"This will rebuild every Alerts/lamp/*.json.gz day file from {start} to {end} "
        f"(~{(end - start).days + 1:,} files, a few minutes). Proceed? (yes/no): "
    )
    if confirmation.lower() not in ("yes", "y"):
        print("You must enter 'yes' to proceed. Exiting.")
        exit(1)

    path = alerts.LOCAL_PARQUET_PATH
    try:
        alerts.fetch_alerts_parquet(path)
        last_seen = alerts.last_seen_by_alert(path)
        winners = alerts.select_winning_versions(path)

        chunk_start = date(start.year, 1, 1)
        while chunk_start <= end:
            chunk_end = min(date(chunk_start.year, 12, 31), end)
            window = (max(chunk_start, start), chunk_end)
            logger.info(f"Backfilling {window[0]} to {window[1]}")

            df = alerts.read_latest_versions(path, window, last_seen, winners=winners)
            built_alerts, day_index = alerts.build_v3_alerts(df, window=window)
            logger.info(f"  {len(day_index)} day files, {len(built_alerts)} alerts")
            alerts._parallel_upload_days(day_index.keys(), built_alerts, day_index)

            chunk_start = date(chunk_start.year + 1, 1, 1)
    finally:
        if os.path.exists(path):
            os.remove(path)
    logger.info("LAMP alerts backfill complete")


if __name__ == "__main__":
    # Usage: python -m chalicelib.lamp.backfill.alerts [START_DATE [END_DATE]]  (YYYY-MM-DD)
    logging.basicConfig(level=logging.INFO)
    args = [date.fromisoformat(arg) for arg in sys.argv[1:3]]
    backfill_all_alerts(*args)
