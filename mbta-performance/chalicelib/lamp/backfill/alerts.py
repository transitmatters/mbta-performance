import logging
import os
from datetime import date, timedelta

from .. import alerts

logger = logging.getLogger(__name__)

# LAMP's alert coverage is sparse before this date -- see chalicelib/lamp/alerts.py
# module docstring. t-performance-dash should not route dates before this to the
# LAMP source.
EARLIEST_LAMP_ALERTS_DATA = date(2019, 9, 1)


def backfill_all_alerts():
    """Rebuild every historical Alerts/lamp/{date}.json.gz day file.

    Unlike the scheduled Lambda job (which only rebuilds a rolling window), this
    processes the entire LAMP alerts archive. It's a one-time/occasional local
    script, not something that runs on a schedule or in Lambda -- reading the full
    history into pandas in one shot costs several GB of RSS.

    We keep that memory bounded by chunking the read into one-year windows (the
    download and the global "latest version per alert" pass still happen once,
    since neither is memory-heavy) at the cost of re-scanning the full parquet
    once per chunk -- an acceptable trade for a script that runs rarely and
    doesn't need to fit in a Lambda's memory ceiling.
    """
    end = date.today() + timedelta(days=alerts.LOOKAHEAD_DAYS)
    confirmation = input(
        f"This will rebuild every historical Alerts/lamp/*.json.gz day file from "
        f"{EARLIEST_LAMP_ALERTS_DATA} to {end} (~3,300 files, ~35MB total, a few "
        "minutes). Proceed? (yes/no): "
    )
    if confirmation.lower() not in ("yes", "y"):
        print("You must enter 'yes' to proceed. Exiting.")
        exit(1)

    path = alerts.fetch_alerts_parquet()
    try:
        last_seen = alerts.last_seen_by_alert(path)

        chunk_start = date(EARLIEST_LAMP_ALERTS_DATA.year, 1, 1)
        while chunk_start <= end:
            chunk_end = min(date(chunk_start.year, 12, 31), end)
            window = (max(chunk_start, EARLIEST_LAMP_ALERTS_DATA), chunk_end)
            logger.info(f"Backfilling {window[0]} to {window[1]}")

            df = alerts.read_latest_versions(path, window, last_seen)
            built_alerts, day_index = alerts.build_v3_alerts(df, window=window)
            logger.info(f"  {len(day_index)} day files, {len(built_alerts)} alerts")
            alerts._parallel_upload_days(day_index.keys(), built_alerts, day_index)

            chunk_start = date(chunk_start.year + 1, 1, 1)
    finally:
        if os.path.exists(path):
            os.remove(path)
    logger.info("LAMP alerts backfill complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    backfill_all_alerts()
