import logging
import os
from datetime import date, timedelta

import pandas as pd

from ... import s3
from ..bus_constants import BUS_S3_BUCKET, BUS_S3_KEY_TEMPLATE
from ..bus_ingest import RTE_DIR_STOP, _parallel_upload, fetch_bus_pq_file_from_remote, ingest_bus_pq_file

logger = logging.getLogger(__name__)

# The bus LAMP parquet schema added previous_stop_id, is_full_trip, and stopped_duration_seconds
# on 2025-07-30; earlier files don't have those columns at all, so reading them with
# BUS_LAMP_COLUMNS raises pyarrow.lib.ArrowInvalid instead of a clean 404. Confirmed by
# downloading and diffing schemas across dates -- don't move this earlier without re-checking.
EARLIEST_BUS_LAMP_DATA = date(2025, 7, 30)

# One events.csv per route-direction-stop per day, ~2,300 a day since the checkpoint filter
# (#101); S3 charges $0.005 per 1,000 PUTs.
# Listing what's already there costs ~5,000 LISTs (~$0.03) and holds ~5M keys in memory (about 1 GB).
FILES_PER_DAY = 2_300
PUT_COST_PER_1000 = 0.005
S3_PREFIX = BUS_S3_KEY_TEMPLATE.split("{route_id}")[0]

# Set GTFS_DATA_DIR to share cached feeds between repos when running locally;
# an explicit LOCAL_ARCHIVE_PATH still takes precedence.
GTFS_DATA_DIR = os.environ.get("GTFS_DATA_DIR")
LOCAL_ARCHIVE_PATH = os.environ.get(
    "LOCAL_ARCHIVE_PATH",
    os.path.join(os.path.expanduser(GTFS_DATA_DIR), "sqlite_feeds") if GTFS_DATA_DIR else "./feeds",
)
if GTFS_DATA_DIR:
    # mbta-gtfs-sqlite only creates the last path component itself
    os.makedirs(os.path.expanduser(GTFS_DATA_DIR), exist_ok=True)


def backfill_all_bus_dates(start_date: date = EARLIEST_BUS_LAMP_DATA, force: bool = False):
    """Backfill all dates with bus LAMP data, most recent to oldest.

    Files whose bytes already match S3 are skipped, so a re-run only pays for what changed.
    force=True uploads every file regardless.
    """

    if start_date < EARLIEST_BUS_LAMP_DATA:
        raise ValueError(f"start_date {start_date} is before earliest available data {EARLIEST_BUS_LAMP_DATA}")

    end_date = date.today() - timedelta(days=1)
    dates = pd.date_range(start_date, end_date, freq="d")
    num_days = len(dates)
    max_cost = num_days * FILES_PER_DAY * PUT_COST_PER_1000 / 1000

    confirmation = input(
        f"This will backfill {num_days} days of bus LAMP data ({start_date} to {end_date}). "
        f"It costs at most ~${max_cost:.2f} in S3 PUTs if every file changes"
        f"{'' if force else ' (unchanged files are skipped)'}. "
        "This will take hours. Are you sure you want to proceed? (yes/no): "
    )
    if confirmation.lower() not in ("yes", "y"):
        print("You must enter 'yes' to proceed. Exiting.")
        exit(1)

    remote_etags = {}  # force: upload everything, no checks
    if not force:
        logger.info(f"Listing s3://{BUS_S3_BUCKET}/{S3_PREFIX} to skip unchanged files...")
        remote_etags = s3.ls_etags_parallel(BUS_S3_BUCKET, S3_PREFIX)
        logger.info(f"Found {len(remote_etags)} existing files")

    total_uploaded = total_files = 0

    for backfill_timestamp in dates[::-1]:
        date_to_backfill = backfill_timestamp.date()
        try:
            pq_df = fetch_bus_pq_file_from_remote(date_to_backfill)
        except ValueError as e:
            logger.warning(f"Failed to fetch {date_to_backfill}: {e}")
            continue

        logger.info(f"Processing {date_to_backfill}")
        processed = ingest_bus_pq_file(pq_df, date_to_backfill, local_archive_path=LOCAL_ARCHIVE_PATH, allow_build=True)

        group_event_groups = processed.groupby(RTE_DIR_STOP)
        logger.info(f"Uploading events for {len(group_event_groups)} route-direction-stop groups to S3")
        uploaded = _parallel_upload(group_event_groups, date_to_backfill, remote_etags=remote_etags)
        total_uploaded += len(uploaded)
        total_files += len(group_event_groups)
        logger.info(
            f"Finished {date_to_backfill}: uploaded {len(uploaded)}, unchanged {len(group_event_groups) - len(uploaded)}"
        )

    logger.info(f"Done: uploaded {total_uploaded}, unchanged {total_files - total_uploaded}")


if __name__ == "__main__":
    import argparse
    import os
    from datetime import datetime

    parser = argparse.ArgumentParser(description="Backfill bus LAMP data from a start date up to yesterday.")
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=EARLIEST_BUS_LAMP_DATA,
        help=f"Start date in YYYY-MM-DD format (default: {EARLIEST_BUS_LAMP_DATA}).",
    )
    parser.add_argument("--force", action="store_true", help="Upload every file, even ones already identical in S3.")
    args = parser.parse_args()

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    backfill_all_bus_dates(start_date=args.start_date, force=args.force)
