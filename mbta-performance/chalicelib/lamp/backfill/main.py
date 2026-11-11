import os
from datetime import date, timedelta

import pandas as pd

from ... import parallel, s3
from ..ingest import S3_BUCKET, S3_KEY_TEMPLATE, fetch_pq_file_from_remote, ingest_pq_file, upload_to_s3

_parallel_upload = parallel.make_parallel(upload_to_s3)

EARLIEST_LAMP_DATA = date(2019, 9, 15)

# One events.csv per stop per day: there are ~500 stop prefixes, and S3 charges $0.005 per 1,000 PUTs.
# Listing what's already there costs ~1,300 LISTs (~$0.01) and holds ~1.3M keys in memory (a few hundred MB).
STOP_FILES_PER_DAY = 500
PUT_COST_PER_1000 = 0.005
S3_PREFIX = S3_KEY_TEMPLATE.split("{stop_id}")[0]

# GTFS feeds are cached here between runs, which matters more than it looks:
# objects in s3://tm-gtfs transition to Glacier Instant Retrieval 180 days after
# upload, and retrieval is billed at $0.03/GB against ~450MB per feed. Access is
# still millisecond -- there is no restore step and nothing to wait for -- but
# you pay per read, and an object is NOT promoted back to Standard by being read.
#
# Keeping this directory populated means a repeated backfill over the same
# stretch of history pays retrieval once rather than once per run. If you are
# about to do several passes over data older than ~6 months, either preserve
# ./feeds between runs or promote the feeds you need back to Standard first:
#
#   aws s3 cp s3://tm-gtfs/<feed_key>/ s3://tm-gtfs/<feed_key>/ \
#       --recursive --storage-class STANDARD
#
# For scale: re-reading the entire 205GB archive once costs about $6.
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


def backfill_all_in_index(force: bool = False):
    """Backfill all the dates in the LAMP index.

    Files whose bytes already match S3 are skipped, so a re-run only pays for what changed.
    force=True uploads every file regardless.
    """

    # all dates that LAMP has data for, starting from 2019-09-15
    dates = pd.date_range(EARLIEST_LAMP_DATA, date.today() - timedelta(days=1), freq="d")
    max_cost = len(dates) * STOP_FILES_PER_DAY * PUT_COST_PER_1000 / 1000

    confirmation = input(
        f"This will backfill {len(dates)} days of LAMP data, from yesterday back to {EARLIEST_LAMP_DATA}. "
        f"It costs at most ~${max_cost:.2f} in S3 PUTs if every file changes"
        f"{'' if force else ' (unchanged files are skipped)'}, and will take hours. "
        "Are you sure you want to proceed? (yes/no): "
    )
    if confirmation.lower() != "yes" and confirmation.lower() != "y":
        print("You must enter 'yes' to proceed. Exiting.")
        exit(1)

    remote_etags = {}  # force: upload everything, no checks
    if not force:
        print(f"Listing s3://{S3_BUCKET}/{S3_PREFIX} to skip unchanged files...")
        remote_etags = s3.ls_etags_parallel(S3_BUCKET, S3_PREFIX)
        print(f"Found {len(remote_etags)} existing files")

    total_uploaded = total_files = 0
    # Backfill each date, most recent to oldest
    for backfill_timestamp in dates[::-1]:
        date_to_backfill = backfill_timestamp.date()
        try:
            pq_df = fetch_pq_file_from_remote(date_to_backfill)
        except ValueError as e:
            # If we can't fetch the file, we can't process it
            print(f"Failed to fetch {date_to_backfill}: {e}")
            continue
        print(f"Processing {date_to_backfill}")
        # allow_build: this runs on a laptop, so it can build a missing feed and
        # publish it. Lambda cannot -- see chalicelib/gtfs.py.
        processed_daily_events = ingest_pq_file(
            pq_df, date_to_backfill, local_archive_path=LOCAL_ARCHIVE_PATH, allow_build=True
        )

        # split daily events by stop_id and parallel upload to s3
        stop_event_groups = processed_daily_events.groupby("stop_id")
        uploaded = _parallel_upload(stop_event_groups, date_to_backfill, remote_etags=remote_etags)
        total_uploaded += len(uploaded)
        total_files += len(stop_event_groups)
        print(f"{date_to_backfill}: uploaded {len(uploaded)}, unchanged {len(stop_event_groups) - len(uploaded)}")

    print(f"Done: uploaded {total_uploaded}, unchanged {total_files - total_uploaded}")


if __name__ == "__main__":
    import argparse
    import logging
    import os

    parser = argparse.ArgumentParser(description="Backfill rapid transit LAMP data from yesterday back to 2019-09-15.")
    parser.add_argument("--force", action="store_true", help="Upload every file, even ones already identical in S3.")
    args = parser.parse_args()

    # Configure logging for local execution
    # Set LOG_LEVEL=DEBUG in environment to enable debug logging
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    backfill_all_in_index(force=args.force)
