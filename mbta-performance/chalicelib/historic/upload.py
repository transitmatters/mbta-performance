"""
Upload processed monthly outputs from data/output/Events/ to S3.

Local paths map 1:1 to S3 keys (data/output/Events/monthly-data/70061/Year=2026/Month=3/events.csv.gz ->
s3://tm-mbta-performance/Events/monthly-data/70061/Year=2026/Month=3/events.csv.gz).

Outputs are deterministic (gzip mtime=0), so a file whose MD5 matches the existing S3 ETag is skipped.
Re-running is cheap and only months that actually changed get written.
"""

import argparse
import hashlib
import os
import pathlib
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

from .. import s3
from .constants import HISTORIC_S3_BUCKET, MODE_FOLDERS

EVENTS_FILE = "events.csv.gz"
YEAR_RE = re.compile(r"^Year=(\d{4})$")
MONTH_RE = re.compile(r"^Month=(\d{1,2})$")


def _in_range(year: int, month: int, start: date | None, end: date | None) -> bool:
    if start is not None and (year, month) < (start.year, start.month):
        return False
    if end is not None and (year, month) > (end.year, end.month):
        return False
    return True


def find_local_files(output_dir: str, mode: str, start: date = None, end: date = None):
    """
    Yield (local_path, s3_key, (year, month), stop_prefix) for every events file of a mode within [start, end].
    Only the year/month of start and end matter.
    """
    root = pathlib.Path(output_dir)
    mode_dir = root / "Events" / MODE_FOLDERS[mode]
    if not mode_dir.is_dir():
        return

    with os.scandir(mode_dir) as stops:
        for stop in stops:
            if not stop.is_dir():
                continue
            with os.scandir(stop.path) as years:
                for year_dir in years:
                    year_match = YEAR_RE.match(year_dir.name)
                    if not year_match or not year_dir.is_dir():
                        continue
                    year = int(year_match.group(1))
                    with os.scandir(year_dir.path) as months:
                        for month_dir in months:
                            month_match = MONTH_RE.match(month_dir.name)
                            if not month_match:
                                continue
                            month = int(month_match.group(1))
                            if not _in_range(year, month, start, end):
                                continue
                            path = pathlib.Path(month_dir.path, EVENTS_FILE)
                            if not path.is_file():
                                continue
                            key = path.relative_to(root).as_posix()
                            stop_prefix = f"Events/{MODE_FOLDERS[mode]}/{stop.name}/"
                            yield path, key, (year, month), stop_prefix


def _md5(path: pathlib.Path) -> str:
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def upload_mode(
    mode: str,
    output_dir: str = "data/output",
    start: date = None,
    end: date = None,
    dry_run: bool = False,
    workers: int = 16,
    bucket: str = HISTORIC_S3_BUCKET,
):
    """Upload one mode's changed/new files. Returns a summary dict."""
    files = list(find_local_files(output_dir, mode, start, end))
    summary = {"mode": mode, "uploaded": 0, "unchanged": 0, "failed": 0, "months": defaultdict(int), "errors": []}
    if not files:
        print(f"[{mode}] no local files found under {output_dir}/Events/{MODE_FOLDERS[mode]} for the given range")
        return summary

    by_stop = defaultdict(list)
    for f in files:
        by_stop[f[3]].append(f)

    def sync_stop(item):
        stop_prefix, stop_files = item
        remote = s3.ls_etags(bucket, stop_prefix)
        results = []
        for path, key, ym, _ in stop_files:
            if remote.get(key) == _md5(path):
                results.append(("unchanged", ym, None))
                continue
            if dry_run:
                results.append(("uploaded", ym, None))
                continue
            try:
                s3.upload_file(bucket, key, path)
                results.append(("uploaded", ym, None))
            except Exception as e:
                results.append(("failed", ym, f"{key}: {e}"))
        return results

    verb = "would upload" if dry_run else "uploading"
    print(f"[{mode}] checking {len(files)} local files across {len(by_stop)} stops ({verb} changes)...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for results in executor.map(sync_stop, by_stop.items()):
            for status, ym, error in results:
                summary[status] += 1
                if status == "uploaded":
                    summary["months"][ym] += 1
                if error:
                    summary["errors"].append(error)

    _print_summary(summary, dry_run)
    return summary


def _print_summary(summary: dict, dry_run: bool):
    label = "would upload" if dry_run else "uploaded"
    print(
        f"[{summary['mode']}] {label}: {summary['uploaded']}, unchanged: {summary['unchanged']}, "
        f"failed: {summary['failed']}"
    )
    for (year, month), count in sorted(summary["months"].items()):
        print(f"    {year}-{month:02d}: {count} files")
    for error in summary["errors"][:20]:
        print(f"    ERROR {error}")


def upload_monthly_outputs(
    output_dir: str = "data/output",
    modes=tuple(MODE_FOLDERS.keys()),
    start: date = None,
    end: date = None,
    dry_run: bool = False,
    workers: int = 16,
):
    """Upload changed monthly outputs for each mode. Raises if any upload failed."""
    summaries = [upload_mode(mode, output_dir, start, end, dry_run, workers) for mode in modes]
    failed = sum(s["failed"] for s in summaries)
    if failed:
        raise RuntimeError(f"{failed} files failed to upload; re-run to retry (unchanged files are skipped)")
    return summaries


def parse_month(value: str) -> date:
    """Parse YYYY-MM into the first of that month."""
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid month: {value}. Use YYYY-MM format.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload processed monthly data in data/output/Events to S3.")
    parser.add_argument(
        "--mode",
        action="append",
        choices=list(MODE_FOLDERS.keys()),
        help="Mode to upload (repeatable). Default: all modes.",
    )
    parser.add_argument("--start", type=parse_month, help="First month to upload (YYYY-MM). Default: no lower bound.")
    parser.add_argument("--end", type=parse_month, help="Last month to upload (YYYY-MM). Default: no upper bound.")
    parser.add_argument("--output-dir", default="data/output", help="Processed output dir (default: data/output).")
    parser.add_argument("--dry-run", action="store_true", help="Report what would be uploaded without writing.")
    parser.add_argument("--workers", type=int, default=16, help="Parallel stops to sync (default: 16).")
    args = parser.parse_args()

    if args.start and args.end and args.start > args.end:
        parser.error("--start must be before or equal to --end")

    upload_monthly_outputs(
        output_dir=args.output_dir,
        modes=args.mode or tuple(MODE_FOLDERS.keys()),
        start=args.start,
        end=args.end,
        dry_run=args.dry_run,
        workers=args.workers,
    )
