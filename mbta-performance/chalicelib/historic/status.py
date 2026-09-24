"""
Report monthly data coverage: what's in S3, what's processed locally, and when MBTA last updated each source.

Use it before an update to see what's missing, and after uploading to confirm it landed and get the values
for the dashboard's date constants.
"""

import argparse
import calendar
import os
import re
from datetime import datetime

import requests

from .. import s3
from .constants import ARCGIS_IDS, BUS_ARCGIS_IDS, FERRY_ARCGIS_ID, HISTORIC_S3_BUCKET, MODE_FOLDERS

# Long-lived stops checked in S3 to find the latest month without listing every object in the bucket.
# The latest month for a mode is the max across its sentinels.
SENTINEL_STOPS = {
    "rapid": ["70061", "70036", "70105", "70150", "70202"],  # Alewife, Oak Grove, Braintree, Park St, Gov't Ctr
    "bus": ["1-0-110", "1-1-64", "28-0-64000"],
    "ferry": ["Boat-F1|0|Boat-Hingham", "Boat-F4|0|Boat-Charlestown"],
}

ARCGIS_ITEM_URL = "https://www.arcgis.com/sharing/rest/content/items/{item_id}?f=json"

YEAR_RE = re.compile(r"Year=(\d{4})/?$")
MONTH_RE = re.compile(r"Month=(\d{1,2})/?$")


def _max_match(names, pattern):
    values = [int(m.group(1)) for m in (pattern.search(n) for n in names) if m]
    return max(values) if values else None


def latest_s3_month(mode: str, bucket: str = HISTORIC_S3_BUCKET):
    """Latest (year, month) present in S3 for any of the mode's sentinel stops, or None."""
    latest = None
    for stop in SENTINEL_STOPS[mode]:
        stop_prefix = f"Events/{MODE_FOLDERS[mode]}/{stop}/"
        year = _max_match(s3.ls_prefixes(bucket, stop_prefix), YEAR_RE)
        if year is None:
            continue
        month = _max_match(s3.ls_prefixes(bucket, f"{stop_prefix}Year={year}/"), MONTH_RE)
        if month is not None and (latest is None or (year, month) > latest):
            latest = (year, month)
    return latest


def latest_local_month(mode: str, output_dir: str = "data/output"):
    """Latest (year, month) directory for any stop under data/output/Events/<mode folder>, or None."""
    mode_dir = os.path.join(output_dir, "Events", MODE_FOLDERS[mode])
    if not os.path.isdir(mode_dir):
        return None
    latest = None
    with os.scandir(mode_dir) as stops:
        for stop in stops:
            if not stop.is_dir():
                continue
            year = _max_match(os.listdir(stop.path), YEAR_RE)
            if year is None:
                continue
            month = _max_match(os.listdir(os.path.join(stop.path, f"Year={year}")), MONTH_RE)
            if month is not None and (latest is None or (year, month) > latest):
                latest = (year, month)
    return latest


def upstream_item_ids():
    """ArcGIS item for each mode's newest source dataset."""
    latest_rapid_year = max(ARCGIS_IDS.keys())
    latest_bus_year = max(BUS_ARCGIS_IDS.keys())
    return {
        "rapid": ARCGIS_IDS[latest_rapid_year],
        "bus": BUS_ARCGIS_IDS[latest_bus_year],
        "ferry": FERRY_ARCGIS_ID,
    }


def upstream_modified(item_id: str):
    """(title, modified datetime) for an ArcGIS item, or (None, None) if it can't be fetched."""
    try:
        response = requests.get(ARCGIS_ITEM_URL.format(item_id=item_id), timeout=15)
        item = response.json()
        return item.get("title"), datetime.fromtimestamp(item["modified"] / 1000)
    except Exception:
        return None, None


def month_end(ym):
    """Last day of a (year, month) as YYYY-MM-DD."""
    year, month = ym
    return f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"


def _fmt(ym):
    return f"{ym[0]}-{ym[1]:02d}" if ym else "none"


def report(output_dir: str = "data/output", check_upstream: bool = True):
    """Print coverage per mode and the dashboard constants implied by what's in S3. Returns the S3 coverage."""
    item_ids = upstream_item_ids()
    in_s3 = {}
    print(f"{'mode':<6} {'latest in S3':<13} {'latest local':<13} upstream source (last modified)")
    for mode in MODE_FOLDERS:
        in_s3[mode] = latest_s3_month(mode)
        local = latest_local_month(mode, output_dir)
        upstream = ""
        if check_upstream:
            title, modified = upstream_modified(item_ids[mode])
            upstream = f"{title} ({modified:%Y-%m-%d})" if modified else "unavailable"
        print(f"{mode:<6} {_fmt(in_s3[mode]):<13} {_fmt(local):<13} {upstream}")
        if local and (in_s3[mode] is None or local > in_s3[mode]):
            print(
                f"       -> local has months not in S3; run: python -m mbta-performance.chalicelib.historic.upload --mode {mode}"
            )

    print("\nDashboard constants for t-performance-dash, based on S3:")
    if in_s3["rapid"] and in_s3["bus"]:
        shared = min(in_s3["rapid"], in_s3["bus"])
        print(f'  server/chalicelib/date_utils.py  MAX_MONTH_DATA_DATE = "{month_end(shared)}"  (min of rapid and bus)')
    if in_s3["bus"]:
        print(f"  common/constants/dates.ts        BUS_MAX_DATE = '{month_end(in_s3['bus'])}'")
    if in_s3["ferry"]:
        print(f"  common/constants/dates.ts        FERRY_MAX_DATE = '{month_end(in_s3['ferry'])}'")
    return in_s3


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Show monthly data coverage in S3, locally, and upstream.")
    parser.add_argument("--output-dir", default="data/output", help="Processed output dir (default: data/output).")
    parser.add_argument("--skip-upstream", action="store_true", help="Don't query ArcGIS for source update dates.")
    args = parser.parse_args()

    report(output_dir=args.output_dir, check_upstream=not args.skip_upstream)
