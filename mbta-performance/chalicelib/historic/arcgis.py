import pathlib
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from .constants import FERRY_LAYER_URL, FERRY_RIDERSHIP_ARCGIS_URL, FERRY_UPDATE_CACHE_URL

# Like the other modes' downloads, kept under data/input/ and replaced on each run
FERRY_CSV_PATH = "data/input/ferry.csv"

ARCGIS_ITEM_URL = "https://www.arcgis.com/sharing/rest/content/items/{item_id}?f=json"

FERRY_CACHE_TIMEOUT_S = 30 * 60
FERRY_CACHE_POLL_S = 30


def upstream_modified(item_id: str):
    """(title, modified datetime) for an ArcGIS item, or (None, None) if it can't be fetched."""
    try:
        response = requests.get(ARCGIS_ITEM_URL.format(item_id=item_id), timeout=15)
        item = response.json()
        return item.get("title"), datetime.fromtimestamp(item["modified"] / 1000)
    except Exception:
        return None, None


def ferry_data_last_edited() -> datetime:
    """When the MBTA last changed the ferry data."""
    response = requests.get(FERRY_LAYER_URL, params={"f": "json"}, timeout=15)
    response.raise_for_status()
    edited_ms = response.json()["editingInfo"]["dataLastEditDate"]
    return datetime.fromtimestamp(edited_ms / 1000, timezone.utc)


def ferry_cache_generated() -> datetime:
    """When the ArcGIS Hub generated the ferry CSV it currently serves."""
    response = requests.head(FERRY_RIDERSHIP_ARCGIS_URL, allow_redirects=True, timeout=15)
    response.raise_for_status()
    return parsedate_to_datetime(response.headers["Last-Modified"])


def ferry_update_cache(timeout_s: float = FERRY_CACHE_TIMEOUT_S, poll_s: float = FERRY_CACHE_POLL_S) -> datetime:
    """
    Make sure the ArcGIS Hub's copy of the ferry CSV includes the latest data, regenerating it if not. The data is
    large enough that the Hub doesn't refresh the copy on its own, and downloading before it has gets the old one.

    The Hub's status API can't tell us when a refresh is done: with updateCache=true it answers "Pending" even when
    there's nothing to regenerate, and without it, it calls the old copy ready. So compare dates instead.

    Returns when the MBTA last changed the data.
    """
    data_edited = ferry_data_last_edited()
    if ferry_cache_generated() >= data_edited:
        return data_edited

    print(f"The ArcGIS Hub's ferry CSV predates the data ({data_edited:%Y-%m-%d %H:%M} UTC); regenerating it...")
    requests.get(FERRY_UPDATE_CACHE_URL, timeout=60).raise_for_status()
    deadline = time.monotonic() + timeout_s
    while (generated := ferry_cache_generated()) < data_edited:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"After {timeout_s / 60:.0f} minutes, the ArcGIS Hub's ferry CSV ({generated:%Y-%m-%d %H:%M} UTC) still "
                f"predates the data ({data_edited:%Y-%m-%d %H:%M} UTC). Try again later."
            )
        time.sleep(poll_s)
    return data_edited


def download_latest_ferry_data(path: str = FERRY_CSV_PATH) -> str:
    response = requests.get(FERRY_RIDERSHIP_ARCGIS_URL, timeout=15)
    response.raise_for_status()

    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as file:
        file.write(response.content)
    return path
