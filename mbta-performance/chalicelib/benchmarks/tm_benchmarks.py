"""TransitMatters benchmarks for rapid-transit travel times.

Reads adjacent stop-pair per-day travel-time percentiles *and* per-stop dwell
percentiles from the slow-zones archive
(s3://tm-mbta-performance/SlowZones/traveltimes/{Color}/{from}_{to}.csv.gz and
s3://tm-mbta-performance/SlowZones/dwells/{Color}/{from}_{to}.csv.gz, where the
dwell file is keyed to stop `from`). Builds a directed graph from the adjacency
filenames and DFS forward from each stop, summing per-day p50s along the path.

Each transition adds the *move time* for the segment plus the *dwell at the
from-stop*, except when the from-stop is the user's starting stop (we depart
from it, so its dwell doesn't count). This matches the dashboard's definition
of travel time (departure from origin -> arrival at destination), which
includes dwells at all intermediate stops but not at the endpoints.

The TM benchmark for a pair is the median of those per-day cumulative sums,
rounded up to the nearest 15s.

Output: s3://tm-mbta-performance/Benchmarks-tm/traveltimes/{Color}.json as
`{"color": "...", "benchmarks": {"{from}|{to}": seconds}}`.

The dashboard compares the TM value against the MBTA scheduled travel time at
render time and shows the lower of the two as the "TransitMatters Benchmark."
"""

import json
import logging
import math
from collections import defaultdict
from io import BytesIO

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

BUCKET = "tm-mbta-performance"
SLOW_ZONES_TT_PREFIX = "SlowZones/traveltimes"
SLOW_ZONES_DWELL_PREFIX = "SlowZones/dwells"
OUTPUT_PREFIX = "Benchmarks-tm/traveltimes"

# Rapid-transit colors as used in the SlowZones archive. Green is a single
# folder spanning all four branches; the adjacency graph disambiguates.
RAPID_TRANSIT_COLORS = ("Red", "Blue", "Orange", "Green", "Mattapan")

# Require at least this many aligned service-days (days on which *every*
# segment in the path has a p50) for a pair to receive a TM benchmark.
MIN_SERVICE_DAYS = 365

# Round the historical p50 up to this granularity (seconds).
ROUND_UP_TO_SECONDS = 30

s3 = boto3.client("s3")


def _list_keys_under(prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv.gz"):
                keys.append(key)
    return keys


def _pair_from_key(key: str) -> tuple[str, str] | None:
    filename = key.rsplit("/", 1)[-1].removesuffix(".csv.gz")
    fr, _, to = filename.partition("_")
    if not fr or not to:
        return None
    return fr, to


def _load_p50(key: str) -> pd.Series:
    """Return a Series of per-day p50 seconds indexed by service_date, NaNs dropped."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    buffer = BytesIO(obj["Body"].read())
    df = pd.read_csv(buffer, compression="gzip")
    if "50%" not in df.columns or "service_date" not in df.columns:
        return pd.Series(dtype="float64")
    df["50%"] = pd.to_numeric(df["50%"], errors="coerce")
    df = df.dropna(subset=["50%", "service_date"])
    return pd.Series(df["50%"].values, index=df["service_date"].values, dtype="float64")


def _ceil_to_15s(seconds: float) -> int:
    return int(math.ceil(seconds / ROUND_UP_TO_SECONDS) * ROUND_UP_TO_SECONDS)


def _benchmark_from_series(cumulative: pd.Series) -> int | None:
    cumulative = cumulative.dropna()
    if len(cumulative) < MIN_SERVICE_DAYS:
        return None
    median = float(cumulative.median())
    if not math.isfinite(median) or median <= 0:
        return None
    return _ceil_to_15s(median)


def _expand_downstream(
    start: str,
    segments: dict[tuple[str, str], pd.Series],
    dwells: dict[str, pd.Series],
    next_stops: dict[str, list[str]],
) -> dict[str, int]:
    """DFS forward from `start`, accumulating per-day p50 sums of move + dwell.

    Returns `{to_stop: benchmark_seconds}` for every stop reachable via the
    adjacency graph. When transitioning `current -> next`, we add the segment
    move time plus the dwell at `current` — unless `current == start`, because
    the dashboard measures travel time from departure at origin (post-dwell)
    to arrival at destination (pre-dwell). Multiple paths to the same stop
    (possible on the Green Line's shared trunk) are resolved by keeping the
    minimum benchmark, matching the "reasonable floor" framing.
    """
    results: dict[str, int] = {}
    # Stack entries: (current_stop, cumulative_series, visited_set)
    stack: list[tuple[str, pd.Series, frozenset[str]]] = [(start, pd.Series(dtype="float64"), frozenset({start}))]

    while stack:
        current, cumulative, visited = stack.pop()
        for nxt in next_stops.get(current, []):
            if nxt in visited:
                continue
            seg = segments.get((current, nxt))
            if seg is None or seg.empty:
                continue
            # Build this transition's contribution: move + dwell at `current`
            # (skipped when current is the starting stop — we depart from it).
            transition = seg
            if current != start:
                dwell = dwells.get(current)
                if dwell is not None and not dwell.empty:
                    transition = transition.add(dwell, fill_value=None).dropna()
            if cumulative.empty:
                new_cumulative = transition
            else:
                # Align on service_date — only keep days present in both.
                new_cumulative = cumulative.add(transition, fill_value=None).dropna()
            if new_cumulative.empty:
                continue
            value = _benchmark_from_series(new_cumulative)
            if value is not None:
                prior = results.get(nxt)
                if prior is None or value < prior:
                    results[nxt] = value
            stack.append((nxt, new_cumulative, visited | {nxt}))
    return results


def generate_travel_time_benchmarks_for_color(color: str) -> dict[str, int]:
    logger.info(f"Generating TM travel-time benchmarks for {color}")
    tt_keys = _list_keys_under(f"{SLOW_ZONES_TT_PREFIX}/{color}/")
    dwell_keys = _list_keys_under(f"{SLOW_ZONES_DWELL_PREFIX}/{color}/")
    logger.info(f"{color}: {len(tt_keys)} traveltime and {len(dwell_keys)} dwell archives")

    # Load every adjacent segment once.
    segments: dict[tuple[str, str], pd.Series] = {}
    next_stops: dict[str, list[str]] = defaultdict(list)
    for key in tt_keys:
        pair = _pair_from_key(key)
        if pair is None:
            logger.warning(f"Could not parse stop pair from key {key}")
            continue
        try:
            series = _load_p50(key)
        except Exception as e:
            logger.warning(f"Failed to read {key}: {e}")
            continue
        if series.empty:
            continue
        segments[pair] = series
        next_stops[pair[0]].append(pair[1])

    # Load dwell p50 per stop. The archive duplicates the file per outgoing
    # pair, so we only need one per unique `from` stop.
    dwells: dict[str, pd.Series] = {}
    for key in dwell_keys:
        pair = _pair_from_key(key)
        if pair is None:
            continue
        fr = pair[0]
        if fr in dwells:
            continue
        try:
            series = _load_p50(key)
        except Exception as e:
            logger.warning(f"Failed to read dwell {key}: {e}")
            continue
        if not series.empty:
            dwells[fr] = series
    logger.info(f"{color}: loaded dwells for {len(dwells)} stops")

    # Expand to all (from, to) pairs reachable downstream from each stop.
    benchmarks: dict[str, int] = {}
    for start in list(next_stops.keys()):
        reachable = _expand_downstream(start, segments, dwells, next_stops)
        for to_stop, value in reachable.items():
            benchmarks[f"{start}|{to_stop}"] = value

    logger.info(f"{color}: computed {len(benchmarks)} pair benchmarks from {len(segments)} adjacent segments")
    _upload_benchmarks(color, benchmarks)
    return benchmarks


def _upload_benchmarks(color: str, benchmarks: dict[str, int]) -> None:
    key = f"{OUTPUT_PREFIX}/{color}.json"
    body = json.dumps({"color": color, "benchmarks": benchmarks}, separators=(",", ":"))
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Wrote {len(benchmarks)} travel-time benchmarks to s3://{BUCKET}/{key}")


def generate_travel_time_benchmarks(colors: tuple[str, ...] = RAPID_TRANSIT_COLORS) -> None:
    """Regenerate TransitMatters travel-time benchmarks for all rapid-transit lines."""
    logger.info(f"Generating TM travel-time benchmarks for colors: {colors}")
    for color in colors:
        try:
            generate_travel_time_benchmarks_for_color(color)
        except Exception as e:
            logger.exception(f"Failed to generate benchmarks for {color}: {e}")


if __name__ == "__main__":
    import os

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    generate_travel_time_benchmarks()
