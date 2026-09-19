"""Rank bus speed segments slowest-first, as plain JSON for the frontend to fetch directly.

This is the segment-level counterpart to `bus_speed_leaderboard` in t-performance-dash
(routes ranked by mph, summed miles_covered/total_time scanned out of DynamoDB), but the
shape of the problem is different enough that it isn't built the same way:

* Routes: ~150-200 rows/day in a slim DynamoDB table carrying summable totals, so ranking an
  arbitrary date range is a cheap full-table scan (small enough that the table's docstring
  calls it "fine for a table this small") followed by summing two numbers per route.
* Segments: ~10.8k distinct segments/day, and the per-segment numbers already published
  (`segments.aggregate_segments`) are percentiles, not summable totals -- trends.py is
  explicit that a percentile of percentiles is not the true percentile, so there's no cheap
  way to combine multiple days' rows after the fact.

Rather than build a new segment-grain DynamoDB table and reproduce the routes' scan-and-cache
pattern (which would scan orders of magnitude more rows per query), this ranks directly from
the same aggregated table `ingest.py` and `trends.py` already build and publish as
GeoParquet/PMTiles -- there's nothing left to compute at read time, so the result is just
published as JSON alongside those siblings and fetched directly by the frontend, the same way
PMTiles are.
"""

import json
import logging

import pandas as pd

from .constants import LEADERBOARD_MIN_TRAVERSALS, LEADERBOARD_SIZE

logger = logging.getLogger(__name__)

# Deliberately narrow, matching pmtiles.py's TILE_PROPERTIES allowlist: only what a
# leaderboard entry needs to display, selected explicitly so a column like p90_speed_mph or
# moving_speed_mph can't end up in the published file by accident. time_band/day_type are not
# repeated per entry here -- they're already the dict keys the entry sits under.
LEADERBOARD_COLUMNS = [
    "route_id",
    "direction_id",
    "from_stop_name",
    "to_stop_name",
    "p50_speed_mph",
    "n_traversals",
    "n_interpolated",
]


def _ranked_slice(rows: pd.DataFrame) -> list[dict]:
    """Slowest-first, capped at LEADERBOARD_SIZE, as plain JSON-safe records.

    Routed through `to_json`/`json.loads` rather than `to_dict` -- the aggregated columns are
    numpy dtypes (int64, float64), which the standard `json` module cannot serialise directly,
    and pandas' own JSON writer already handles that correctly.
    """
    ranked = rows.sort_values("p50_speed_mph").head(LEADERBOARD_SIZE)
    ranked = ranked[LEADERBOARD_COLUMNS].assign(p50_speed_mph=lambda df: df.p50_speed_mph.round(1))
    return json.loads(ranked.to_json(orient="records", double_precision=1))


def build_leaderboard(aggregated: pd.DataFrame) -> dict:
    """Rank segments slowest-first within each (day_type, time_band) slice.

    `aggregated` is the same per-(segment, [day_type,] time_band) table ingest.py and
    trends.py already build (segments.aggregate_segments's output merged with geometry --
    geometry columns are simply ignored here, this never touches the map). Segments aren't
    blended across time bands for the same reason trends.py splits by day_type: a segment
    that's slow at 7am rush and one that's slow at midnight aren't comparable, so each band
    gets its own ranked list rather than one blended number.

    Segments with fewer than LEADERBOARD_MIN_TRAVERSALS traversals in a slice are dropped
    first -- higher than the map's MIN_TRAVERSALS_HINT (see that constant's docstring):
    a leaderboard entry is a much louder claim ("the #1 slowest segment") than one faint line
    on a map among thousands, and verified against real data, the low end is dominated by
    3-6-traversal noise up through that lower bar. A single day often can't clear this for
    every band -- some may come back sparse or empty -- but the weekly/monthly rollups pool
    enough traversals to comfortably clear it.

    Returns a dict keyed by time_band, e.g. {"am_peak": [...]}. When `aggregated` carries a
    `day_type` column (the weekly/monthly rollups from trends.py -- a single day is already
    wholly one type, so ingest.py's daily frames never have it), the dict is keyed by
    day_type first: {"business_day": {"am_peak": [...]}, "weekend_or_holiday": {...}} --
    matching the "only columns actually present" convention pmtiles.py's TILE_PROPERTIES
    already follows for the same column.
    """
    filtered = aggregated[aggregated.n_traversals >= LEADERBOARD_MIN_TRAVERSALS]

    if "day_type" in filtered.columns:
        result = {
            str(day_type): {
                str(time_band): _ranked_slice(band_rows)
                for time_band, band_rows in day_rows.groupby("time_band", sort=False)
            }
            for day_type, day_rows in filtered.groupby("day_type", sort=False)
        }
    else:
        result = {str(time_band): _ranked_slice(rows) for time_band, rows in filtered.groupby("time_band", sort=False)}

    logger.info(
        f"Built leaderboard from {len(filtered)} of {len(aggregated)} rows "
        f"(dropped below {LEADERBOARD_MIN_TRAVERSALS} traversals)"
    )
    return result
