"""Aggregate how long buses stood at each stop, as points for the map.

The segment lines on the map show running speed only -- departure from one stop to arrival at
the next (see segments.build_traversals) -- so the time a bus spends at a stop has to be drawn
somewhere else. This gives it one point per (route, direction, stop, time band), plus the same
`all_day` and day_type splits the segments have, so the map can draw and filter the two layers
the same way.

A dwell is the time from a bus's arrival at a stop to its departure, as LAMP records them. Those
aren't taken at the stop itself (see constants.STOP_ZONE_M), so it's best read as time around
the stop -- slowing, standing and pulling out -- rather than time with the doors open. Only
dwells LAMP observed count (segments.observed_dwell_seconds): a trip's first stop is layover, and
an interpolated stop has none. A trip's last stop has no departure, so no dwell either.

Each point sits where the stop falls on the route's shape -- the first point of the segment
leaving it, from the pattern that ran it most -- so it lands on the route's own line rather
than at the pole, which can be across the street. No GTFS stops fetch is needed for it.
"""

import logging

import numpy as np
import pandas as pd

from .constants import ALL_DAY_BAND
from .segments import observed_dwell_seconds, with_time_bands

logger = logging.getLogger(__name__)

STOP_KEY = ["route_id", "direction_id", "stop_id"]


def _aggregate_dwells(dwells: pd.DataFrame, group_key: list[str]) -> pd.DataFrame:
    return (
        dwells.groupby(group_key, sort=False)
        .agg(
            n_dwells=("dwell_seconds", "size"),
            median_dwell_seconds=("dwell_seconds", "median"),
            p90_dwell_seconds=("dwell_seconds", lambda series: float(np.percentile(series, 90))),
        )
        .reset_index()
    )


def _stop_locations(result: pd.DataFrame) -> pd.DataFrame:
    """One (route, direction, stop) row with the stop's name and its point on the route shape."""
    leaving = result.sort_values("pattern_traversals", ascending=False).drop_duplicates(
        ["route_id", "direction_id", "from_stop_id"]
    )
    return pd.DataFrame(
        {
            "route_id": leaving.route_id,
            "direction_id": leaving.direction_id,
            "stop_id": leaving.from_stop_id,
            "stop_name": leaving.from_stop_name,
            "coordinates": [coordinates[0] for coordinates in leaving.coordinates],
        }
    )


def aggregate_dwells(
    traversals: pd.DataFrame, result: pd.DataFrame, extra_group_columns: tuple[str, ...] = ("service_date",)
) -> pd.DataFrame:
    """One row per (route, direction, stop, extra_group_columns, time band), plus all_day.

    `traversals` and `extra_group_columns` are as for segments.aggregate_segments, and `result`
    is its output merged with segment geometry, which each stop's location is taken from. A stop
    with no segment on the map leaving it has nowhere to go, and is dropped.

    Carries `n_dwells`, `median_dwell_seconds` and `p90_dwell_seconds`, and `coordinates` as a
    single (lon, lat) point.
    """
    dwells = with_time_bands(traversals[observed_dwell_seconds(traversals).notna()]).rename(
        columns={"from_stop_id": "stop_id"}
    )

    stop_key = STOP_KEY + list(extra_group_columns)
    by_band = _aggregate_dwells(dwells, stop_key + ["time_band"])
    all_day = _aggregate_dwells(dwells, stop_key).assign(time_band=ALL_DAY_BAND)
    aggregated = pd.concat([by_band, all_day[by_band.columns]], ignore_index=True)

    located = aggregated.merge(_stop_locations(result), on=STOP_KEY, how="inner")
    logger.info(
        f"Aggregated {len(dwells)} observed dwells to {len(located)} stop rows ({len(aggregated)} before location)"
    )
    return located
