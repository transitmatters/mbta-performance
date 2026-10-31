"""Pool the routes that run between the same two stops into one segment, for the map.

segments.py gives every route its own row for a stop pair, so wherever routes share a street --
SL4 and SL5 along Washington St, SL1, SL2, SL3 and SLW through the Silver Line tunnel -- the map
draws one line per route exactly over the others, and the colour you see is whichever route
happens to be on top. This adds one more row for each such stop pair, from every route's
traversals together: its median is the median trip along that stretch of street, recomputed
from the traversals rather than averaged from each route's median (see trends.py on percentiles
of percentiles). The map draws it in place of the routes' own lines whenever all of them are
selected.

Only routes the map would draw are pooled: ones with at least MIN_TRAVERSALS_HINT traversals
in the slice. A pair pools only when two or more of them are really the same piece of street --
the same direction_id (a few pairs are inbound on one route and outbound on another), and
lengths within SHARED_LENGTH_TOLERANCE_M of each other.

In the tiles, a pooled row's route_id is its routes' ids sorted and comma-joined ("749,751"),
and that string is also the shared_routes of every row on the pair, pooled or not, so the
frontend can tell which rows a pooled one stands in for. These rows only go to the map's
PMTiles: the GeoParquet and the leaderboard stay one row per route, and "749,751" is not a
route.

Cost, measured on 2026-10-29's day, week and month: a third of the band rows the map draws are on
a pooled pair, so the archives grow 7-10%, plus the larger tile limit they need (see
pmtiles.MAXIMUM_TILE_BYTES) -- about 1GB more S3 a year, and about 10% more tile transfer for
whatever part of the map is viewed. Computing them takes 2s for a day and 12s for a month.
"""

import logging

import pandas as pd

from .constants import ALL_DAY_BAND, MIN_TRAVERSALS_HINT
from .segments import SEGMENT_KEY, _aggregate, with_time_bands

logger = logging.getLogger(__name__)

STOP_PAIR_KEY = ["direction_id", "from_stop_id", "to_stop_id"]

# Routes on the same stop pair are almost always cut from the same street: on 2026-10-28 the
# median and p95 difference in their lengths was 0m, and only one pair differed by more than this.
SHARED_LENGTH_TOLERANCE_M = 25


def _shared_routes(result: pd.DataFrame, group_key: list[str]) -> pd.DataFrame:
    """The (route, stop pair, slice) rows that pool, each with the shared_routes of its pair."""
    drawn = result[result.n_traversals >= MIN_TRAVERSALS_HINT]
    groups = drawn.groupby(group_key, sort=False)
    length_spread = groups.segment_length_m.transform("max") - groups.segment_length_m.transform("min")
    members = drawn[(groups.route_id.transform("size") > 1) & (length_spread <= SHARED_LENGTH_TOLERANCE_M)]

    shared_routes = (
        members.sort_values("route_id").groupby(group_key, sort=False).route_id.agg(",".join).rename("shared_routes")
    )
    return members[["route_id"] + group_key].join(shared_routes, on=group_key)


def add_shared_segments(
    result: pd.DataFrame, traversals: pd.DataFrame, extra_group_columns: tuple[str, ...] = ("service_date",)
) -> pd.DataFrame:
    """`result` plus one pooled row for every stop pair two or more routes share.

    `result` is aggregate_segments' output merged with geometry, and `traversals` the traversals
    it was built from, with the same `extra_group_columns` (see aggregate_segments). Rows on a
    pooled pair gain shared_routes; every other row has it null.
    """
    group_key = STOP_PAIR_KEY + list(extra_group_columns) + ["time_band"]
    members = _shared_routes(result, group_key)
    if members.empty:
        return result.assign(shared_routes=None)

    # As in aggregate_segments: band rows from the traversals in that band, all_day rows from
    # every traversal in any band.
    traversals = with_time_bands(traversals)
    is_all_day = members.time_band == ALL_DAY_BAND
    by_band = traversals.merge(members[~is_all_day], on=SEGMENT_KEY + list(extra_group_columns) + ["time_band"])
    all_day = traversals.drop(columns=["time_band"]).merge(
        members[is_all_day], on=SEGMENT_KEY + list(extra_group_columns)
    )
    pooled = pd.concat(
        [_aggregate(by_band, group_key + ["shared_routes"]), _aggregate(all_day, group_key + ["shared_routes"])],
        ignore_index=True,
    )

    # Geometry and stop names from the route that ran the pair most.
    busiest = (
        result.merge(members, on=["route_id"] + group_key)
        .sort_values(["n_traversals", "route_id"], ascending=[False, True])
        .drop_duplicates(group_key)
    )
    pooled = pooled.merge(busiest[group_key + ["from_stop_name", "to_stop_name", "coordinates"]], on=group_key)
    pooled["route_id"] = pooled.shared_routes

    logger.info(f"Pooled {len(members)} route rows into {len(pooled)} shared stop pair rows")
    # Pooled rows first: where tippecanoe thins out the low zooms (see pmtiles.py), it keeps the
    # first of a run of identical lines, and the pooled one is what the map draws whenever every
    # route on the pair is selected, as it is when no route is.
    return pd.concat([pooled, result.merge(members, on=["route_id"] + group_key, how="left")], ignore_index=True)
