import pandas as pd

# Green Line branches share stops through the central subway, so their headways there are trunk headways.
GREEN_LINE_BRANCH_PATTERN = r"^Green-[A-Z]$"

# Stops served only by Ashmont or only by Braintree trains. GTFS and LAMP use route_id "Red" for both
# branches, so a Red Line trip's branch comes from which of these it visits.
RED_LINE_ASHMONT_STOPS = {"70085", "70086", "70087", "70088", "70089", "70090", "70091", "70092", "70093", "70094"}
RED_LINE_BRAINTREE_STOPS = {
    "70095",
    "70096",
    "70097",
    "70098",
    "70099",
    "70100",
    "70101",
    "70102",
    "70103",
    "70104",
    "70105",
}


def trunk_route_id(route_ids: pd.Series) -> pd.Series:
    """Map each route_id to the trunk it runs on: Green-B/C/D/E become Green, everything else is unchanged.

    Red already uses a single route_id for both branches, and bus/ferry/Mattapan routes have no trunk.
    """
    return route_ids.str.replace(GREEN_LINE_BRANCH_PATTERN, "Green", regex=True)


def branch_route_id(stop_times: pd.DataFrame) -> pd.Series:
    """Map each row (an actual event or a scheduled stop time) to the branch its trip runs on.

    Red Line trips become Red-A or Red-B by which branch's stops the trip visits, and stay Red if they only
    visit trunk stops. Every other route is already its own branch (Green-B, Orange, 47).

    Needs trip_id, route_id and stop_id. The result is aligned to stop_times by position, so a
    duplicated index is fine.
    """
    stop_ids = stop_times["stop_id"].astype("string")
    trips = (
        pd.DataFrame(
            {
                "trip_id": stop_times["trip_id"].array,
                "route_id": stop_times["route_id"].array,
                "ashmont": stop_ids.isin(RED_LINE_ASHMONT_STOPS).to_numpy(dtype=bool),
                "braintree": stop_ids.isin(RED_LINE_BRAINTREE_STOPS).to_numpy(dtype=bool),
            }
        )
        .groupby("trip_id")
        .agg(route_id=("route_id", "first"), ashmont=("ashmont", "any"), braintree=("braintree", "any"))
    )
    is_red = (trips["route_id"] == "Red").fillna(False).astype(bool)
    branches = trips["route_id"].mask(is_red & trips["ashmont"] & ~trips["braintree"], "Red-A")
    branches = branches.mask(is_red & trips["braintree"] & ~trips["ashmont"], "Red-B")
    # astype: with no trips (or no trip_ids), map gives float NaN, which merges can't match against route_ids
    branches = stop_times["trip_id"].map(branches).astype(stop_times["route_id"].dtype)
    return pd.Series(branches.array, index=stop_times.index, name="branch_route_id")
