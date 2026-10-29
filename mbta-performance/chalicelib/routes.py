import pandas as pd

# Green Line branches share stops through the central subway, so their headways there are trunk headways.
GREEN_LINE_BRANCH_PATTERN = r"^Green-[A-Z]$"


def trunk_route_id(route_ids: pd.Series) -> pd.Series:
    """Map each route_id to the trunk it runs on: Green-B/C/D/E become Green, everything else is unchanged.

    Red already uses a single route_id for both branches, and bus/ferry/Mattapan routes have no trunk.
    """
    return route_ids.str.replace(GREEN_LINE_BRANCH_PATTERN, "Green", regex=True)
