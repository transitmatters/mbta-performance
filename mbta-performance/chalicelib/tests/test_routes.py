import pandas as pd

from ..routes import trunk_route_id


def test_trunk_route_id_pools_green_branches():
    route_ids = pd.Series(["Green-B", "Green-C", "Green-D", "Green-E"])
    assert list(trunk_route_id(route_ids)) == ["Green"] * 4


def test_trunk_route_id_leaves_other_routes_alone():
    route_ids = pd.Series(["Red", "Orange", "Blue", "Mattapan", "1", "SL1", "Boat-F1", "CR-Worcester"])
    assert list(trunk_route_id(route_ids)) == list(route_ids)


def test_trunk_route_id_keeps_string_dtype():
    route_ids = pd.Series(["Green-B", "Red"], dtype=pd.StringDtype())
    result = trunk_route_id(route_ids)
    assert result.dtype == pd.StringDtype()
    assert list(result) == ["Green", "Red"]
