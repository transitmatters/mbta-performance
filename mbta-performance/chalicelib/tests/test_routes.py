import pandas as pd

from ..routes import branch_route_id, trunk_route_id


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


def _trip(trip_id, route_id, stop_ids):
    return pd.DataFrame({"trip_id": trip_id, "route_id": route_id, "stop_id": stop_ids})


def test_branch_route_id_red_line_by_branch_stops():
    stop_times = pd.concat(
        [
            _trip("ashmont", "Red", ["70061", "70063", "70085"]),
            _trip("braintree", "Red", ["70061", "70063", "70095"]),
            _trip("trunk", "Red", ["70061", "70063"]),
        ]
    )
    branches = branch_route_id(stop_times)
    assert list(branches) == ["Red-A"] * 3 + ["Red-B"] * 3 + ["Red"] * 2


def test_branch_route_id_other_routes_are_their_own_branch():
    stop_times = pd.concat(
        [
            _trip("gb", "Green-B", ["70107", "70110"]),
            _trip("o", "Orange", ["70001", "70003"]),
            _trip("bus", "47", ["1", "2"]),
        ]
    )
    assert list(branch_route_id(stop_times)) == ["Green-B"] * 2 + ["Orange"] * 2 + ["47"] * 2


def test_branch_route_id_is_aligned_by_position():
    """LAMP stacks arrivals on departures, so the index repeats; every row still gets its own trip's branch."""
    stop_times = pd.concat([_trip("a", "Red", ["70085"]), _trip("b", "Red", ["70095"])]).set_index(pd.Index([0, 0]))
    branches = branch_route_id(stop_times)
    assert list(branches) == ["Red-A", "Red-B"]
    assert list(branches.index) == [0, 0]


def test_branch_route_id_keeps_route_id_dtype_when_empty():
    """An empty schedule must still merge against events' route_ids."""
    stop_times = pd.DataFrame(
        {
            "trip_id": pd.array([], dtype="string"),
            "route_id": pd.array([], dtype="string"),
            "stop_id": pd.array([], dtype="string"),
        }
    )
    assert branch_route_id(stop_times).dtype == pd.StringDtype()
