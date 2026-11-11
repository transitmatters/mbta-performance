from datetime import date

import pandas as pd

from ..date import EASTERN_TIME
from ..routes import branch_route_id
from ..schedule import (
    BRANCH_DIR_STOP,
    RTE_DIR_STOP,
    TRUNK_DIR_STOP,
    add_scheduled_travel_times,
    gtfs_scheduled_headways,
    smooth_scheduled_headways,
)

DAY = date(2024, 2, 7)


def _events(times, route_ids=None, stop_id="70201", headways=None, tz=EASTERN_TIME):
    n = len(times)
    event_time = pd.to_datetime([f"{DAY} {t}" for t in times], format="mixed")
    return pd.DataFrame(
        {
            "route_id": route_ids or ["Green-D"] * n,
            "direction_id": [0] * n,
            "stop_id": [stop_id] * n,
            "trip_id": [f"actual{i}" for i in range(n)],
            "event_time": event_time.tz_localize(tz) if tz else event_time,
            "scheduled_headway": headways if headways is not None else [None] * n,
        }
    )


def _gtfs(rows):
    """rows: (trip_id, route_id, stop_id, seconds after midnight)."""
    gtfs = pd.DataFrame(rows, columns=["trip_id", "route_id", "stop_id", "arrival_time"])
    gtfs["direction_id"] = 0
    gtfs["arrival_time"] = gtfs["arrival_time"].astype("Int64")
    return gtfs.sort_values("arrival_time", kind="stable").reset_index(drop=True)


# --- smooth_scheduled_headways ---


def test_smooth_averages_each_half_hour_separately():
    events = _events(["08:00", "08:10", "08:29:59", "08:30", "08:45"], headways=[100, 200, 300, 600, 700])
    result = smooth_scheduled_headways(events, DAY)
    assert list(result.scheduled_headway) == [200, 200, 200, 650, 650]


def test_smooth_rounds_to_ten_seconds():
    events = _events(["08:00", "08:05"], headways=[241, 250])
    assert list(smooth_scheduled_headways(events, DAY).scheduled_headway) == [250, 250]


def test_smooth_pools_trunk_by_default_and_branches_when_asked():
    events = _events(["08:00", "08:05"], route_ids=["Green-D", "Green-E"], headways=[60, 120])
    events["branch_route_id"] = events["route_id"]
    events["scheduled_headway_branch"] = [600, 480]

    pools = {"scheduled_headway": TRUNK_DIR_STOP, "scheduled_headway_branch": BRANCH_DIR_STOP}
    result = smooth_scheduled_headways(events, DAY, pools)

    assert list(result.scheduled_headway) == [90, 90]
    assert list(result.scheduled_headway_branch) == [600, 480]
    assert list(result.columns) == list(events.columns)


def test_smooth_keeps_bus_routes_separate():
    events = _events(["08:00", "08:05"], route_ids=["1", "47"], headways=[600, 900])
    assert list(smooth_scheduled_headways(events, DAY).scheduled_headway) == [600, 900]


def test_smooth_buckets_naive_and_tz_aware_times_alike():
    """Historic events are naive Eastern time, LAMP's are tz-aware; both use wall-clock windows."""
    times, headways = ["07:59", "08:01", "08:20", "08:31"], [100, 200, 400, 800]
    naive = smooth_scheduled_headways(_events(times, headways=headways, tz=None), DAY)
    aware = smooth_scheduled_headways(_events(times, headways=headways), DAY)
    assert list(naive.scheduled_headway) == list(aware.scheduled_headway) == [100, 300, 300, 800]


def test_smooth_keeps_rows_and_order_with_a_repeated_index():
    events = _events(["08:20", "08:00", "08:10"], headways=[300, 100, 200]).set_index(pd.Index([0, 0, 1]))
    result = smooth_scheduled_headways(events, DAY)
    assert list(result.event_time) == list(events.event_time)
    assert list(result.scheduled_headway) == [200] * 3


def test_smooth_keeps_events_outside_the_service_day():
    events = _events(["08:00"], headways=[300])
    events.loc[1] = events.loc[0]
    events.loc[1, "event_time"] = pd.Timestamp("2024-02-06 23:00", tz=EASTERN_TIME)
    result = smooth_scheduled_headways(events, DAY)
    assert len(result) == 2
    assert result.scheduled_headway.iloc[0] == 300
    assert pd.isna(result.scheduled_headway.iloc[1])


def test_smooth_fall_back_hour_runs_into_the_1_30_window():
    """On 2024-11-03 the 1:30 window starts at 1:30 EDT and runs to 2:00 EST, through the repeated hour."""
    day = date(2024, 11, 2)
    event_time = pd.DatetimeIndex(
        [
            pd.Timestamp("2024-11-03 05:10", tz="UTC"),  # 1:10 EDT, 1:00 window
            pd.Timestamp("2024-11-03 05:40", tz="UTC"),  # 1:40 EDT, 1:30 window
            pd.Timestamp("2024-11-03 06:10", tz="UTC"),  # 1:10 EST, still the 1:30 window
            pd.Timestamp("2024-11-03 07:05", tz="UTC"),  # 2:05 EST, 2:00 window
        ]
    ).tz_convert(EASTERN_TIME)
    events = _events(["00:00"] * 4, headways=[100, 200, 400, 800])
    events["event_time"] = event_time
    result = smooth_scheduled_headways(events, day)
    assert list(result.scheduled_headway) == [100, 300, 300, 800]


# --- gtfs_scheduled_headways ---


def test_gtfs_headways_take_the_latest_scheduled_arrival():
    gtfs = _gtfs([("s0", "Green-D", "70201", 8 * 3600), ("s1", "Green-E", "70201", 8 * 3600 + 240)])
    events = _events(["07:59", "08:01", "08:05"], tz=None)
    headways = gtfs_scheduled_headways(events, gtfs, DAY)
    # nothing scheduled yet, then the first arrival (no gap before it), then the 240s trunk gap
    assert headways.isna().tolist() == [True, True, False]
    assert headways.iloc[2] == 240


def test_gtfs_headways_count_tied_branches_once():
    gtfs = _gtfs(
        [
            ("s0", "Green-B", "70154", 8 * 3600),
            ("s1", "Green-C", "70154", 8 * 3600 + 300),
            ("s2", "Green-D", "70154", 8 * 3600 + 300),
        ]
    )
    events = _events(["08:06"], stop_id="70154", tz=None)
    assert list(gtfs_scheduled_headways(events, gtfs, DAY)) == [300]


def test_gtfs_headways_are_aligned_by_position():
    gtfs = _gtfs([("s0", "Red", "70061", 8 * 3600), ("s1", "Red", "70061", 8 * 3600 + 600)])
    events = _events(["08:20", "08:05"], route_ids=["Red", "Red"], stop_id="70061").set_index(pd.Index([7, 7]))
    headways = gtfs_scheduled_headways(events, gtfs, DAY)
    assert headways.isna().tolist() == [False, True]
    assert headways.iloc[0] == 600
    assert list(headways.index) == [7, 7]


# --- add_scheduled_travel_times ---


def _trip_events(trip_id, route_id, stops_and_times):
    return pd.DataFrame(
        {
            "trip_id": trip_id,
            "route_id": route_id,
            "direction_id": 0,
            "stop_id": [stop for stop, _ in stops_and_times],
            "event_time": [pd.Timestamp(f"{DAY} {t}", tz=EASTERN_TIME) for _, t in stops_and_times],
        }
    )


def test_travel_times_match_the_nearest_scheduled_start():
    gtfs = _gtfs(
        [
            ("early", "Orange", "70036", 8 * 3600),
            ("early", "Orange", "70034", 8 * 3600 + 120),
            ("late", "Orange", "70036", 8 * 3600 + 600),
            ("late", "Orange", "70034", 8 * 3600 + 780),
        ]
    )
    events = _trip_events("t", "Orange", [("70036", "08:08"), ("70034", "08:11")])
    result = add_scheduled_travel_times(events, gtfs, DAY)
    assert list(result.scheduled_trip_id) == ["late", "late"]
    assert list(result.scheduled_tt) == [0, 180]
    # same dtype as GTFS arrival_time, so LAMP's files keep writing "180" rather than "180.0"
    assert result.scheduled_tt.dtype == "Int64"


def test_travel_times_match_red_line_trips_by_branch():
    """A Braintree train matches the Braintree trip, even when an Ashmont trip left Alewife closer in time."""
    gtfs = _gtfs(
        [
            ("ashmont", "Red", "70061", 8 * 3600),
            ("ashmont", "Red", "70085", 8 * 3600 + 1500),
            ("braintree", "Red", "70061", 8 * 3600 + 360),
            ("braintree", "Red", "70095", 8 * 3600 + 1860),
        ]
    )
    events = _trip_events("t", "Red", [("70061", "08:01"), ("70095", "08:26")])
    gtfs["branch_route_id"] = branch_route_id(gtfs).array
    events["branch_route_id"] = branch_route_id(events).array

    result = add_scheduled_travel_times(events, gtfs, DAY, match_on=(*RTE_DIR_STOP, "branch_route_id"))
    assert list(result.scheduled_trip_id) == ["braintree", "braintree"]
    assert list(result.scheduled_tt) == [0, 1500]


def test_travel_times_fall_back_to_the_median_off_the_matched_trip():
    """An interlined trip's stops past the interline aren't on its scheduled trip; use nearby trips' median."""
    gtfs = _gtfs(
        [
            ("e", "Green-E", "70201", 8 * 3600),
            ("d1", "Green-D", "70201", 8 * 3600 - 600),
            ("d1", "Green-D", "70187", 8 * 3600 + 300),
            ("d2", "Green-D", "70201", 8 * 3600 - 300),
            ("d2", "Green-D", "70187", 8 * 3600 + 720),
        ]
    )
    events = pd.concat(
        [
            _trip_events("t", "Green-E", [("70201", "08:00")]),
            _trip_events("t", "Green-D", [("70187", "08:08")]),
        ]
    )
    result = add_scheduled_travel_times(events, gtfs, DAY)
    assert list(result.scheduled_trip_id) == ["e", "e"]
    assert list(result.scheduled_tt) == [0, 960]  # median of d1's 900 and d2's 1020


def test_travel_times_match_repeat_visits_by_visit_rank():
    gtfs = _gtfs([("s", "1", "A", 0), ("s", "1", "B", 300), ("s", "1", "A", 600)])
    gtfs["visit_rank"] = [0, 0, 1]
    events = _trip_events("t", "1", [("A", "00:00"), ("B", "00:05"), ("A", "00:10")])
    events["visit_rank"] = [0, 0, 1]
    result = add_scheduled_travel_times(events, gtfs, DAY, stop_on=("stop_id", "visit_rank"))
    assert len(result) == 3
    assert list(result.scheduled_tt) == [0, 300, 600]


def test_travel_times_without_any_match_are_empty():
    gtfs = _gtfs([("s", "Red", "70061", 8 * 3600)]).astype({"trip_id": "string", "route_id": "string"})
    events = _trip_events("t", "Orange", [("70036", "08:00")]).astype({"trip_id": "string", "route_id": "string"})
    result = add_scheduled_travel_times(events, gtfs, DAY)
    assert len(result) == 1
    assert result[["scheduled_trip_id", "scheduled_tt"]].isna().all().all()


def test_travel_times_replace_existing_values_and_reset_the_index():
    gtfs = _gtfs([("s", "Orange", "70036", 8 * 3600), ("s", "Orange", "70034", 8 * 3600 + 120)])
    events = _trip_events("t", "Orange", [("70036", "08:00"), ("70034", "08:02")])
    events["scheduled_tt"] = [99, 99]
    events.index = [5, 5]
    result = add_scheduled_travel_times(events, gtfs, DAY)
    assert list(result.scheduled_tt) == [0, 120]
    assert list(result.index) == [0, 1]
