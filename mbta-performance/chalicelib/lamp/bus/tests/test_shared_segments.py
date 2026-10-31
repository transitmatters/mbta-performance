import unittest
from datetime import date
from unittest import mock

import pandas as pd

from .. import ingest, segments, shared_segments
from ..constants import ALL_DAY_BAND


def _traversal(route_id: str, total_time: float, **overrides) -> dict:
    return {
        "route_id": route_id,
        "direction_id": 1,
        "from_stop_id": "s1",
        "to_stop_id": "s2",
        "service_date": date(2026, 10, 28),
        "depart_seconds": 8 * 3600,
        "total_time_seconds": total_time,
        "moving_time_seconds": total_time * 0.8,
        "dwell_seconds": total_time * 0.2,
        "is_interpolated": False,
        "segment_length_m": 500.0,
        **overrides,
    }


def _traversals(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _sl4_and_sl5(**overrides) -> list[dict]:
    """SL5 (749) three times and SL4 (751) four times over the same stop pair in am_peak."""
    return [_traversal("749", time, **overrides) for time in (100, 110, 120)] + [
        _traversal("751", time, **overrides) for time in (200, 210, 220, 230)
    ]


def _result(traversals: pd.DataFrame, extra_group_columns: tuple[str, ...] = ("service_date",)) -> pd.DataFrame:
    """aggregate_segments' output with geometry, as ingest.py merges it: a line per route, offset
    by route so a test can tell whose was picked."""
    aggregated = segments.aggregate_segments(traversals, extra_group_columns)
    geometry = traversals.drop_duplicates(segments.SEGMENT_KEY)[segments.SEGMENT_KEY].assign(
        from_stop_name=lambda df: "Stop " + df.from_stop_id,
        to_stop_name=lambda df: "Stop " + df.to_stop_id,
    )
    geometry["coordinates"] = [[(-71.0, 42.0 + int(route_id) / 1000), (-71.1, 42.1)] for route_id in geometry.route_id]
    return aggregated.merge(geometry, on=segments.SEGMENT_KEY)


def _pooled(frame: pd.DataFrame) -> pd.DataFrame:
    return frame[frame.route_id.str.contains(",")]


class TestAddSharedSegments(unittest.TestCase):
    def test_pools_every_traversal_on_a_shared_stop_pair(self):
        traversals = _traversals(_sl4_and_sl5())

        mapped = shared_segments.add_shared_segments(_result(traversals), traversals)

        pooled = _pooled(mapped[mapped.time_band == "am_peak"])
        self.assertEqual(len(pooled), 1)
        row = pooled.iloc[0]
        self.assertEqual(row.route_id, "749,751")
        self.assertEqual(row.shared_routes, "749,751")
        self.assertEqual(row.n_traversals, 7)
        # The median of all seven trips -- not the mean of SL5's 110 and SL4's 215.
        self.assertAlmostEqual(row.p50_total_time_seconds, 200.0)
        self.assertAlmostEqual(row.p50_speed_mph, 500.0 / 200.0 * 2.2369362920544)
        self.assertEqual((row.from_stop_name, row.to_stop_name), ("Stop s1", "Stop s2"))

    def test_marks_each_routes_own_rows_with_the_routes_they_share_with(self):
        traversals = _traversals(
            _sl4_and_sl5() + [_traversal("751", time, to_stop_id="s3", from_stop_id="s2") for time in (60, 70, 80)]
        )

        mapped = shared_segments.add_shared_segments(_result(traversals), traversals)

        own = mapped[~mapped.route_id.str.contains(",")].set_index(["route_id", "from_stop_id", "time_band"])
        self.assertEqual(own.loc[("749", "s1", "am_peak")].shared_routes, "749,751")
        self.assertEqual(own.loc[("751", "s1", "am_peak")].shared_routes, "749,751")
        # Only SL4 runs s2 -> s3, so there's nothing to pool there.
        self.assertTrue(pd.isna(own.loc[("751", "s2", "am_peak")].shared_routes))
        self.assertEqual(len(_pooled(mapped)), 2)  # s1 -> s2's am_peak and all_day

    def test_all_day_pools_every_band(self):
        traversals = _traversals(
            _sl4_and_sl5() + [_traversal("749", time, depart_seconds=13 * 3600) for time in (50, 60, 70)]
        )

        mapped = shared_segments.add_shared_segments(_result(traversals), traversals)

        pooled = _pooled(mapped).set_index("time_band")
        self.assertEqual(sorted(pooled.index), ["all_day", "am_peak"])
        # SL5 alone in midday has no one to pool with, but its trips still count all day.
        self.assertEqual(pooled.loc[ALL_DAY_BAND].n_traversals, 10)
        self.assertAlmostEqual(pooled.loc[ALL_DAY_BAND].p50_total_time_seconds, 115.0)

    def test_leaves_out_routes_the_map_would_not_draw(self):
        # Route 11 runs the pair twice, below MIN_TRAVERSALS_HINT, so the map never draws it.
        traversals = _traversals(_sl4_and_sl5() + [_traversal("11", time) for time in (500, 500)])

        mapped = shared_segments.add_shared_segments(_result(traversals), traversals)

        pooled = _pooled(mapped[mapped.time_band == "am_peak"]).iloc[0]
        self.assertEqual(pooled.route_id, "749,751")
        self.assertEqual(pooled.n_traversals, 7)
        route_11 = mapped[(mapped.route_id == "11") & (mapped.time_band == "am_peak")].iloc[0]
        self.assertTrue(pd.isna(route_11.shared_routes))

    def test_does_not_pool_routes_that_differ_in_length(self):
        rows = _sl4_and_sl5()
        for row in rows:
            if row["route_id"] == "751":
                row["segment_length_m"] = 500.0 + shared_segments.SHARED_LENGTH_TOLERANCE_M + 1
        traversals = _traversals(rows)

        mapped = shared_segments.add_shared_segments(_result(traversals), traversals)

        self.assertTrue(_pooled(mapped).empty)
        self.assertTrue(mapped.shared_routes.isna().all())

    def test_does_not_pool_routes_running_the_pair_in_opposite_directions(self):
        rows = _sl4_and_sl5()
        for row in rows:
            if row["route_id"] == "751":
                row["direction_id"] = 0
        traversals = _traversals(rows)

        mapped = shared_segments.add_shared_segments(_result(traversals), traversals)

        self.assertTrue(_pooled(mapped).empty)

    def test_takes_geometry_from_the_route_that_ran_the_pair_most(self):
        traversals = _traversals(_sl4_and_sl5())
        result = _result(traversals)

        mapped = shared_segments.add_shared_segments(result, traversals)

        sl4 = result[(result.route_id == "751") & (result.time_band == "am_peak")].iloc[0]
        self.assertEqual(_pooled(mapped).iloc[0].coordinates, sl4.coordinates)

    def test_pooled_rows_come_first(self):
        # So tippecanoe keeps them over the identical route lines when it thins out low zooms.
        traversals = _traversals(_sl4_and_sl5())

        mapped = shared_segments.add_shared_segments(_result(traversals), traversals)

        self.assertEqual(list(mapped.route_id[:2]), ["749,751", "749,751"])

    def test_pools_each_day_type_separately(self):
        # Both routes run the pair on the weekday, only SL4 at the weekend.
        weekday = [{**row, "day_type": "business_day"} for row in _sl4_and_sl5()]
        weekend = [_traversal("751", time, day_type="weekend_or_holiday") for time in (300, 310, 320)]
        traversals = _traversals(weekday + weekend)

        mapped = shared_segments.add_shared_segments(_result(traversals, ("day_type",)), traversals, ("day_type",))

        pooled = _pooled(mapped)
        self.assertEqual(set(pooled.day_type), {"business_day"})
        self.assertEqual(pooled[pooled.time_band == "am_peak"].iloc[0].n_traversals, 7)
        weekend_sl4 = mapped[(mapped.day_type == "weekend_or_holiday") & (mapped.route_id == "751")]
        self.assertTrue(weekend_sl4.shared_routes.isna().all())

    def test_sorts_route_ids_as_strings(self):
        # The frontend builds the same key with a plain string sort.
        rows = [_traversal("8", time) for time in (100, 110, 120)] + [
            _traversal("19", time) for time in (130, 140, 150)
        ]
        traversals = _traversals(rows)

        mapped = shared_segments.add_shared_segments(_result(traversals), traversals)

        self.assertEqual(set(_pooled(mapped).route_id), {"19,8"})

    def test_adds_nothing_without_a_shared_stop_pair(self):
        traversals = _traversals([_traversal("749", time) for time in (100, 110, 120)])
        result = _result(traversals)

        mapped = shared_segments.add_shared_segments(result, traversals)

        self.assertEqual(len(mapped), len(result))
        self.assertTrue(mapped.shared_routes.isna().all())


class TestIngestPoolsOnlyTheTiles(unittest.TestCase):
    def test_only_the_pmtiles_get_the_pooled_rows(self):
        traversals = _traversals(_sl4_and_sl5()).assign(route_pattern_id=lambda df: df.route_id + "-1")
        geometry = pd.DataFrame(
            [
                {
                    "route_pattern_id": f"{route_id}-1",
                    "route_id": route_id,
                    "direction_id": 1,
                    "from_stop_id": "s1",
                    "to_stop_id": "s2",
                    "from_stop_name": "A",
                    "to_stop_name": "B",
                    "coordinates": [(-71.05, 42.36), (-71.06, 42.37)],
                }
                for route_id in ("749", "751")
            ]
        )
        with (
            mock.patch.object(ingest, "build_traversals_for_date", return_value=(traversals, geometry)),
            mock.patch.object(ingest, "upload_speed_segments") as upload_segments,
            mock.patch.object(ingest, "upload_pmtiles") as upload_pmtiles,
            mock.patch.object(ingest, "upload_leaderboard"),
            mock.patch.object(ingest, "build_leaderboard") as build_leaderboard,
        ):
            ingest.generate_speed_segments(date(2026, 10, 28), upload=True, write_pmtiles=True, write_leaderboard=True)

        self.assertEqual(set(upload_pmtiles.call_args[0][0].route_id), {"749", "751", "749,751"})
        self.assertEqual(set(upload_segments.call_args[0][0].route_id), {"749", "751"})
        self.assertEqual(set(build_leaderboard.call_args[0][0].route_id), {"749", "751"})
