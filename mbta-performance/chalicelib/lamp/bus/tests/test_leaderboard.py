import json
import unittest
from datetime import date

import pandas as pd

from .. import leaderboard, segments


def _row(
    route_id,
    direction_id,
    from_stop,
    to_stop,
    p50_speed_mph,
    n_traversals,
    n_interpolated=0,
    time_band="am_peak",
    day_type=None,
):
    row = {
        "route_id": route_id,
        "direction_id": direction_id,
        "from_stop_name": from_stop,
        "to_stop_name": to_stop,
        "time_band": time_band,
        "p50_speed_mph": p50_speed_mph,
        "n_traversals": n_traversals,
        "n_interpolated": n_interpolated,
    }
    if day_type is not None:
        row["day_type"] = day_type
    return row


class TestBuildLeaderboardDaily(unittest.TestCase):
    """A daily frame carries no day_type column, so the result is keyed by time_band alone."""

    def test_ranks_slowest_first_within_a_time_band(self):
        aggregated = pd.DataFrame(
            [
                _row("1", 0, "A", "B", 20.0, 25),
                _row("1", 0, "B", "C", 5.0, 25),
                _row("1", 0, "C", "D", 12.0, 25),
            ]
        )

        result = leaderboard.build_leaderboard(aggregated)

        speeds = [entry["p50_speed_mph"] for entry in result["am_peak"]]
        self.assertEqual(speeds, sorted(speeds))
        self.assertEqual(speeds[0], 5.0)

    def test_drops_segments_below_the_traversal_threshold(self):
        # Below LEADERBOARD_MIN_TRAVERSALS (20) -- would otherwise be the slowest entry, but a
        # handful of trips is noise, not a real signal (verified against real data -- see
        # LEADERBOARD_MIN_TRAVERSALS's docstring in constants.py).
        aggregated = pd.DataFrame(
            [
                _row("1", 0, "A", "B", 1.0, leaderboard.LEADERBOARD_MIN_TRAVERSALS - 1),
                _row("1", 0, "B", "C", 5.0, 25),
            ]
        )

        result = leaderboard.build_leaderboard(aggregated)

        self.assertEqual(len(result["am_peak"]), 1)
        self.assertEqual(result["am_peak"][0]["from_stop_name"], "B")

    def test_caps_each_slice_at_leaderboard_size(self):
        rows = [_row(str(i), 0, f"S{i}", f"S{i + 1}", float(i), 25) for i in range(leaderboard.LEADERBOARD_SIZE + 10)]
        aggregated = pd.DataFrame(rows)

        result = leaderboard.build_leaderboard(aggregated)

        self.assertEqual(len(result["am_peak"]), leaderboard.LEADERBOARD_SIZE)
        # The slowest LEADERBOARD_SIZE entries survive, not an arbitrary prefix.
        self.assertEqual(result["am_peak"][0]["route_id"], "0")
        self.assertEqual(result["am_peak"][-1]["route_id"], str(leaderboard.LEADERBOARD_SIZE - 1))

    def test_separate_time_bands_stay_in_separate_lists(self):
        aggregated = pd.DataFrame(
            [
                _row("1", 0, "A", "B", 20.0, 25, time_band="am_peak"),
                _row("1", 0, "A", "B", 5.0, 25, time_band="late_night"),
            ]
        )

        result = leaderboard.build_leaderboard(aggregated)

        self.assertEqual(set(result.keys()), {"am_peak", "late_night"})
        self.assertEqual(len(result["am_peak"]), 1)
        self.assertEqual(len(result["late_night"]), 1)

    def test_entries_are_plain_json_serialisable_native_types(self):
        # aggregate_segments produces numpy dtypes (int64/float64); json.dumps chokes on
        # those directly, so this guards against that regression rather than relying on
        # to_dict()'s default behaviour.
        aggregated = pd.DataFrame([_row("1", 0, "A", "B", 12.34, 25, n_interpolated=2)])

        result = leaderboard.build_leaderboard(aggregated)
        entry = result["am_peak"][0]

        json.dumps(result)
        self.assertIsInstance(entry["n_traversals"], int)
        self.assertIsInstance(entry["n_interpolated"], int)
        self.assertIsInstance(entry["direction_id"], int)
        self.assertIsInstance(entry["p50_speed_mph"], float)
        self.assertAlmostEqual(entry["p50_speed_mph"], 12.3, places=1)

    def test_geometry_and_percentile_columns_are_not_exposed(self):
        aggregated = pd.DataFrame(
            [
                {
                    **_row("1", 0, "A", "B", 10.0, 25),
                    "coordinates": [(-71.05, 42.36), (-71.06, 42.37)],
                    "p90_speed_mph": 4.0,
                    "moving_speed_mph": 15.0,
                    "segment_length_m": 500.0,
                }
            ]
        )

        result = leaderboard.build_leaderboard(aggregated)

        self.assertEqual(set(result["am_peak"][0].keys()), set(leaderboard.LEADERBOARD_COLUMNS))


class TestBuildLeaderboardWithDayType(unittest.TestCase):
    """Weekly/monthly frames (trends.py) carry day_type, so it becomes the outer key."""

    def test_nests_by_day_type_then_time_band(self):
        aggregated = pd.DataFrame(
            [
                _row("1", 0, "A", "B", 20.0, 25, time_band="am_peak", day_type="business_day"),
                _row("1", 0, "A", "B", 5.0, 25, time_band="am_peak", day_type="weekend_or_holiday"),
            ]
        )

        result = leaderboard.build_leaderboard(aggregated)

        self.assertEqual(set(result.keys()), {"business_day", "weekend_or_holiday"})
        self.assertEqual(result["business_day"]["am_peak"][0]["p50_speed_mph"], 20.0)
        self.assertEqual(result["weekend_or_holiday"]["am_peak"][0]["p50_speed_mph"], 5.0)


def _aggregated(day_types: list[str] | None = None) -> pd.DataFrame:
    """Real aggregate_segments output: 25 traversals at 8am and 25 at 1pm per day type."""
    rows = []
    for day_type in day_types or [None]:
        for depart_hour, total_time in [(8, 60.0)] * 25 + [(13, 120.0)] * 25:
            row = {
                "route_id": "1",
                "direction_id": 0,
                "from_stop_id": "s1",
                "to_stop_id": "s2",
                "service_date": date(2026, 9, 3),
                "depart_seconds": depart_hour * 3600,
                "total_time_seconds": total_time,
                "moving_time_seconds": total_time,
                "dwell_seconds": 0.0,
                "is_interpolated": False,
                "segment_length_m": 500.0,
            }
            if day_type is not None:
                row["day_type"] = day_type
            rows.append(row)
    extra_group_columns = ("day_type",) if day_types else ("service_date",)
    aggregated = segments.aggregate_segments(pd.DataFrame(rows), extra_group_columns=extra_group_columns)
    return aggregated.assign(from_stop_name="A", to_stop_name="B")


class TestBuildLeaderboardAllDay(unittest.TestCase):
    """aggregate_segments' all_day rows reach the leaderboard as their own slice."""

    def test_daily_leaderboard_has_a_top_level_all_day_key(self):
        result = leaderboard.build_leaderboard(_aggregated())

        self.assertEqual(set(result), {"am_peak", "midday", "all_day"})
        entry = result["all_day"][0]
        self.assertEqual(set(entry), set(leaderboard.LEADERBOARD_COLUMNS))
        self.assertEqual(entry["n_traversals"], 50)
        json.dumps(result)

    def test_trend_leaderboard_has_all_day_under_each_day_type(self):
        result = leaderboard.build_leaderboard(_aggregated(["business_day", "weekend_or_holiday"]))

        self.assertEqual(set(result), {"business_day", "weekend_or_holiday"})
        for day_type in result:
            self.assertIn("all_day", result[day_type])
            self.assertEqual(result[day_type]["all_day"][0]["n_traversals"], 50)
