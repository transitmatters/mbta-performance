import unittest
from datetime import date
from unittest import mock

import pandas as pd
import pyarrow as pa

from .. import trends


def _traversal_and_segment(service_date: date, total_time: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    traversal = pd.DataFrame(
        [
            {
                "route_id": "1",
                "direction_id": 0,
                "route_pattern_id": "1-0",
                "from_stop_id": "s1",
                "to_stop_id": "s2",
                "service_date": service_date,
                "depart_seconds": 8 * 3600,
                "total_time_seconds": total_time,
                "moving_time_seconds": total_time,
                "dwell_seconds": 0.0,
                "is_interpolated": False,
                "from_interpolated": False,
                "is_trip_start": False,
                "is_trip_end": False,
                "segment_length_m": 500.0,
            }
        ]
    )
    segment = pd.DataFrame(
        [
            {
                "route_pattern_id": "1-0",
                "route_id": "1",
                "direction_id": 0,
                "from_stop_id": "s1",
                "to_stop_id": "s2",
                "from_stop_name": "Stop 1",
                "to_stop_name": "Stop 2",
                "coordinates": [(-71.05, 42.36), (-71.06, 42.37)],
            }
        ]
    )
    return traversal, segment


class TestBuildPeriodResult(unittest.TestCase):
    def test_aggregates_across_every_date_and_skips_bad_ones(self):
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

        def build_side_effect(service_date):
            if service_date == date(2026, 1, 6):
                raise ValueError("No LAMP bus events for 2026-01-06")
            return _traversal_and_segment(service_date, total_time=100.0 if service_date.day == 5 else 200.0)

        with (
            mock.patch.object(trends, "build_traversals_for_date", side_effect=build_side_effect) as build,
            mock.patch.object(trends, "day_type_for", return_value="business_day"),
        ):
            result, _ = trends._build_period_result(dates)

        self.assertEqual(build.call_count, 3)
        # Both surviving dates share one segment/day-type/time-band -- they collapse to one
        # am_peak row (plus its all_day row), combining the 1/5 and 1/7 traversals rather
        # than keeping a row per date.
        self.assertEqual(sorted(result.time_band), ["all_day", "am_peak"])
        self.assertEqual(list(result.n_traversals), [2, 2])

    def test_different_day_types_stay_in_separate_rows(self):
        # 2026-01-05 is a Monday, 2026-01-10 is the Saturday that ends the same week.
        dates = [date(2026, 1, 5), date(2026, 1, 10)]

        def day_type_side_effect(service_date):
            return "weekend_or_holiday" if service_date.weekday() >= 5 else "business_day"

        with (
            mock.patch.object(
                trends, "build_traversals_for_date", side_effect=lambda d: _traversal_and_segment(d, 100.0)
            ),
            mock.patch.object(trends, "day_type_for", side_effect=day_type_side_effect),
        ):
            result, _ = trends._build_period_result(dates)

        self.assertEqual(len(result), 4)
        self.assertEqual(set(result.day_type), {"business_day", "weekend_or_holiday"})

    def test_all_day_rows_are_split_by_day_type(self):
        # A weekday at 8am (am_peak) and 1pm (midday), and a Saturday at 8am. all_day gets one
        # row per day type, each built from that day type's traversals only.
        def build_side_effect(service_date):
            if service_date.weekday() >= 5:
                return _traversal_and_segment(service_date, 400.0)
            morning, segment = _traversal_and_segment(service_date, 100.0)
            afternoon, _ = _traversal_and_segment(service_date, 300.0)
            afternoon["depart_seconds"] = 13 * 3600
            return pd.concat([morning, afternoon], ignore_index=True), segment

        with (
            mock.patch.object(trends, "build_traversals_for_date", side_effect=build_side_effect),
            mock.patch.object(
                trends,
                "day_type_for",
                side_effect=lambda d: "weekend_or_holiday" if d.weekday() >= 5 else "business_day",
            ),
        ):
            result, _ = trends._build_period_result([date(2026, 1, 5), date(2026, 1, 10)])

        all_day = result[result.time_band == "all_day"].set_index("day_type")
        self.assertEqual(sorted(all_day.index), ["business_day", "weekend_or_holiday"])
        self.assertEqual(all_day.loc["business_day"].n_traversals, 2)
        self.assertAlmostEqual(all_day.loc["business_day"].p50_total_time_seconds, 200.0)
        self.assertEqual(all_day.loc["weekend_or_holiday"].n_traversals, 1)
        self.assertAlmostEqual(all_day.loc["weekend_or_holiday"].p50_total_time_seconds, 400.0)
        # Geometry is merged onto the all_day rows like any band's.
        self.assertTrue(all_day.coordinates.notna().all())

    def test_raises_when_every_date_fails(self):
        with mock.patch.object(trends, "build_traversals_for_date", side_effect=ValueError("no data")):
            with self.assertRaises(ValueError):
                trends._build_period_result([date(2026, 1, 5)])

    def test_a_missing_newest_date_is_skipped_by_default(self):
        dates = [date(2026, 1, 5), date(2026, 1, 6)]

        def build_side_effect(service_date):
            if service_date == date(2026, 1, 6):
                raise ValueError("No LAMP bus events for 2026-01-06")
            return _traversal_and_segment(service_date, 100.0)

        with (
            mock.patch.object(trends, "build_traversals_for_date", side_effect=build_side_effect),
            mock.patch.object(trends, "day_type_for", return_value="business_day"),
        ):
            result, _ = trends._build_period_result(dates)

        self.assertEqual(list(result.n_traversals), [1, 1])

    def test_require_newest_date_fails_before_reading_the_rest(self):
        # LAMP hasn't published 1/7 yet. The period isn't built without it, and the other dates
        # aren't read just to be thrown away.
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
        with mock.patch.object(
            trends, "build_traversals_for_date", side_effect=ValueError("no row group covering 2026-01-07")
        ) as build:
            with self.assertRaisesRegex(ValueError, "2026-01-07, the newest date"):
                trends._build_period_result(dates, require_newest_date=True)

        build.assert_called_once_with(date(2026, 1, 7))

    def test_require_newest_date_still_skips_an_older_gap(self):
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

        def build_side_effect(service_date):
            if service_date == date(2026, 1, 5):
                raise ValueError("No LAMP bus events for 2026-01-05")
            return _traversal_and_segment(service_date, 100.0)

        with (
            mock.patch.object(trends, "build_traversals_for_date", side_effect=build_side_effect),
            mock.patch.object(trends, "day_type_for", return_value="business_day"),
        ):
            result, _ = trends._build_period_result(dates, require_newest_date=True)

        self.assertEqual(list(result.n_traversals), [2, 2])

    def test_an_unreadable_file_is_not_skipped_as_a_missing_date(self):
        # ArrowInvalid is a ValueError, but it means the file was unreadable (mid-rewrite on
        # 2026-11-10), not that the date had no data -- skipping it would publish the period
        # without a day that LAMP actually has.
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

        def build_side_effect(service_date):
            if service_date == date(2026, 1, 5):
                raise pa.ArrowInvalid("Parquet magic bytes not found in footer")
            return _traversal_and_segment(service_date, 100.0)

        with (
            mock.patch.object(trends, "build_traversals_for_date", side_effect=build_side_effect),
            mock.patch.object(trends, "day_type_for", return_value="business_day"),
        ):
            with self.assertRaises(pa.ArrowInvalid):
                trends._build_period_result(dates)

    def test_dates_are_combined_in_date_order_though_read_newest_first(self):
        # Two dates give the same pattern's segment different coordinates. The earliest date's
        # copy is the one kept, as it was before dates were read newest first.
        def build_side_effect(service_date):
            traversal, segment = _traversal_and_segment(service_date, 100.0)
            if service_date == date(2026, 1, 6):
                segment["coordinates"] = [[(-71.0, 42.0), (-71.1, 42.1)]]
            return traversal, segment

        with (
            mock.patch.object(trends, "build_traversals_for_date", side_effect=build_side_effect) as build,
            mock.patch.object(trends, "day_type_for", return_value="business_day"),
        ):
            result, traversals = trends._build_period_result([date(2026, 1, 5), date(2026, 1, 6)])

        self.assertEqual([call.args[0] for call in build.call_args_list], [date(2026, 1, 6), date(2026, 1, 5)])
        self.assertEqual(list(traversals.service_date), [date(2026, 1, 5), date(2026, 1, 6)])
        self.assertEqual([list(c) for c in result.coordinates], [[(-71.05, 42.36), (-71.06, 42.37)]] * 2)


class TestGenerateWeeklySpeedSegments(unittest.TestCase):
    def test_uses_the_iso_week_range_for_the_given_year_and_week(self):
        # ISO 2026-W02 is 2026-01-05..2026-01-11.
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 1, 10)),
            mock.patch.object(
                trends, "_build_period_result", return_value=(pd.DataFrame({"n_traversals": [1]}), pd.DataFrame())
            ) as build,
        ):
            trends.generate_weekly_speed_segments(2026, 2)

        called_dates = build.call_args[0][0]
        self.assertEqual(
            called_dates,
            [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8), date(2026, 1, 9)],
        )

    def test_upload_and_pmtiles_are_opt_in(self):
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 1, 10)),
            mock.patch.object(
                trends, "_build_period_result", return_value=(pd.DataFrame({"n_traversals": [1]}), pd.DataFrame())
            ),
            mock.patch.object(trends, "upload_weekly_speed_segments") as upload_segments,
            mock.patch.object(trends, "upload_weekly_pmtiles") as upload_pmtiles,
            mock.patch.object(trends, "upload_weekly_leaderboard") as upload_leaderboard,
        ):
            trends.generate_weekly_speed_segments(2026, 2)

        upload_segments.assert_not_called()
        upload_pmtiles.assert_not_called()
        upload_leaderboard.assert_not_called()

    def test_upload_and_pmtiles_flags_are_forwarded(self):
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 1, 10)),
            mock.patch.object(
                trends, "_build_period_result", return_value=(pd.DataFrame({"n_traversals": [1]}), pd.DataFrame())
            ),
            mock.patch.object(trends, "upload_weekly_speed_segments") as upload_segments,
            mock.patch.object(trends, "upload_weekly_pmtiles") as upload_pmtiles,
            mock.patch.object(trends, "upload_weekly_leaderboard") as upload_leaderboard,
            mock.patch.object(trends, "build_leaderboard", return_value={"am_peak": []}) as build_leaderboard,
            mock.patch.object(trends, "add_shared_segments", return_value="with shared") as add_shared,
        ):
            trends.generate_weekly_speed_segments(2026, 2, upload=True, write_pmtiles=True, write_leaderboard=True)

        upload_segments.assert_called_once_with(mock.ANY, 2026, 2)
        # The tiles get the pooled shared rows; the GeoParquet and leaderboard don't.
        add_shared.assert_called_once_with(mock.ANY, mock.ANY, trends.PERIOD_GROUP_COLUMNS)
        upload_pmtiles.assert_called_once_with("with shared", 2026, 2)
        self.assertIsInstance(upload_segments.call_args[0][0], pd.DataFrame)
        build_leaderboard.assert_called_once()
        self.assertIsInstance(build_leaderboard.call_args[0][0], pd.DataFrame)
        upload_leaderboard.assert_called_once_with({"am_peak": []}, 2026, 2)

    def test_require_newest_date_is_forwarded(self):
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 1, 10)),
            mock.patch.object(
                trends, "_build_period_result", return_value=(pd.DataFrame({"n_traversals": [1]}), pd.DataFrame())
            ) as build,
        ):
            trends.generate_weekly_speed_segments(2026, 2, require_newest_date=True)

        self.assertTrue(build.call_args.kwargs["require_newest_date"])

    def test_raises_for_a_week_with_no_available_dates_yet(self):
        # "Yesterday" is before this week even starts, so there's nothing to aggregate.
        with mock.patch.object(trends, "get_current_service_date", return_value=date(2025, 12, 20)):
            with self.assertRaises(ValueError):
                trends.generate_weekly_speed_segments(2025, 51)


class TestGenerateMonthlySpeedSegments(unittest.TestCase):
    def test_uses_the_calendar_month_range_for_the_given_year_and_month(self):
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 1, 15)),
            mock.patch.object(
                trends, "_build_period_result", return_value=(pd.DataFrame({"n_traversals": [1]}), pd.DataFrame())
            ) as build,
        ):
            trends.generate_monthly_speed_segments(2026, 1)

        called_dates = build.call_args[0][0]
        self.assertEqual(called_dates[0], date(2026, 1, 1))
        self.assertEqual(called_dates[-1], date(2026, 1, 14))

    def test_upload_and_pmtiles_flags_are_forwarded(self):
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 2, 1)),
            mock.patch.object(
                trends, "_build_period_result", return_value=(pd.DataFrame({"n_traversals": [1]}), pd.DataFrame())
            ),
            mock.patch.object(trends, "upload_monthly_speed_segments") as upload_segments,
            mock.patch.object(trends, "upload_monthly_pmtiles") as upload_pmtiles,
            mock.patch.object(trends, "upload_monthly_leaderboard") as upload_leaderboard,
            mock.patch.object(trends, "build_leaderboard", return_value={"am_peak": []}),
            mock.patch.object(trends, "add_shared_segments", return_value="with shared"),
        ):
            trends.generate_monthly_speed_segments(2026, 1, upload=True, write_pmtiles=True, write_leaderboard=True)

        upload_segments.assert_called_once_with(mock.ANY, 2026, 1)
        upload_pmtiles.assert_called_once_with("with shared", 2026, 1)
        upload_leaderboard.assert_called_once_with({"am_peak": []}, 2026, 1)

    def test_raises_for_a_month_with_no_available_dates_yet(self):
        with mock.patch.object(trends, "get_current_service_date", return_value=date(2025, 12, 20)):
            with self.assertRaises(ValueError):
                trends.generate_monthly_speed_segments(2025, 11)


class TestGenerateLatestPeriods(unittest.TestCase):
    FLAGS = dict(upload=True, write_pmtiles=True, write_leaderboard=True)

    def test_the_day_after_a_week_ends_builds_only_that_week(self):
        # Today Monday 2026-11-09, so yesterday is the Sunday that ends ISO 2026-W45.
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 11, 9)),
            mock.patch.object(trends, "generate_weekly_speed_segments") as generate,
        ):
            trends.generate_latest_weeks(**self.FLAGS)

        generate.assert_called_once_with(2026, 45, require_newest_date=True, **self.FLAGS)

    def test_rebuilds_the_week_that_just_ended_after_the_current_one(self):
        # Today Wednesday 2026-11-11: yesterday is in W46, and Sunday 11-08 (W45) is still one
        # of the last three service dates.
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 11, 11)),
            mock.patch.object(trends, "generate_weekly_speed_segments") as generate,
        ):
            trends.generate_latest_weeks(**self.FLAGS)

        self.assertEqual(
            generate.call_args_list,
            [
                mock.call(2026, 46, require_newest_date=True, **self.FLAGS),
                mock.call(2026, 45, require_newest_date=True, **self.FLAGS),
            ],
        )

    def test_attempts_every_week_then_raises_if_any_failed(self):
        # LAMP is late with Tuesday: this week can't be built yet, but last week still is.
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 11, 11)),
            mock.patch.object(
                trends, "generate_weekly_speed_segments", side_effect=[ValueError("2026-11-10 missing"), None]
            ) as generate,
        ):
            with self.assertRaisesRegex(RuntimeError, "2026-W46"):
                trends.generate_latest_weeks()

        self.assertEqual(generate.call_count, 2)
        self.assertEqual(generate.call_args_list[1].args, (2026, 45))

    def test_months_rebuild_the_month_that_just_ended(self):
        # Today 2026-11-03: yesterday is 11-02, and 10-31 is still one of the last three.
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 11, 3)),
            mock.patch.object(trends, "generate_monthly_speed_segments") as generate,
        ):
            trends.generate_latest_months(**self.FLAGS)

        self.assertEqual(
            generate.call_args_list,
            [
                mock.call(2026, 11, require_newest_date=True, **self.FLAGS),
                mock.call(2026, 10, require_newest_date=True, **self.FLAGS),
            ],
        )
