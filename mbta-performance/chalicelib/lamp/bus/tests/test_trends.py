import unittest
from datetime import date
from unittest import mock

import pandas as pd

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
            result = trends._build_period_result(dates)

        self.assertEqual(build.call_count, 3)
        # Both surviving dates share one segment/day-type/time-band -- they collapse to one
        # row, combining the 1/5 and 1/7 traversals rather than keeping a row per date.
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0].n_traversals, 2)

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
            result = trends._build_period_result(dates)

        self.assertEqual(len(result), 2)
        self.assertEqual(set(result.day_type), {"business_day", "weekend_or_holiday"})

    def test_raises_when_every_date_fails(self):
        with mock.patch.object(trends, "build_traversals_for_date", side_effect=ValueError("no data")):
            with self.assertRaises(ValueError):
                trends._build_period_result([date(2026, 1, 5)])


class TestGenerateWeeklySpeedSegments(unittest.TestCase):
    def test_uses_the_calendar_week_range_for_the_given_week_number(self):
        # Week 3 is 2026-01-05..2026-01-11 (Mondays: wk1=12/22, wk2=12/29, wk3=1/5).
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 1, 10)),
            mock.patch.object(trends, "_build_period_result", return_value=pd.DataFrame({"n_traversals": [1]})) as build,
        ):
            trends.generate_weekly_speed_segments(3)

        called_dates = build.call_args[0][0]
        self.assertEqual(
            called_dates,
            [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8), date(2026, 1, 9)],
        )

    def test_upload_and_pmtiles_are_opt_in(self):
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 1, 10)),
            mock.patch.object(trends, "_build_period_result", return_value=pd.DataFrame({"n_traversals": [1]})),
            mock.patch.object(trends, "upload_weekly_speed_segments") as upload_segments,
            mock.patch.object(trends, "upload_weekly_pmtiles") as upload_pmtiles,
        ):
            trends.generate_weekly_speed_segments(3)

        upload_segments.assert_not_called()
        upload_pmtiles.assert_not_called()

    def test_upload_and_pmtiles_flags_are_forwarded(self):
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 1, 10)),
            mock.patch.object(trends, "_build_period_result", return_value=pd.DataFrame({"n_traversals": [1]})),
            mock.patch.object(trends, "upload_weekly_speed_segments") as upload_segments,
            mock.patch.object(trends, "upload_weekly_pmtiles") as upload_pmtiles,
        ):
            trends.generate_weekly_speed_segments(3, upload=True, write_pmtiles=True)

        upload_segments.assert_called_once_with(mock.ANY, 3)
        upload_pmtiles.assert_called_once_with(mock.ANY, 3)

    def test_raises_for_a_week_with_no_available_dates_yet(self):
        # "Yesterday" is before week 1 even starts, so there's nothing to aggregate.
        with mock.patch.object(trends, "get_current_service_date", return_value=date(2025, 12, 20)):
            with self.assertRaises(ValueError):
                trends.generate_weekly_speed_segments(1)


class TestGenerateMonthlySpeedSegments(unittest.TestCase):
    def test_uses_the_calendar_month_range_for_the_given_month_number(self):
        # Month 2 is January 2026 (month 1 is December 2025, the month EARLIEST_LAMP_BUS_DATA falls in).
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 1, 15)),
            mock.patch.object(trends, "_build_period_result", return_value=pd.DataFrame({"n_traversals": [1]})) as build,
        ):
            trends.generate_monthly_speed_segments(2)

        called_dates = build.call_args[0][0]
        self.assertEqual(called_dates[0], date(2026, 1, 1))
        self.assertEqual(called_dates[-1], date(2026, 1, 14))

    def test_upload_and_pmtiles_flags_are_forwarded(self):
        with (
            mock.patch.object(trends, "get_current_service_date", return_value=date(2026, 2, 1)),
            mock.patch.object(trends, "_build_period_result", return_value=pd.DataFrame({"n_traversals": [1]})),
            mock.patch.object(trends, "upload_monthly_speed_segments") as upload_segments,
            mock.patch.object(trends, "upload_monthly_pmtiles") as upload_pmtiles,
        ):
            trends.generate_monthly_speed_segments(2, upload=True, write_pmtiles=True)

        upload_segments.assert_called_once_with(mock.ANY, 2)
        upload_pmtiles.assert_called_once_with(mock.ANY, 2)

    def test_raises_for_a_month_with_no_available_dates_yet(self):
        with mock.patch.object(trends, "get_current_service_date", return_value=date(2025, 12, 20)):
            with self.assertRaises(ValueError):
                trends.generate_monthly_speed_segments(1)
