import unittest
from datetime import date
from unittest import mock

from .. import trends_backfill


class TestBackfillWeeks(unittest.TestCase):
    def test_runs_the_pipeline_once_per_iso_week_in_the_inclusive_range(self):
        # 2026-01-05 is ISO 2026-W02's Monday; 2026-01-12 is ISO 2026-W03's Monday.
        with mock.patch.object(trends_backfill, "generate_weekly_speed_segments") as generate:
            trends_backfill.backfill_weeks(date(2026, 1, 5), date(2026, 1, 12))

        self.assertEqual(generate.call_count, 2)
        called_keys = [call.args[:2] for call in generate.call_args_list]
        self.assertEqual(called_keys, [(2026, 2), (2026, 3)])

    def test_a_range_spanning_a_year_boundary_rolls_the_iso_year_over(self):
        # 2025-12-24 (2025-W52) through 2026-01-04 (still 2026-W01's Sunday).
        with mock.patch.object(trends_backfill, "generate_weekly_speed_segments") as generate:
            trends_backfill.backfill_weeks(date(2025, 12, 24), date(2026, 1, 4))

        called_keys = [call.args[:2] for call in generate.call_args_list]
        self.assertEqual(called_keys, [(2025, 52), (2026, 1)])

    def test_defaults_to_earliest_data_through_yesterday(self):
        with (
            mock.patch.object(trends_backfill, "get_current_service_date", return_value=date(2025, 12, 30)),
            mock.patch.object(trends_backfill, "generate_weekly_speed_segments") as generate,
        ):
            trends_backfill.backfill_weeks()

        # EARLIEST_LAMP_BUS_DATA (2025-12-24, ISO week 2025-W52) through yesterday
        # (2025-12-29, ISO week 2026-W01).
        called_keys = [call.args[:2] for call in generate.call_args_list]
        self.assertEqual(called_keys, [(2025, 52), (2026, 1)])

    def test_upload_and_pmtiles_are_opt_in_and_forwarded(self):
        with mock.patch.object(trends_backfill, "generate_weekly_speed_segments") as generate:
            trends_backfill.backfill_weeks(date(2026, 1, 5), date(2026, 1, 5))
        generate.assert_called_once_with(2026, 2, upload=False, write_pmtiles=False, write_leaderboard=False)

        with mock.patch.object(trends_backfill, "generate_weekly_speed_segments") as generate:
            trends_backfill.backfill_weeks(
                date(2026, 1, 5), date(2026, 1, 5), upload=True, write_pmtiles=True, write_leaderboard=True
            )
        generate.assert_called_once_with(2026, 2, upload=True, write_pmtiles=True, write_leaderboard=True)

    def test_a_bad_week_does_not_stop_the_rest_of_the_range(self):
        # Mondays 1/5, 1/12, 1/19 are ISO weeks 2026-W02, W03, W04.
        def side_effect(year, week, **kwargs):
            if (year, week) == (2026, 3):
                raise ValueError("no service dates with available LAMP bus data yet")

        with mock.patch.object(trends_backfill, "generate_weekly_speed_segments", side_effect=side_effect) as generate:
            trends_backfill.backfill_weeks(date(2026, 1, 5), date(2026, 1, 19))

        self.assertEqual(generate.call_count, 3)

    def test_an_unexpected_failure_on_one_week_does_not_stop_the_rest_of_the_range(self):
        def side_effect(year, week, **kwargs):
            if (year, week) == (2026, 3):
                raise RuntimeError("tippecanoe failed (100): could not make tile small enough")

        with mock.patch.object(trends_backfill, "generate_weekly_speed_segments", side_effect=side_effect) as generate:
            trends_backfill.backfill_weeks(date(2026, 1, 5), date(2026, 1, 19))

        self.assertEqual(generate.call_count, 3)


class TestBackfillMonths(unittest.TestCase):
    def test_runs_the_pipeline_once_per_calendar_month_in_the_inclusive_range(self):
        with mock.patch.object(trends_backfill, "generate_monthly_speed_segments") as generate:
            trends_backfill.backfill_months(date(2025, 12, 24), date(2026, 2, 15))

        called_keys = [call.args[:2] for call in generate.call_args_list]
        self.assertEqual(called_keys, [(2025, 12), (2026, 1), (2026, 2)])

    def test_defaults_to_earliest_data_through_yesterday(self):
        with (
            mock.patch.object(trends_backfill, "get_current_service_date", return_value=date(2026, 1, 15)),
            mock.patch.object(trends_backfill, "generate_monthly_speed_segments") as generate,
        ):
            trends_backfill.backfill_months()

        called_keys = [call.args[:2] for call in generate.call_args_list]
        self.assertEqual(called_keys, [(2025, 12), (2026, 1)])

    def test_upload_and_pmtiles_are_opt_in_and_forwarded(self):
        with mock.patch.object(trends_backfill, "generate_monthly_speed_segments") as generate:
            trends_backfill.backfill_months(date(2026, 1, 1), date(2026, 1, 31))
        generate.assert_called_once_with(2026, 1, upload=False, write_pmtiles=False, write_leaderboard=False)

        with mock.patch.object(trends_backfill, "generate_monthly_speed_segments") as generate:
            trends_backfill.backfill_months(
                date(2026, 1, 1), date(2026, 1, 31), upload=True, write_pmtiles=True, write_leaderboard=True
            )
        generate.assert_called_once_with(2026, 1, upload=True, write_pmtiles=True, write_leaderboard=True)

    def test_a_bad_month_does_not_stop_the_rest_of_the_range(self):
        def side_effect(year, month, **kwargs):
            if (year, month) == (2026, 1):
                raise ValueError("no service dates with available LAMP bus data yet")

        with mock.patch.object(trends_backfill, "generate_monthly_speed_segments", side_effect=side_effect) as generate:
            trends_backfill.backfill_months(date(2025, 12, 24), date(2026, 2, 15))

        self.assertEqual(generate.call_count, 3)
