import unittest
from datetime import date
from unittest import mock

from .. import backfill


class TestBackfillRange(unittest.TestCase):
    def setUp(self):
        self.ensure_table = mock.patch.object(backfill.dynamo, "create_table_if_not_exists").start()
        self.addCleanup(mock.patch.stopall)

    def test_runs_the_pipeline_once_per_date_in_the_inclusive_range(self):
        with mock.patch.object(backfill, "generate_speed_segments") as generate:
            backfill.backfill_range(date(2026, 9, 1), date(2026, 9, 3))

        self.assertEqual(generate.call_count, 3)
        called_dates = [call.args[0] for call in generate.call_args_list]
        self.assertEqual(called_dates, [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)])

    def test_ensures_the_table_exists_before_writing(self):
        with mock.patch.object(backfill, "generate_speed_segments"):
            backfill.backfill_range(date(2026, 9, 1), date(2026, 9, 1))

        self.ensure_table.assert_called_once_with("DeliveredTripMetricsBus", hash_key="route", range_key="date")

    def test_each_call_writes_to_dynamo_but_not_upload_or_pmtiles_by_default(self):
        with mock.patch.object(backfill, "generate_speed_segments") as generate:
            backfill.backfill_range(date(2026, 9, 1), date(2026, 9, 1))

        generate.assert_called_once_with(
            date(2026, 9, 1), upload=False, write_to_dynamo=True, write_pmtiles=False, write_leaderboard=False
        )

    def test_upload_and_pmtiles_flags_are_forwarded(self):
        with mock.patch.object(backfill, "generate_speed_segments") as generate:
            backfill.backfill_range(
                date(2026, 9, 1), date(2026, 9, 1), upload=True, write_pmtiles=True, write_leaderboard=True
            )

        generate.assert_called_once_with(
            date(2026, 9, 1), upload=True, write_to_dynamo=True, write_pmtiles=True, write_leaderboard=True
        )

    def test_a_bad_date_does_not_stop_the_rest_of_the_range(self):
        def side_effect(service_date, **kwargs):
            if service_date == date(2026, 9, 2):
                raise ValueError("No LAMP bus events for 2026-09-02")

        with mock.patch.object(backfill, "generate_speed_segments", side_effect=side_effect) as generate:
            backfill.backfill_range(date(2026, 9, 1), date(2026, 9, 3))

        self.assertEqual(generate.call_count, 3)

    def test_an_unexpected_failure_on_one_date_does_not_stop_the_rest_of_the_range(self):
        # e.g. a transient S3 error or tippecanoe choking on an unusually dense day -- not
        # just the ValueError raised for a date with no LAMP events.
        def side_effect(service_date, **kwargs):
            if service_date == date(2026, 9, 2):
                raise RuntimeError("tippecanoe failed (100): could not make tile small enough")

        with mock.patch.object(backfill, "generate_speed_segments", side_effect=side_effect) as generate:
            backfill.backfill_range(date(2026, 9, 1), date(2026, 9, 3))

        self.assertEqual(generate.call_count, 3)
