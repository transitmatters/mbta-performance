import json
import unittest
from datetime import date
from unittest import mock

from .. import catch_up

TODAY = date(2026, 11, 11)
YESTERDAY = date(2026, 11, 10)


def _context(remaining_ms: int):
    return mock.Mock(get_remaining_time_in_millis=mock.Mock(return_value=remaining_ms))


class TestCatchUp(unittest.TestCase):
    def setUp(self):
        self.ingested = set()
        patches = [
            mock.patch("chalicelib.lamp.catch_up.get_current_service_date", return_value=TODAY),
            mock.patch("chalicelib.lamp.catch_up.is_ingested", side_effect=lambda _, day: day in self.ingested),
            mock.patch("chalicelib.lamp.catch_up.mark_ingested"),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.mark_ingested = catch_up.mark_ingested

    def test_days_to_ingest_runs_yesterday_first_then_oldest_first(self):
        days = catch_up.days_to_ingest("daily-data", TODAY, lookback_days=4)
        self.assertEqual(days, [YESTERDAY, date(2026, 11, 7), date(2026, 11, 8), date(2026, 11, 9)])

    def test_days_to_ingest_skips_days_with_a_marker(self):
        self.ingested = {YESTERDAY, date(2026, 11, 8)}
        days = catch_up.days_to_ingest("daily-data", TODAY, lookback_days=4)
        self.assertEqual(days, [date(2026, 11, 7), date(2026, 11, 9)])

    def test_days_to_ingest_never_includes_today(self):
        days = catch_up.days_to_ingest("daily-data", TODAY)
        self.assertNotIn(TODAY, days)
        self.assertEqual(len(days), catch_up.LOOKBACK_DAYS)

    def test_normal_day_ingests_only_yesterday(self):
        self.ingested = {date(2026, 11, n) for n in range(4, 10)}
        ingest_day = mock.Mock(return_value=473)

        catch_up.ingest_recent_days("daily-data", ingest_day, 40)

        ingest_day.assert_called_once_with(YESTERDAY)
        self.mark_ingested.assert_called_once_with("daily-data", YESTERDAY, 473)

    def test_nothing_to_do(self):
        self.ingested = {date(2026, 11, n) for n in range(4, 11)}
        ingest_day = mock.Mock()
        catch_up.ingest_recent_days("daily-data", ingest_day, 40)
        ingest_day.assert_not_called()

    def test_a_missed_day_is_retried_on_the_next_run(self):
        """The bug in #133: one failed run used to lose the day for good."""
        self.ingested = {date(2026, 11, n) for n in range(4, 9)}  # 11-09 failed on yesterday's run
        ingest_day = mock.Mock(return_value=1)

        catch_up.ingest_recent_days("bus-daily-data", ingest_day, 120)

        self.assertEqual(ingest_day.call_args_list, [mock.call(YESTERDAY), mock.call(date(2026, 11, 9))])

    def test_a_failure_does_not_stop_other_days_but_fails_the_run(self):
        self.ingested = {date(2026, 11, n) for n in range(4, 9)}
        error = RuntimeError("GTFS feed 20261102 is not in s3://tm-gtfs")
        ingest_day = mock.Mock(side_effect=[error, 1])

        with self.assertRaises(RuntimeError) as context:
            catch_up.ingest_recent_days("daily-data", ingest_day, 40)

        self.assertEqual(ingest_day.call_count, 2)
        # only the day that worked gets a marker, so the failed one is retried next time
        self.mark_ingested.assert_called_once_with("daily-data", date(2026, 11, 9), 1)
        self.assertIn("2026-11-10", str(context.exception))
        self.assertNotIn("2026-11-09", str(context.exception))
        self.assertIs(context.exception.__cause__, error)

    def test_a_failed_marker_write_fails_the_run(self):
        """Otherwise a missing s3:PutObject grant would re-ingest every day, every run, silently."""
        self.ingested = {date(2026, 11, n) for n in range(4, 10)}
        self.mark_ingested.side_effect = RuntimeError("AccessDenied")
        with self.assertRaises(RuntimeError):
            catch_up.ingest_recent_days("daily-data", mock.Mock(return_value=1), 40)

    def test_stops_before_the_lambda_timeout(self):
        ingest_day = mock.Mock(return_value=1)
        # 90s left, then 50s, then 30s: room for two 40s days
        context = mock.Mock(get_remaining_time_in_millis=mock.Mock(side_effect=[90_000, 50_000, 30_000]))

        with self.assertLogs("chalicelib.lamp.catch_up", level="WARNING"):
            catch_up.ingest_recent_days("daily-data", ingest_day, 40, context)

        self.assertEqual(ingest_day.call_args_list, [mock.call(YESTERDAY), mock.call(date(2026, 11, 4))])
        self.assertEqual(self.mark_ingested.call_count, 2)

    def test_deferred_days_alone_do_not_fail_the_run(self):
        ingest_day = mock.Mock(return_value=1)
        catch_up.ingest_recent_days("daily-data", ingest_day, 40, _context(10_000))
        ingest_day.assert_not_called()

    def test_no_context_means_no_time_limit(self):
        """Local runs (python -m ...) have no Lambda context."""
        ingest_day = mock.Mock(return_value=1)
        catch_up.ingest_recent_days("daily-data", ingest_day, 40, None)
        self.assertEqual(ingest_day.call_count, catch_up.LOOKBACK_DAYS)


class TestMarkers(unittest.TestCase):
    def test_marker_key(self):
        self.assertEqual(
            catch_up.marker_key("bus-daily-data", date(2026, 1, 5)),
            "Events-lamp/ingest-markers/bus-daily-data/2026-01-05.json",
        )

    def test_is_ingested_heads_the_marker(self):
        with mock.patch("chalicelib.lamp.catch_up.s3.head_etag", side_effect=["abc", None]) as mock_head:
            self.assertTrue(catch_up.is_ingested("daily-data", YESTERDAY))
            self.assertFalse(catch_up.is_ingested("daily-data", YESTERDAY))
        mock_head.assert_called_with("tm-mbta-performance", "Events-lamp/ingest-markers/daily-data/2026-11-10.json")

    def test_mark_ingested_writes_uncompressed_json(self):
        with mock.patch("chalicelib.lamp.catch_up.s3.upload") as mock_upload:
            catch_up.mark_ingested("daily-data", YESTERDAY, 473)
        bucket, key, body = mock_upload.call_args[0]
        self.assertEqual(
            (bucket, key), ("tm-mbta-performance", "Events-lamp/ingest-markers/daily-data/2026-11-10.json")
        )
        self.assertEqual(mock_upload.call_args.kwargs, {"compress": False})
        payload = json.loads(body)
        self.assertEqual(payload["service_date"], "2026-11-10")
        self.assertEqual(payload["files"], 473)
        self.assertIn("ingested_at", payload)
