import unittest
from datetime import date
from unittest import mock

import pandas as pd

from .. import remote_parquet


class TestReadServiceDateRetries(unittest.TestCase):
    def setUp(self):
        self.sleep_patch = mock.patch.object(remote_parquet.time, "sleep")
        self.sleep_patch.start()
        self.addCleanup(self.sleep_patch.stop)

    def test_returns_the_result_on_the_first_success(self):
        expected = pd.DataFrame({"service_date": [date(2026, 9, 14)]})
        with mock.patch.object(remote_parquet, "_read_service_date_once", return_value=expected) as read_once:
            result = remote_parquet.read_service_date("http://example/x.parquet", date(2026, 9, 14), ["service_date"])

        self.assertIs(result, expected)
        read_once.assert_called_once()

    def test_retries_on_an_os_error_and_eventually_succeeds(self):
        expected = pd.DataFrame({"service_date": [date(2026, 9, 14)]})
        with mock.patch.object(
            remote_parquet,
            "_read_service_date_once",
            side_effect=[OSError("Couldn't deserialize thrift"), expected],
        ) as read_once:
            result = remote_parquet.read_service_date("http://example/x.parquet", date(2026, 9, 14), ["service_date"])

        self.assertIs(result, expected)
        self.assertEqual(read_once.call_count, 2)

    def test_gives_up_and_raises_after_max_attempts(self):
        with mock.patch.object(
            remote_parquet, "_read_service_date_once", side_effect=OSError("Couldn't deserialize thrift")
        ) as read_once:
            with self.assertRaises(OSError):
                remote_parquet.read_service_date("http://example/x.parquet", date(2026, 9, 14), ["service_date"])

        self.assertEqual(read_once.call_count, remote_parquet.MAX_READ_ATTEMPTS)

    def test_a_missing_row_group_is_not_retried(self):
        # ValueError ("no row group covering this date") is a permanent condition, not a
        # transient read failure -- retrying it would just waste time.
        with mock.patch.object(
            remote_parquet, "_read_service_date_once", side_effect=ValueError("no row group")
        ) as read_once:
            with self.assertRaises(ValueError):
                remote_parquet.read_service_date("http://example/x.parquet", date(2026, 9, 14), ["service_date"])

        read_once.assert_called_once()
