import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .. import remote_parquet


class TestReadServiceDateOnce(unittest.TestCase):
    """Against a local parquet standing in for the remote export."""

    def _read(self, service_date: date) -> pd.DataFrame:
        # Groups of four put two of 2026-11-10's rows in each of the first two groups.
        dates = [date(2026, 11, 9)] * 2 + [date(2026, 11, 10)] * 4 + [date(2026, 11, 11)] * 2
        table = pa.table({"service_date": dates, "stop_id": [str(i) for i in range(len(dates))]})
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "events.parquet"
            pq.write_table(table, path, row_group_size=4)
            with path.open("rb") as file, mock.patch.object(remote_parquet, "HttpRangeFile", return_value=file):
                return remote_parquet._read_service_date_once(
                    "http://example/x.parquet", service_date, ["service_date", "stop_id"]
                )

    def test_reads_a_day_split_across_row_groups_whole(self):
        self.assertEqual(self._read(date(2026, 11, 10)).stop_id.tolist(), ["2", "3", "4", "5"])

    def test_keeps_only_the_date_asked_for(self):
        self.assertEqual(self._read(date(2026, 11, 9)).stop_id.tolist(), ["0", "1"])

    def test_raises_for_a_date_no_row_group_covers(self):
        with self.assertRaises(ValueError):
            self._read(date(2026, 11, 12))


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
