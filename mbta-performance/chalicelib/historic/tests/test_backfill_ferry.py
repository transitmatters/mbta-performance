import unittest
from datetime import datetime, timezone
from unittest import mock

import requests

from ..backfill import ferry
from ..constants import FERRY_ARCGIS_ID


class TestFetchFerryCsv(unittest.TestCase):
    def test_download_is_archived_with_the_data_edit_date(self):
        edited = datetime(2026, 4, 21, 17, 47, tzinfo=timezone.utc)
        with (
            mock.patch.object(ferry, "ferry_update_cache", return_value=edited),
            mock.patch.object(ferry, "download_latest_ferry_data", return_value="data/input/ferry.csv"),
            mock.patch.object(ferry, "validate_source_file"),
            mock.patch.object(ferry, "archive_source_file") as mock_archive,
            mock.patch.object(ferry, "restore_source_file") as mock_restore,
        ):
            self.assertEqual(ferry.fetch_ferry_csv(), "data/input/ferry.csv")

        mock_archive.assert_called_once_with(
            "data/input/ferry.csv", "ferry", item_id=FERRY_ARCGIS_ID, source_date=edited
        )
        mock_restore.assert_not_called()

    def test_arcgis_error_uses_archive(self):
        with (
            mock.patch.object(ferry, "ferry_update_cache", side_effect=requests.HTTPError("400")),
            mock.patch.object(ferry, "archive_source_file") as mock_archive,
            mock.patch.object(ferry, "restore_source_file", return_value="data/input/ferry.csv") as mock_restore,
        ):
            self.assertEqual(ferry.fetch_ferry_csv(), "data/input/ferry.csv")

        mock_restore.assert_called_once_with("data/input/ferry.csv", "ferry")
        mock_archive.assert_not_called()

    def test_invalid_csv_uses_archive(self):
        with (
            mock.patch.object(ferry, "ferry_update_cache"),
            mock.patch.object(ferry, "download_latest_ferry_data", return_value="data/input/ferry.csv"),
            mock.patch.object(ferry, "validate_source_file", side_effect=ferry.InvalidSourceFile("no rows")),
            mock.patch.object(ferry, "archive_source_file") as mock_archive,
            mock.patch.object(ferry, "restore_source_file", return_value="data/input/ferry.csv") as mock_restore,
        ):
            ferry.fetch_ferry_csv()

        mock_archive.assert_not_called()
        mock_restore.assert_called_once_with("data/input/ferry.csv", "ferry")

    def test_arcgis_error_without_archive_raises(self):
        with (
            mock.patch.object(ferry, "ferry_update_cache", side_effect=requests.HTTPError("400")),
            mock.patch.object(ferry, "restore_source_file", side_effect=FileNotFoundError("there's no copy")),
        ):
            with self.assertRaises(ValueError):
                ferry.fetch_ferry_csv()
