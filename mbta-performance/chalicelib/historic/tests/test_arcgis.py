import pathlib
import tempfile
import unittest
from unittest import mock

import requests

from .. import arcgis
from ..constants import FERRY_LAYER_URL, FERRY_RIDERSHIP_ARCGIS_URL, FERRY_UPDATE_CACHE_URL

DATA_EDITED_MS = 1776793621759  # 2026-04-21 17:47:01 UTC
BEFORE_EDIT = "Mon, 20 Apr 2026 10:00:00 GMT"
AFTER_EDIT = "Wed, 22 Apr 2026 10:32:25 GMT"


def layer_response():
    return mock.Mock(json=mock.Mock(return_value={"editingInfo": {"dataLastEditDate": DATA_EDITED_MS}}))


def download_head(last_modified):
    return mock.Mock(headers={"Last-Modified": last_modified})


class TestFerryUpdateCache(unittest.TestCase):
    def test_current_cache_is_not_regenerated(self):
        """A copy generated after the data last changed is used as is."""
        with mock.patch("requests.get", return_value=layer_response()) as mock_get:
            with mock.patch("requests.head", return_value=download_head(AFTER_EDIT)):
                arcgis.ferry_update_cache()

        mock_get.assert_called_once_with(FERRY_LAYER_URL, params={"f": "json"}, timeout=15)

    def test_stale_cache_is_regenerated_and_waited_for(self):
        """A copy older than the data is regenerated, and the download waits until the new one is served."""
        heads = [download_head(BEFORE_EDIT), download_head(BEFORE_EDIT), download_head(AFTER_EDIT)]
        with mock.patch("requests.get", side_effect=[layer_response(), mock.Mock()]) as mock_get:
            with mock.patch("requests.head", side_effect=heads) as mock_head:
                with mock.patch("time.sleep") as mock_sleep:
                    arcgis.ferry_update_cache()

        mock_get.assert_called_with(FERRY_UPDATE_CACHE_URL, timeout=60)
        self.assertEqual(mock_head.call_count, 3)
        mock_sleep.assert_called_once()

    def test_stale_cache_that_never_updates_times_out(self):
        """Rather than process the old copy, give up with an error."""
        with mock.patch("requests.get", side_effect=[layer_response(), mock.Mock()]):
            with mock.patch("requests.head", return_value=download_head(BEFORE_EDIT)):
                with self.assertRaises(TimeoutError):
                    arcgis.ferry_update_cache(timeout_s=0)

    def test_layer_request_error(self):
        response = mock.Mock(raise_for_status=mock.Mock(side_effect=requests.HTTPError("503")))
        with mock.patch("requests.get", return_value=response):
            with self.assertRaises(requests.HTTPError):
                arcgis.ferry_update_cache()


class TestDownloadLatestFerryData(unittest.TestCase):
    def setUp(self):
        self.path = pathlib.Path(tempfile.mkdtemp()) / "input" / "ferry.csv"

    def test_download_writes_csv(self):
        content = b"ferry,data,content\n1,2,3"
        with mock.patch("requests.get", return_value=mock.Mock(content=content)) as mock_get:
            result = arcgis.download_latest_ferry_data(str(self.path))

        mock_get.assert_called_once_with(FERRY_RIDERSHIP_ARCGIS_URL, timeout=15)
        self.assertEqual(result, str(self.path))
        self.assertEqual(self.path.read_bytes(), content)

    def test_download_error_writes_nothing(self):
        """An error page isn't saved as the CSV."""
        response = mock.Mock(content=b"<html>error</html>", raise_for_status=mock.Mock(side_effect=requests.HTTPError))
        with mock.patch("requests.get", return_value=response):
            with self.assertRaises(requests.HTTPError):
                arcgis.download_latest_ferry_data(str(self.path))

        self.assertFalse(self.path.exists())

    def test_ferry_constants_exist(self):
        for url in (FERRY_UPDATE_CACHE_URL, FERRY_RIDERSHIP_ARCGIS_URL, FERRY_LAYER_URL):
            self.assertIn("arcgis", url.lower())
