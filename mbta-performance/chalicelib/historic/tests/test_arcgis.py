import unittest
from unittest import mock

from .. import arcgis
from ..constants import FERRY_RIDERSHIP_ARCGIS_URL, FERRY_UPDATE_CACHE_URL


class TestArcGIS(unittest.TestCase):
    def test_ferry_update_cache(self):
        """Test ferry_update_cache sends request to update cache."""
        with mock.patch("requests.get") as mock_get:
            arcgis.ferry_update_cache()

            # Verify request was made to correct URL
            mock_get.assert_called_once_with(FERRY_UPDATE_CACHE_URL)

    def test_ferry_update_cache_with_error(self):
        """Test ferry_update_cache handles request errors."""
        with mock.patch("requests.get", side_effect=Exception("Network error")):
            # Should raise the exception
            with self.assertRaises(Exception):
                arcgis.ferry_update_cache()

    def test_download_latest_ferry_data_success(self):
        """Test successful download of ferry data."""
        mock_content = b"ferry,data,content\n1,2,3"
        mock_response = mock.Mock(content=mock_content)

        with mock.patch("requests.get", return_value=mock_response) as mock_get:
            with mock.patch("builtins.open", mock.mock_open()) as mock_file:
                result = arcgis.download_latest_ferry_data()

                # Verify correct URL was called
                mock_get.assert_called_once_with(FERRY_RIDERSHIP_ARCGIS_URL, timeout=15)

                # Verify file was written
                mock_file().write.assert_called_once_with(mock_content)

                # Verify a temp file path was returned (it will be a real temp file path)
                self.assertIsInstance(result, str)
                self.assertTrue(len(result) > 0)

    def test_download_latest_ferry_data_timeout(self):
        """Test download with timeout specified."""
        mock_content = b"ferry data"
        mock_response = mock.Mock(content=mock_content)

        with mock.patch("requests.get", return_value=mock_response) as mock_get:
            with mock.patch("builtins.open", mock.mock_open()):
                arcgis.download_latest_ferry_data()

                # Verify timeout was set
                call_kwargs = mock_get.call_args[1]
                self.assertEqual(call_kwargs["timeout"], 15)

    def test_download_latest_ferry_data_request_error(self):
        """Test download handles request errors."""
        with mock.patch("requests.get", side_effect=Exception("Connection timeout")):
            # Should propagate the exception
            with self.assertRaises(Exception) as context:
                arcgis.download_latest_ferry_data()

            self.assertIn("Connection timeout", str(context.exception))

    def test_ferry_constants_exist(self):
        """Test that required ferry constants are defined."""
        # Verify the constants are strings and not empty
        self.assertIsInstance(FERRY_UPDATE_CACHE_URL, str)
        self.assertIsInstance(FERRY_RIDERSHIP_ARCGIS_URL, str)
        self.assertTrue(len(FERRY_UPDATE_CACHE_URL) > 0)
        self.assertTrue(len(FERRY_RIDERSHIP_ARCGIS_URL) > 0)
        # Verify they are ArcGIS URLs
        self.assertIn("arcgis", FERRY_UPDATE_CACHE_URL.lower())
        self.assertIn("arcgis", FERRY_RIDERSHIP_ARCGIS_URL.lower())
