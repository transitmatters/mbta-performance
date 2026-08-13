import unittest
from datetime import date
from unittest import mock

import pandas as pd

from .. import gtfs


class TestGTFS(unittest.TestCase):
    def test_max_query_depth_constant(self):
        """Test that MAX_QUERY_DEPTH is set to expected value."""
        self.assertEqual(gtfs.MAX_QUERY_DEPTH, 900)

    def test_max_query_depth_is_less_than_actual_limit(self):
        """Test that MAX_QUERY_DEPTH is less than the actual SQLite limit of 1000."""
        # The actual query limit is 1000, but we use 900 to be safe
        self.assertLess(gtfs.MAX_QUERY_DEPTH, 1000)
        self.assertGreater(gtfs.MAX_QUERY_DEPTH, 0)

    def test_fetch_stop_times_from_gtfs_signature(self):
        """Test that fetch_stop_times_from_gtfs has the expected signature."""
        import inspect

        sig = inspect.signature(gtfs.fetch_stop_times_from_gtfs)
        params = list(sig.parameters.keys())

        # Verify expected parameters
        self.assertIn("trip_ids", params)
        self.assertIn("service_date", params)
        self.assertIn("local_archive_path", params)
        self.assertEqual(len(params), 3)

    def test_fetch_stop_times_from_gtfs_return_type_annotation(self):
        """Test that fetch_stop_times_from_gtfs has proper type annotations."""
        import inspect

        sig = inspect.signature(gtfs.fetch_stop_times_from_gtfs)

        # Check that parameters have proper annotations
        self.assertIn("trip_ids", sig.parameters)
        self.assertIn("service_date", sig.parameters)

        # Verify trip_ids parameter exists and accepts an Iterable
        trip_ids_param = sig.parameters["trip_ids"]
        self.assertIsNotNone(trip_ids_param.annotation)

        # Verify service_date parameter exists and accepts a date
        service_date_param = sig.parameters["service_date"]
        self.assertEqual(service_date_param.annotation, date)

    def _make_mock_feed(self):
        """Return a MagicMock that looks like an MbtaGtfsArchive feed."""
        mock_feed = mock.MagicMock()
        mock_feed.exists_remotely.return_value = True
        return mock_feed

    def _make_mock_archive(self, mock_feed):
        mock_archive = mock.MagicMock()
        mock_archive.get_feed_for_date.return_value = mock_feed
        return mock_archive

    def test_fetch_stop_times_from_gtfs_returns_dataframe(self):
        """fetch_stop_times_from_gtfs should return a DataFrame with the expected columns."""
        mock_df = pd.DataFrame(
            {
                "trip_id": ["trip1"],
                "stop_id": ["70061"],
                "arrival_time": [28800],
                "route_id": ["Red"],
                "direction_id": pd.array([0], dtype="int16"),
            }
        )

        mock_feed = self._make_mock_feed()
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs.pd.read_sql", return_value=mock_df):
                    result = gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2024, 2, 7))

        self.assertIsInstance(result, pd.DataFrame)
        for col in ["trip_id", "stop_id", "arrival_time", "route_id", "direction_id"]:
            self.assertIn(col, result.columns)

    def test_fetch_stop_times_from_gtfs_batches_at_threshold(self):
        """Lists of more than MAX_QUERY_DEPTH trip IDs should be split into multiple batches."""
        # 1901 trip IDs → ceil(1901 / 900) = 3 batches
        trip_ids = [f"trip{i}" for i in range(1901)]
        empty_df = pd.DataFrame(
            {
                "trip_id": pd.Series([], dtype=str),
                "stop_id": pd.Series([], dtype=str),
                "arrival_time": pd.Series([], dtype=float),
                "route_id": pd.Series([], dtype=str),
                "direction_id": pd.array([], dtype="int16"),
            }
        )

        mock_feed = self._make_mock_feed()
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs.pd.read_sql", return_value=empty_df) as mock_read_sql:
                    gtfs.fetch_stop_times_from_gtfs(trip_ids, date(2024, 2, 7))

        # 1901 IDs → 3 batches (900 + 900 + 101)
        self.assertEqual(mock_read_sql.call_count, 3)

    def test_fetch_stop_times_from_gtfs_single_batch_for_small_list(self):
        """A list smaller than MAX_QUERY_DEPTH should result in exactly one SQL query."""
        trip_ids = [f"trip{i}" for i in range(50)]
        empty_df = pd.DataFrame(
            {
                "trip_id": pd.Series([], dtype=str),
                "stop_id": pd.Series([], dtype=str),
                "arrival_time": pd.Series([], dtype=float),
                "route_id": pd.Series([], dtype=str),
                "direction_id": pd.array([], dtype="int16"),
            }
        )

        mock_feed = self._make_mock_feed()
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs.pd.read_sql", return_value=empty_df) as mock_read_sql:
                    gtfs.fetch_stop_times_from_gtfs(trip_ids, date(2024, 2, 7))

        self.assertEqual(mock_read_sql.call_count, 1)

    def test_fetch_stop_times_from_gtfs_uploads_when_not_remote(self):
        """If the feed does not exist remotely, it should be uploaded after building."""
        empty_df = pd.DataFrame(
            {
                "trip_id": pd.Series([], dtype=str),
                "stop_id": pd.Series([], dtype=str),
                "arrival_time": pd.Series([], dtype=float),
                "route_id": pd.Series([], dtype=str),
                "direction_id": pd.array([], dtype="int16"),
            }
        )

        mock_feed = mock.MagicMock()
        mock_feed.exists_remotely.return_value = False  # not on S3 yet
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs.pd.read_sql", return_value=empty_df):
                    gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2024, 2, 7))

        mock_feed.upload_to_s3.assert_called_once()

    def test_fetch_stop_times_from_gtfs_skips_upload_when_remote(self):
        """If the feed already exists remotely, upload_to_s3 should NOT be called."""
        empty_df = pd.DataFrame(
            {
                "trip_id": pd.Series([], dtype=str),
                "stop_id": pd.Series([], dtype=str),
                "arrival_time": pd.Series([], dtype=float),
                "route_id": pd.Series([], dtype=str),
                "direction_id": pd.array([], dtype="int16"),
            }
        )

        mock_feed = self._make_mock_feed()  # exists_remotely = True
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs.pd.read_sql", return_value=empty_df):
                    gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2024, 2, 7))

        mock_feed.upload_to_s3.assert_not_called()

    def test_fetch_stop_times_download_or_build_failure(self):
        """fetch_stop_times_from_gtfs should re-raise when download_or_build fails."""
        mock_feed = self._make_mock_feed()
        mock_feed.download_or_build.side_effect = RuntimeError("build failed")
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with self.assertRaises(RuntimeError):
                    gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2024, 2, 7))

    def test_fetch_stop_times_upload_to_s3_failure(self):
        """fetch_stop_times_from_gtfs should re-raise when upload_to_s3 fails."""
        empty_df = pd.DataFrame(
            {
                "trip_id": pd.Series([], dtype=str),
                "stop_id": pd.Series([], dtype=str),
                "arrival_time": pd.Series([], dtype=float),
                "route_id": pd.Series([], dtype=str),
                "direction_id": pd.array([], dtype="int16"),
            }
        )

        mock_feed = mock.MagicMock()
        mock_feed.exists_remotely.return_value = False  # triggers upload path
        mock_feed.upload_to_s3.side_effect = RuntimeError("S3 upload failed")
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs.pd.read_sql", return_value=empty_df):
                    with self.assertRaises(RuntimeError):
                        gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2024, 2, 7))
