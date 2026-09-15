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
        self.assertIn("allow_build", params)
        self.assertEqual(len(params), 4)
        # Lambda must never build: a feed build is ~200s and ~1GB of disk against
        # a 60s timeout. Only the backfill scripts opt in.
        self.assertIs(sig.parameters["allow_build"].default, False)

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

    def test_missing_feed_enqueues_a_build_and_raises(self):
        """The 2026-09-15 failure mode: the MBTA published feed 20260907, no bundle
        existed, and download_or_build() burned the 60s timeout every 30 minutes for
        5 hours. Fail in ~2s and hand the build to data-ingestion instead."""
        mock_feed = self._make_mock_feed()
        mock_feed.key = "20260907"
        mock_feed.exists_locally.return_value = False
        mock_feed.exists_remotely.return_value = False
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs._enqueue_gtfs_build") as mock_enqueue:
                    with self.assertRaises(RuntimeError) as ctx:
                        gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2026, 9, 15))

        mock_enqueue.assert_called_once_with("20260907")
        self.assertIn("20260907", str(ctx.exception))
        mock_feed.build_locally.assert_not_called()
        mock_feed.upload_to_s3.assert_not_called()

    def test_enqueue_failure_does_not_mask_the_error(self):
        """If SQS is unreachable the RuntimeError is still the actionable signal."""
        mock_feed = self._make_mock_feed()
        mock_feed.exists_locally.return_value = False
        mock_feed.exists_remotely.return_value = False
        mock_archive = self._make_mock_archive(mock_feed)

        def resource(name, *args, **kwargs):
            # s3 is needed to construct the archive; only sqs is broken here.
            if name == "sqs":
                raise RuntimeError("no sqs")
            return mock.MagicMock()

        with mock.patch("chalicelib.gtfs.boto3.resource", side_effect=resource):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with self.assertRaises(RuntimeError) as ctx:
                    gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2026, 9, 15))

        self.assertIn("not in s3://tm-gtfs", str(ctx.exception))

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

    def test_fetch_stop_times_download_failure(self):
        """A failed download must re-raise rather than fall back to building."""
        mock_feed = self._make_mock_feed()
        mock_feed.exists_locally.return_value = False
        mock_feed.exists_remotely.return_value = True
        mock_feed.download_from_s3.side_effect = RuntimeError("download failed")
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with self.assertRaises(RuntimeError):
                    gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2024, 2, 7))

        mock_feed.build_locally.assert_not_called()

    def test_backfill_may_build_and_upload(self):
        """allow_build=True is the backfill path -- it runs on a laptop, so it can
        build a missing feed and publish it for everyone else."""
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
        mock_feed.exists_locally.return_value = False
        mock_feed.exists_remotely.return_value = False
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs.pd.read_sql", return_value=empty_df):
                    with mock.patch("chalicelib.gtfs._enqueue_gtfs_build") as mock_enqueue:
                        gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2024, 2, 7), allow_build=True)

        mock_feed.build_locally.assert_called_once()
        mock_feed.upload_to_s3.assert_called_once()
        mock_enqueue.assert_not_called()
