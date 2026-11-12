import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
from mbta_gtfs_sqlite.models import StopTime, Trip
from mbta_gtfs_sqlite.session import create_sqlalchemy_session
from mbta_gtfs_sqlite.feed import ALL_DB_FILES, GtfsFeed

from .. import gtfs


class _FakeBucket:
    """Just enough of the tm-gtfs bucket for a real GtfsFeed: a listing and downloads."""

    def __init__(self, feed_key: str):
        self.keys = [f"{feed_key}/{name}" for name in ALL_DB_FILES]
        self.downloads = []
        self.objects = mock.Mock()
        self.objects.filter.side_effect = lambda Prefix: [
            SimpleNamespace(key=k) for k in self.keys if k.startswith(Prefix)
        ]

    def download_file(self, key, filename):
        self.downloads.append(key)
        Path(filename).touch()  # an empty file is a valid empty SQLite database


class TestGTFSCache(unittest.TestCase):
    """#134: the default archive path was TemporaryDirectory().name, a new random
    directory each call that nothing deleted. Warm Lambdas re-downloaded the feed
    every run and left the old copy in /tmp until it filled up."""

    FEED_KEY = "20260907"

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.cache_dir = self.tmp / "gtfs_feeds"
        self.bucket = _FakeBucket(self.FEED_KEY)
        self.archive_paths = []

        def make_archive(local_archive_path, s3_bucket):
            self.archive_paths.append(local_archive_path)
            archive = SimpleNamespace(local_archive_path=local_archive_path, s3_bucket=self.bucket)
            feed = GtfsFeed(
                archive=archive,
                key=self.FEED_KEY,
                url="",
                version="",
                start_date=date(2026, 9, 7),
                end_date=date(2026, 12, 31),
            )
            archive.get_feed_for_date = lambda _: feed
            return archive

        for patcher in (
            mock.patch.object(gtfs, "GTFS_CACHE_DIR", str(self.cache_dir)),
            mock.patch("chalicelib.gtfs.boto3.resource"),
            mock.patch("chalicelib.gtfs.MbtaGtfsArchive", side_effect=make_archive),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def _fetch(self, local_archive_path=None):
        return gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2026, 11, 11), local_archive_path=local_archive_path)

    def test_warm_run_reuses_the_cached_feed(self):
        self._fetch()
        self._fetch()

        self.assertEqual(self.archive_paths, [str(self.cache_dir)] * 2)
        # One download of each file, by the first run only.
        self.assertCountEqual(self.bucket.downloads, self.bucket.keys)

    def test_new_feed_replaces_the_cached_one(self):
        old_feed = self.cache_dir / "20260830"
        old_feed.mkdir(parents=True)
        (old_feed / "gtfs.sqlite3").touch()
        # What s3transfer leaves behind when a timeout kills a download mid-file
        partial = self.cache_dir / self.FEED_KEY / "gtfs.sqlite3.a1B2c3D4"
        partial.parent.mkdir()
        partial.touch()

        self._fetch()

        self.assertEqual(sorted(os.listdir(self.cache_dir)), [self.FEED_KEY])
        self.assertEqual(sorted(os.listdir(self.cache_dir / self.FEED_KEY)), sorted(ALL_DB_FILES))

    def test_caller_archive_is_left_alone(self):
        """Backfills keep every feed they've used in LOCAL_ARCHIVE_PATH."""
        archive = self.tmp / "feeds"
        old_feed = archive / "20260830"
        old_feed.mkdir(parents=True)
        (old_feed / "gtfs.sqlite3").touch()

        self._fetch(local_archive_path=str(archive))

        self.assertTrue((old_feed / "gtfs.sqlite3").exists())
        self.assertTrue((archive / self.FEED_KEY / "gtfs.sqlite3").exists())
        self.assertFalse(self.cache_dir.exists())


class TestGTFS(unittest.TestCase):
    def setUp(self):
        # Tests that leave local_archive_path unset clear GTFS_CACHE_DIR, so keep them
        # off the real one in /tmp.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        patcher = mock.patch.object(gtfs, "GTFS_CACHE_DIR", os.path.join(tmp.name, "gtfs_feeds"))
        patcher.start()
        self.addCleanup(patcher.stop)

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

    def test_empty_batch_does_not_downcast_string_columns(self):
        """2026-10-19: the last batch was all ADDED- trips, matched nothing, and came back
        as object columns. Concatenating it made route_id object, and pandas 3's
        merge_asof rejected it against LAMP's StringDtype route_id."""
        trip_ids = [f"trip{i}" for i in range(901)]
        matched = pd.DataFrame(
            {
                "trip_id": pd.array(["trip1"], dtype="string"),
                "stop_id": pd.array(["70061"], dtype="string"),
                "arrival_time": pd.array([28800], dtype="Int64"),
                "route_id": pd.array(["Red"], dtype="string"),
                "direction_id": pd.array([0], dtype="int16"),
            }
        )
        # What read_sql(dtype_backend="numpy_nullable") returns for zero rows
        unmatched = pd.DataFrame(
            {
                "trip_id": pd.Series([], dtype=object),
                "stop_id": pd.Series([], dtype=object),
                "arrival_time": pd.Series([], dtype=object),
                "route_id": pd.Series([], dtype=object),
                "direction_id": pd.array([], dtype="int16"),
            }
        )

        mock_feed = self._make_mock_feed()
        mock_archive = self._make_mock_archive(mock_feed)

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs.pd.read_sql", side_effect=[matched, unmatched]):
                    result = gtfs.fetch_stop_times_from_gtfs(trip_ids, date(2026, 10, 19))

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result["route_id"].dtype, pd.StringDtype)
        self.assertEqual(result["arrival_time"].dtype, "Int64")

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

    def test_no_feed_for_date_raises_a_clear_error(self):
        """get_feed_for_date returns None when no feed in the MBTA's index covers the date, which
        used to surface as AttributeError: 'NoneType' object has no attribute 'key'."""
        mock_archive = self._make_mock_archive(None)
        mock_archive.archive_url = "https://cdn.mbta.com/archive/archived_feeds.txt"

        with mock.patch("chalicelib.gtfs.boto3.resource"):
            with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                with mock.patch("chalicelib.gtfs._enqueue_gtfs_build") as mock_enqueue:
                    with self.assertRaisesRegex(RuntimeError, "No GTFS feed covers 2030-01-01"):
                        gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2030, 1, 1))

        # With no feed key there's nothing data-ingestion could build
        mock_enqueue.assert_not_called()

    def test_no_trip_ids_returns_empty_without_touching_the_feed(self):
        """Zero trip_ids used to raise IndexError. There's nothing to look up, so skip the download."""
        with mock.patch("chalicelib.gtfs.MbtaGtfsArchive") as mock_archive_cls:
            result = gtfs.fetch_stop_times_from_gtfs([], date(2024, 2, 7))

        mock_archive_cls.assert_not_called()
        self.assertTrue(result.empty)
        self.assertEqual({col: str(dtype) for col, dtype in result.dtypes.items()}, gtfs.STOP_TIMES_DTYPES)

    def test_unmatched_trips_return_the_matched_dtypes(self):
        """Against a real feed database: a query that matches no trips comes back with object
        columns, which merge_asof rejects against LAMP's string keys. The empty result has to
        match the dtypes of a real one, and the mismatch is logged as an error."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "gtfs.sqlite3")
            session = create_sqlalchemy_session(db_path)
            session.add(
                Trip(
                    **dict.fromkeys(["trip_headsign", "trip_short_name", "block_id", "shape_id"], ""),
                    route_id="Red",
                    service_id="weekday",
                    trip_id="trip1",
                    direction_id="0",
                    start_time=28800,
                    end_time=28800,
                    stop_count=1,
                    feed_info_id=1,
                )
            )
            session.add(
                StopTime(
                    **dict.fromkeys(["stop_headsign", "pickup_type", "drop_off_type"], ""),
                    trip_id="trip1",
                    stop_id="70061",
                    arrival_time=28800,
                    departure_time=28800,
                    stop_sequence=1,
                    checkpoint_id="alfcl",
                    feed_info_id=1,
                )
            )
            session.commit()

            mock_feed = self._make_mock_feed()
            mock_feed.key = "20240201"
            mock_feed.exists_locally.return_value = True
            mock_feed.create_sqlite_session.side_effect = lambda: create_sqlalchemy_session(db_path)
            mock_archive = self._make_mock_archive(mock_feed)

            with mock.patch("chalicelib.gtfs.boto3.resource"):
                with mock.patch("chalicelib.gtfs.MbtaGtfsArchive", return_value=mock_archive):
                    matched = gtfs.fetch_stop_times_from_gtfs(["trip1"], date(2024, 2, 7))
                    with self.assertLogs(gtfs.logger, "ERROR") as logs:
                        unmatched = gtfs.fetch_stop_times_from_gtfs(["ADDED-1"], date(2024, 2, 7))

        self.assertEqual(len(matched), 1)
        self.assertTrue(unmatched.empty)
        pd.testing.assert_series_equal(unmatched.dtypes, matched.dtypes)
        self.assertEqual({col: str(dtype) for col, dtype in matched.dtypes.items()}, gtfs.STOP_TIMES_DTYPES)
        self.assertIn("20240201", logs.output[0])
        self.assertIn("ADDED-1", logs.output[0])

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
