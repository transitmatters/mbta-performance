import datetime
import pathlib
import tempfile
import unittest
from unittest import mock

import pandas as pd

from ...date import to_dateint
from .. import gtfs_archive


class TestGTFSArchive(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.test_date = datetime.date(2024, 2, 7)
        self.test_dateint = 20240207

        # Create mock ARCHIVES dataframe
        self.mock_archives = pd.DataFrame(
            {
                "feed_start_date": [20240201, 20240301],
                "feed_end_date": [20240228, 20240331],
                "archive_url": [
                    "https://cdn.mbta.com/archive/20240201.zip",
                    "https://cdn.mbta.com/archive/20240301.zip",
                ],
            }
        )

    def tearDown(self):
        # Clean up temp directory
        import shutil

        if pathlib.Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def test_get_gtfs_archive_existing(self):
        """Test get_gtfs_archive when archive already exists."""
        dateint = 20240207
        archive_name = "20240201"

        with mock.patch.object(gtfs_archive, "ARCHIVES", self.mock_archives):
            with mock.patch.object(gtfs_archive, "MAIN_DIR", pathlib.Path(self.temp_dir)):
                # Create the archive directory
                (pathlib.Path(self.temp_dir) / archive_name).mkdir()
                for f in gtfs_archive.REQUIRED_ARCHIVE_FILES:
                    (pathlib.Path(self.temp_dir) / archive_name / f).touch()

                result = gtfs_archive.get_gtfs_archive(dateint)

                # Verify it returns the existing directory
                self.assertEqual(result, pathlib.Path(self.temp_dir) / archive_name)

    def test_get_gtfs_archive_download(self):
        """Test get_gtfs_archive when archive needs to be downloaded."""
        dateint = 20240207

        with mock.patch.object(gtfs_archive, "ARCHIVES", self.mock_archives):
            with mock.patch.object(gtfs_archive, "MAIN_DIR", pathlib.Path(self.temp_dir)):
                with mock.patch("urllib.request.urlretrieve") as mock_retrieve:
                    with mock.patch("urllib.request.urlcleanup") as mock_cleanup:
                        with mock.patch(
                            "shutil.unpack_archive",
                            side_effect=lambda _zip, extract_dir, format: pathlib.Path(extract_dir).mkdir(),
                        ) as mock_unpack:
                            mock_retrieve.return_value = (f"{self.temp_dir}/temp.zip", None)

                            result = gtfs_archive.get_gtfs_archive(dateint)

                            # Verify download was attempted
                            mock_retrieve.assert_called_once()
                            mock_unpack.assert_called_once()
                            mock_cleanup.assert_called_once()

                            # Extracted to a temp dir, then renamed into place
                            self.assertIn(".partial", str(mock_unpack.call_args.kwargs["extract_dir"]))
                            self.assertEqual(result, pathlib.Path(self.temp_dir) / "20240201")
                            self.assertTrue(result.is_dir())
                            self.assertEqual([p.name for p in pathlib.Path(self.temp_dir).iterdir()], ["20240201"])

                            # Verify correct archive URL was used
                            call_args = mock_retrieve.call_args[0]
                            self.assertIn("20240201.zip", call_args[0])

    def test_get_gtfs_archive_redownloads_incomplete_archive(self):
        """An archive dir left half-extracted by an interrupted run is replaced, not reused."""
        dateint = 20240207
        partial = pathlib.Path(self.temp_dir) / "20240201"
        partial.mkdir()
        (partial / "calendar.txt").touch()  # no trips.txt / stop_times.txt

        def unpack(_zip, extract_dir, format):
            pathlib.Path(extract_dir).mkdir()
            for f in gtfs_archive.REQUIRED_ARCHIVE_FILES:
                (pathlib.Path(extract_dir) / f).touch()

        with mock.patch.object(gtfs_archive, "ARCHIVES", self.mock_archives):
            with mock.patch.object(gtfs_archive, "MAIN_DIR", pathlib.Path(self.temp_dir)):
                with mock.patch("urllib.request.urlretrieve", return_value=(f"{self.temp_dir}/t.zip", None)):
                    with mock.patch("urllib.request.urlcleanup"):
                        with mock.patch("shutil.unpack_archive", side_effect=unpack) as mock_unpack:
                            result = gtfs_archive.get_gtfs_archive(dateint)

        mock_unpack.assert_called_once()
        self.assertTrue((result / "trips.txt").exists())

    def test_get_gtfs_archive_concurrent_download(self):
        """Test get_gtfs_archive when another process extracts the same archive first."""
        dateint = 20240207
        archive_dir = pathlib.Path(self.temp_dir) / "20240201"

        def unpack(_zip, extract_dir, format):
            pathlib.Path(extract_dir).mkdir()
            # simulate another process finishing first
            archive_dir.mkdir()
            for f in gtfs_archive.REQUIRED_ARCHIVE_FILES:
                (pathlib.Path(extract_dir) / f).touch()
                (archive_dir / f).touch()

        with mock.patch.object(gtfs_archive, "ARCHIVES", self.mock_archives):
            with mock.patch.object(gtfs_archive, "MAIN_DIR", pathlib.Path(self.temp_dir)):
                with mock.patch("urllib.request.urlretrieve", return_value=(f"{self.temp_dir}/temp.zip", None)):
                    with mock.patch("urllib.request.urlcleanup"):
                        with mock.patch("shutil.unpack_archive", side_effect=unpack):
                            result = gtfs_archive.get_gtfs_archive(dateint)

        self.assertEqual(result, archive_dir)
        self.assertEqual([p.name for p in pathlib.Path(self.temp_dir).iterdir()], ["20240201"])

    def test_main_dir_uses_gtfs_data_dir(self):
        """Test MAIN_DIR honors the GTFS_DATA_DIR env var."""
        import importlib

        try:
            with mock.patch.dict("os.environ", {"GTFS_DATA_DIR": self.temp_dir}):
                with mock.patch("pandas.read_csv", return_value=self.mock_archives):
                    importlib.reload(gtfs_archive)
                self.assertEqual(gtfs_archive.MAIN_DIR, pathlib.Path(self.temp_dir) / "gtfs_archives")
                self.assertTrue(gtfs_archive.MAIN_DIR.is_dir())
        finally:
            with mock.patch("pandas.read_csv", return_value=self.mock_archives):
                importlib.reload(gtfs_archive)

    def test_get_services(self):
        """Test get_services returns correct service IDs for a date."""
        test_date = datetime.date(2024, 2, 7)  # Wednesday
        archive_dir = pathlib.Path(self.temp_dir)

        # Create mock calendar.txt
        calendar_data = pd.DataFrame(
            {
                "service_id": ["weekday", "weekend", "special"],
                "monday": [1, 0, 0],
                "tuesday": [1, 0, 0],
                "wednesday": [1, 0, 1],
                "thursday": [1, 0, 0],
                "friday": [1, 0, 0],
                "saturday": [0, 1, 0],
                "sunday": [0, 1, 0],
                "start_date": [20240101, 20240101, 20240201],
                "end_date": [20241231, 20241231, 20240229],
            }
        )

        # Create mock calendar_dates.txt
        calendar_dates_data = pd.DataFrame(
            {
                "service_id": ["special", "weekday"],
                "date": [20240207, 20240207],
                "exception_type": [2, 1],  # 2=removed, 1=added
            }
        )

        with mock.patch("pandas.read_csv") as mock_read_csv:

            def side_effect(path):
                if "calendar.txt" in str(path):
                    return calendar_data
                elif "calendar_dates.txt" in str(path):
                    return calendar_dates_data

            mock_read_csv.side_effect = side_effect

            result = gtfs_archive.get_services(test_date, archive_dir)

            # Wednesday should have 'weekday', not 'special' (removed), and 'weekday' added again
            self.assertIn("weekday", result)
            self.assertNotIn("special", result)
            self.assertNotIn("weekend", result)

    def test_read_gtfs_success(self):
        """Test read_gtfs successfully returns trips and stops."""
        test_date = datetime.date(2024, 2, 7)
        archive_dir = pathlib.Path(self.temp_dir) / "archive"
        archive_dir.mkdir()

        # Create mock files
        (archive_dir / "trips.txt").touch()
        (archive_dir / "stop_times.txt").touch()

        # Mock data
        mock_trips = pd.DataFrame(
            {
                "route_id": ["Red", "Blue"],
                "service_id": ["weekday", "weekday"],
                "trip_id": ["trip1", "trip2"],
            }
        )

        mock_stops = pd.DataFrame(
            {
                "trip_id": ["trip1", "trip1", "trip2"],
                "stop_id": ["stop1", "stop2", "stop1"],
                "arrival_time": ["08:00:00", "08:10:00", "09:00:00"],
                "departure_time": ["08:00:00", "08:10:00", "09:00:00"],
            }
        )

        with mock.patch("chalicelib.historic.gtfs_archive.get_gtfs_archive", return_value=archive_dir):
            with mock.patch("chalicelib.historic.gtfs_archive.get_services", return_value=["weekday"]):
                with mock.patch("pandas.read_csv") as mock_read_csv:

                    def side_effect(path, **kwargs):
                        if "trips.txt" in str(path):
                            return mock_trips
                        elif "stop_times.txt" in str(path):
                            return mock_stops

                    mock_read_csv.side_effect = side_effect

                    trips, stops = gtfs_archive.read_gtfs(test_date)

                    # Verify results
                    self.assertIsInstance(trips, pd.DataFrame)
                    self.assertIsInstance(stops, pd.DataFrame)
                    self.assertEqual(len(trips), 2)
                    self.assertEqual(len(stops), 3)

    def test_read_gtfs_missing_trips_file(self):
        """Test read_gtfs when trips.txt is missing."""
        test_date = datetime.date(2024, 2, 7)
        archive_dir = pathlib.Path(self.temp_dir) / "archive"
        archive_dir.mkdir()

        # Only create stop_times.txt, not trips.txt
        (archive_dir / "stop_times.txt").touch()

        with mock.patch("chalicelib.historic.gtfs_archive.get_gtfs_archive", return_value=archive_dir):
            trips, stops = gtfs_archive.read_gtfs(test_date)

            # Should return None, None
            self.assertIsNone(trips)
            self.assertIsNone(stops)

    def test_read_gtfs_missing_stop_times_file(self):
        """Test read_gtfs when stop_times.txt is missing."""
        test_date = datetime.date(2024, 2, 7)
        archive_dir = pathlib.Path(self.temp_dir) / "archive"
        archive_dir.mkdir()

        # Only create trips.txt, not stop_times.txt
        (archive_dir / "trips.txt").touch()

        with mock.patch("chalicelib.historic.gtfs_archive.get_gtfs_archive", return_value=archive_dir):
            trips, stops = gtfs_archive.read_gtfs(test_date)

            # Should return None, None
            self.assertIsNone(trips)
            self.assertIsNone(stops)

    def test_read_gtfs_exception_handling(self):
        """Test read_gtfs handles exceptions gracefully."""
        test_date = datetime.date(2024, 2, 7)
        archive_dir = pathlib.Path(self.temp_dir) / "archive"
        archive_dir.mkdir()

        # Create mock files
        (archive_dir / "trips.txt").touch()
        (archive_dir / "stop_times.txt").touch()

        with mock.patch("chalicelib.historic.gtfs_archive.get_gtfs_archive", return_value=archive_dir):
            with mock.patch("chalicelib.historic.gtfs_archive.get_services", return_value=["weekday"]):
                with mock.patch("pandas.read_csv", side_effect=Exception("Read error")):
                    trips, stops = gtfs_archive.read_gtfs(test_date)

                    # Should return None, None on exception
                    self.assertIsNone(trips)
                    self.assertIsNone(stops)

    def test_add_gtfs_headways(self):
        """Test add_gtfs_headways enriches events with GTFS data."""
        # Create mock events dataframe
        events_df = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2024-02-07")] * 4,
                "route_id": ["Red", "Red", "Red", "Red"],
                "direction_id": [0, 0, 0, 0],
                "stop_id": ["stop1", "stop1", "stop1", "stop1"],
                "trip_id": ["trip1", "trip2", "trip3", "trip4"],
                "event_time": [
                    pd.Timestamp("2024-02-07 08:00:00"),
                    pd.Timestamp("2024-02-07 08:10:00"),
                    pd.Timestamp("2024-02-07 08:20:00"),
                    pd.Timestamp("2024-02-07 08:30:00"),
                ],
            }
        )

        # Create mock GTFS data
        mock_trips = pd.DataFrame(
            {
                "trip_id": ["sched1", "sched2", "sched3"],
                "route_id": ["Red", "Red", "Red"],
                "direction_id": [0, 0, 0],
            }
        )

        mock_stops = pd.DataFrame(
            {
                "trip_id": ["sched1", "sched1", "sched2", "sched2", "sched3", "sched3"],
                "stop_id": ["stop1", "stop2", "stop1", "stop2", "stop1", "stop2"],
                "arrival_time": [
                    pd.Timedelta(hours=8),
                    pd.Timedelta(hours=8, minutes=5),
                    pd.Timedelta(hours=8, minutes=10),
                    pd.Timedelta(hours=8, minutes=15),
                    pd.Timedelta(hours=8, minutes=20),
                    pd.Timedelta(hours=8, minutes=25),
                ],
            }
        )

        with mock.patch("chalicelib.historic.gtfs_archive.read_gtfs", return_value=(mock_trips, mock_stops)):
            result = gtfs_archive.add_gtfs_headways(events_df)

            # Verify result is a dataframe
            self.assertIsInstance(result, pd.DataFrame)

            # Verify scheduled_headway and scheduled_tt columns exist
            self.assertIn("scheduled_headway", result.columns)
            self.assertIn("scheduled_tt", result.columns)

    def _shared_stop_gtfs(self, route_ids):
        """Four scheduled trips alternating between two routes, each starting at its own stop and
        reaching the shared stop 5 minutes later, at 8:00, 8:04, 8:10 and 8:14."""
        offsets = [0, 4, 10, 14]
        trips = pd.DataFrame(
            {
                "trip_id": [f"sched{i}" for i in range(4)],
                "route_id": [route_ids[i % 2] for i in range(4)],
                "direction_id": [1] * 4,
            }
        )
        stops = pd.DataFrame(
            {
                "trip_id": [f"sched{i}" for i in range(4) for _ in range(2)],
                "stop_id": [s for i in range(4) for s in (f"branch-{route_ids[i % 2]}", "shared")],
                "arrival_time": [
                    pd.Timedelta(hours=8, minutes=offset + delay) for offset in offsets for delay in (-5, 0)
                ],
            }
        )
        events = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2024-02-07")] * 4,
                "route_id": list(trips.route_id),
                "direction_id": [1] * 4,
                "stop_id": ["shared"] * 4,
                "trip_id": [f"actual{i}" for i in range(4)],
                "event_time": [pd.Timestamp("2024-02-07 08:00:30") + pd.Timedelta(minutes=o) for o in offsets],
            }
        )
        return trips, stops, events

    def test_add_gtfs_headways_pools_green_branches_on_trunk(self):
        """Green Line branches sharing a stop get trunk scheduled headways, but keep per-branch travel times."""
        trips, stops, events = self._shared_stop_gtfs(["Green-B", "Green-C"])

        with mock.patch("chalicelib.historic.gtfs_archive.read_gtfs", return_value=(trips, stops)):
            result = gtfs_archive.add_gtfs_headways(events).sort_values("event_time")

        # gaps between consecutive trains of either branch, not between trains of the same branch (600s)
        self.assertListEqual(list(result.scheduled_headway[1:]), [240, 360, 240])
        self.assertListEqual(list(result.scheduled_tt), [300] * 4)
        self.assertNotIn("trunk_route_id", result.columns)

    def test_add_gtfs_headways_keeps_bus_routes_separate(self):
        """Routes without a trunk (bus) still get per-route scheduled headways at a shared stop."""
        trips, stops, events = self._shared_stop_gtfs(["1", "47"])

        with mock.patch("chalicelib.historic.gtfs_archive.read_gtfs", return_value=(trips, stops)):
            result = gtfs_archive.add_gtfs_headways(events).sort_values("event_time")

        self.assertListEqual(list(result.scheduled_headway[2:]), [600, 600])
        self.assertTrue(result.scheduled_headway[:2].isna().all())

    def test_add_gtfs_headways_with_incomplete_gtfs(self):
        """add_gtfs_headways keeps events for dates with incomplete GTFS, without scheduled values."""
        events_df = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2024-02-07"), pd.Timestamp("2024-02-08")],
                "route_id": ["Red", "Red"],
                "direction_id": [0, 0],
                "stop_id": ["stop1", "stop1"],
                "trip_id": ["trip1", "trip2"],
                "event_time": [pd.Timestamp("2024-02-07 08:00:00"), pd.Timestamp("2024-02-08 08:00:00")],
            }
        )

        # Return None for incomplete GTFS
        with mock.patch("chalicelib.historic.gtfs_archive.read_gtfs", return_value=(None, None)):
            result = gtfs_archive.add_gtfs_headways(events_df)

        self.assertEqual(len(result), 2)
        self.assertEqual(list(result.trip_id), ["trip1", "trip2"])

    def test_add_gtfs_headways_empty_events(self):
        """Test add_gtfs_headways with empty events dataframe."""
        events_df = pd.DataFrame(
            {
                "service_date": [],
                "route_id": [],
                "direction_id": [],
                "stop_id": [],
                "trip_id": [],
                "event_time": [],
            }
        )

        # Should raise ValueError for empty concat
        with self.assertRaises(ValueError):
            gtfs_archive.add_gtfs_headways(events_df)

    def test_add_gtfs_headways_no_matching_routes(self):
        """add_gtfs_headways skips a date when no GTFS trips match the events' route."""
        events_df = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2024-02-07")],
                "route_id": ["NonExistentRoute"],
                "direction_id": [0],
                "stop_id": ["stop1"],
                "trip_id": ["trip1"],
                "event_time": [pd.Timestamp("2024-02-07 08:00:00")],
            }
        )

        mock_trips = pd.DataFrame(
            {
                "trip_id": ["sched1"],
                "route_id": ["Red"],  # does not match events_df
                "direction_id": [0],
            }
        )
        mock_stops = pd.DataFrame(
            {
                "trip_id": ["sched1"],
                "stop_id": ["stop1"],
                "arrival_time": [pd.Timedelta(hours=8)],
            }
        )

        with mock.patch("chalicelib.historic.gtfs_archive.read_gtfs", return_value=(mock_trips, mock_stops)):
            # All dates are skipped (empty gtfs_stops_clean) → results list is empty → ValueError on concat
            with self.assertRaises(ValueError):
                gtfs_archive.add_gtfs_headways(events_df)

    def test_add_gtfs_headways_null_event_times(self):
        """add_gtfs_headways skips a date when all event_times are null (arrival_time becomes NaT)."""
        events_df = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2024-02-07")],
                "route_id": ["Red"],
                "direction_id": [0],
                "stop_id": ["stop1"],
                "trip_id": ["trip1"],
                "event_time": [pd.NaT],  # null event_time → arrival_time = NaT after subtraction
            }
        )

        mock_trips = pd.DataFrame({"trip_id": ["sched1"], "route_id": ["Red"], "direction_id": [0]})
        mock_stops = pd.DataFrame(
            {"trip_id": ["sched1"], "stop_id": ["stop1"], "arrival_time": [pd.Timedelta(hours=8)]}
        )

        with mock.patch("chalicelib.historic.gtfs_archive.read_gtfs", return_value=(mock_trips, mock_stops)):
            # days_events_clean becomes empty after dropna on arrival_time → results list empty → ValueError
            with self.assertRaises(ValueError):
                gtfs_archive.add_gtfs_headways(events_df)

    def test_to_dateint_integration(self):
        """Test that to_dateint function is available and works correctly."""
        test_date = datetime.date(2024, 2, 7)
        result = to_dateint(test_date)
        self.assertEqual(result, 20240207)
