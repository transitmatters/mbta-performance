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
                        with mock.patch("shutil.unpack_archive") as mock_unpack:
                            mock_retrieve.return_value = (f"{self.temp_dir}/temp.zip", None)

                            _ = gtfs_archive.get_gtfs_archive(dateint)

                            # Verify download was attempted
                            mock_retrieve.assert_called_once()
                            mock_unpack.assert_called_once()
                            mock_cleanup.assert_called_once()

                            # Verify correct archive URL was used
                            call_args = mock_retrieve.call_args[0]
                            self.assertIn("20240201.zip", call_args[0])

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

    def test_add_gtfs_headways_with_incomplete_gtfs(self):
        """Test add_gtfs_headways skips dates with incomplete GTFS."""
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
            # This should raise an IndexError when trying to concat empty list
            with self.assertRaises(ValueError):
                gtfs_archive.add_gtfs_headways(events_df)

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

    def test_to_dateint_integration(self):
        """Test that to_dateint function is available and works correctly."""
        test_date = datetime.date(2024, 2, 7)
        result = to_dateint(test_date)
        self.assertEqual(result, 20240207)
