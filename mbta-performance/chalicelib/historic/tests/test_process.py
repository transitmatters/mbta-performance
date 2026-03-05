import datetime
import pathlib
import tempfile
import unittest
from unittest import mock

import pandas as pd

from .. import process


class TestProcess(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.test_csv = pathlib.Path(self.temp_dir) / "test.csv"

        # Create sample event data
        self.sample_events = pd.DataFrame(
            {
                "service_date": ["2024-02-07", "2024-02-07", "2024-02-07"],
                "route_id": ["Red", "Red", "Red"],
                "trip_id": ["trip1", "trip1", "trip2"],
                "direction_id": [0, 0, 0],
                "stop_id": ["70061", "70063", "70061"],
                "stop_sequence": [1, 2, 1],
                "vehicle_id": ["R-001", "R-001", "R-002"],
                "vehicle_label": ["1801", "1801", "1802"],
                "event_type": ["DEP", "ARR", "DEP"],
                "event_time_sec": [28800, 28920, 29100],
            }
        )

    def tearDown(self):
        # Clean up temp directory
        import shutil

        if pathlib.Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def test_process_events(self):
        """Test process_events reads and processes CSV correctly."""
        # Write test CSV
        self.sample_events.to_csv(self.test_csv, index=False)

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=lambda x: x):
            with mock.patch("chalicelib.historic.process.to_disk") as mock_to_disk:
                process.process_events(str(self.test_csv), self.temp_dir, nozip=True)

                # Verify to_disk was called
                mock_to_disk.assert_called_once()

                # Get the dataframe that was passed to to_disk
                call_args = mock_to_disk.call_args[0]
                df = call_args[0]

                # Verify event_time column was created and is datetime
                self.assertIn("event_time", df.columns)
                self.assertTrue(pd.api.types.is_datetime64_any_dtype(df["event_time"]))

    def test_process_events_with_sync_stop_sequence(self):
        """Test process_events handles sync_stop_sequence column rename."""
        events_with_sync = self.sample_events.copy()
        events_with_sync.rename(columns={"stop_sequence": "sync_stop_sequence"}, inplace=True)
        events_with_sync.to_csv(self.test_csv, index=False)

        # Import the LAMP constants which have sync_stop_sequence
        from ..constants import HISTORIC_COLUMNS_LAMP

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=lambda x: x):
            with mock.patch("chalicelib.historic.process.to_disk") as mock_to_disk:
                # Use HISTORIC_COLUMNS_LAMP which includes sync_stop_sequence
                process.process_events(str(self.test_csv), self.temp_dir, nozip=True, columns=HISTORIC_COLUMNS_LAMP)

                # Get the dataframe
                df = mock_to_disk.call_args[0][0]

                # Verify column was renamed
                self.assertIn("stop_sequence", df.columns)
                self.assertNotIn("sync_stop_sequence", df.columns)

    def test_process_events_gtfs_failure(self):
        """Test process_events handles GTFS failures gracefully."""
        self.sample_events.to_csv(self.test_csv, index=False)

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=IndexError("GTFS error")):
            with mock.patch("chalicelib.historic.process.to_disk") as mock_to_disk:
                # Should not raise exception
                process.process_events(str(self.test_csv), self.temp_dir, nozip=True)

                # Verify to_disk was still called
                mock_to_disk.assert_called_once()

    def test_to_disk(self):
        """Test to_disk creates proper directory structure and files."""
        events_df = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2024-02-07"), pd.Timestamp("2024-02-07")],
                "stop_id": ["70061", "70061"],
                "route_id": ["Red", "Red"],
                "direction_id": [0, 0],
                "trip_id": ["trip1", "trip1"],
                "event_type": ["ARR", "DEP"],
            }
        )

        with mock.patch("pathlib.Path.mkdir") as mock_mkdir:
            with mock.patch("pandas.DataFrame.to_csv") as mock_to_csv:
                process.to_disk(events_df, self.temp_dir, nozip=False)

                # Verify directory was created
                mock_mkdir.assert_called()

                # Verify CSV was written with gzip compression
                mock_to_csv.assert_called_once()
                call_kwargs = mock_to_csv.call_args[1]
                self.assertIn("compression", call_kwargs)
                self.assertEqual(call_kwargs["compression"]["method"], "gzip")

    def test_to_disk_nozip(self):
        """Test to_disk without compression."""
        events_df = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2024-02-07")],
                "stop_id": ["70061"],
                "route_id": ["Red"],
                "direction_id": [0],
                "trip_id": ["trip1"],
                "event_type": ["ARR"],
            }
        )

        with mock.patch("pathlib.Path.mkdir"):
            with mock.patch("pandas.DataFrame.to_csv") as mock_to_csv:
                process.to_disk(events_df, self.temp_dir, nozip=True)

                # Verify CSV was written without compression
                call_kwargs = mock_to_csv.call_args[1]
                self.assertIsNone(call_kwargs["compression"])

    def test_to_disk_ferry(self):
        """Test to_disk_ferry creates proper ferry directory structure."""
        events_df = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2024-02-07")],
                "stop_id": ["Boat-Long"],
                "route_id": ["Boat-F1"],
                "direction_id": [0],
                "trip_id": ["ferry1"],
                "event_type": ["ARR"],
            }
        )

        with mock.patch("pathlib.Path.mkdir") as mock_mkdir:
            with mock.patch("pandas.DataFrame.to_csv") as mock_to_csv:
                process.to_disk_ferry(events_df, self.temp_dir, nozip=False)

                # Verify directory was created
                mock_mkdir.assert_called()

                # Verify CSV was written
                mock_to_csv.assert_called_once()

                # Verify correct path structure (route_id|direction_id|stop_id)
                call_args = mock_to_csv.call_args[0]
                path = str(call_args[0])
                self.assertIn("Boat-F1|0|Boat-Long", path)

    def test_process_ferry(self):
        """Test process_ferry processes ferry data correctly."""
        ferry_data = pd.DataFrame(
            {
                "service_date": ["2024-02-07 00:00:00+00:00", "2024-02-07 00:00:00+00:00"],
                "route_id": ["F1", "F1"],
                "trip_id": ["trip1", "trip1"],
                "travel_direction": ["To Boston", "From Boston"],
                "departure_terminal": ["Hingham", "Boston"],
                "arrival_terminal": ["Boston", "Hingham"],
                "mbta_sched_arrival": ["2024-02-07 08:00:00+00:00", "2024-02-07 09:00:00+00:00"],
                "mbta_sched_departure": ["2024-02-07 07:45:00+00:00", "2024-02-07 08:45:00+00:00"],
                "actual_arrival": ["2024-02-07 08:02:00", "2024-02-07 09:01:00"],
                "actual_departure": ["2024-02-07 07:46:00", "2024-02-07 08:46:00"],
                "vessel_time_slot": ["slot1", "slot1"],
            }
        )

        ferry_csv = pathlib.Path(self.temp_dir) / "ferry.csv"
        ferry_data.to_csv(ferry_csv, index=False)

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=lambda x: x):
            with mock.patch("chalicelib.historic.process.to_disk_ferry") as mock_to_disk:
                process.process_ferry(str(ferry_csv), self.temp_dir, nozip=True)

                # Verify to_disk_ferry was called
                mock_to_disk.assert_called_once()

                # Get the dataframe
                df = mock_to_disk.call_args[0][0]

                # Verify direction mapping was applied
                self.assertIn(1, df["direction_id"].values)  # To Boston -> 1
                self.assertIn(0, df["direction_id"].values)  # From Boston -> 0

                # Verify route mapping was applied
                self.assertTrue(all(df["route_id"].str.startswith("Boat-")))

    def test_process_ferry_with_date_filtering(self):
        """Test process_ferry with date range filtering."""
        ferry_data = pd.DataFrame(
            {
                "service_date": [
                    "2024-02-05 00:00:00+00:00",
                    "2024-02-07 00:00:00+00:00",
                    "2024-02-10 00:00:00+00:00",
                ],
                "route_id": ["F1", "F1", "F1"],
                "trip_id": ["trip1", "trip2", "trip3"],
                "travel_direction": ["To Boston", "To Boston", "To Boston"],
                "departure_terminal": ["Hingham", "Hingham", "Hingham"],
                "arrival_terminal": ["Boston", "Boston", "Boston"],
                "mbta_sched_arrival": [
                    "2024-02-05 08:00:00+00:00",
                    "2024-02-07 08:00:00+00:00",
                    "2024-02-10 08:00:00+00:00",
                ],
                "mbta_sched_departure": [
                    "2024-02-05 07:45:00+00:00",
                    "2024-02-07 07:45:00+00:00",
                    "2024-02-10 07:45:00+00:00",
                ],
                "actual_arrival": ["2024-02-05 08:02:00", "2024-02-07 08:02:00", "2024-02-10 08:02:00"],
                "actual_departure": ["2024-02-05 07:46:00", "2024-02-07 07:46:00", "2024-02-10 07:46:00"],
                "vessel_time_slot": ["slot1", "slot1", "slot1"],
            }
        )

        ferry_csv = pathlib.Path(self.temp_dir) / "ferry.csv"
        ferry_data.to_csv(ferry_csv, index=False)

        start_date = datetime.date(2024, 2, 6)
        end_date = datetime.date(2024, 2, 8)

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=lambda x: x):
            with mock.patch("chalicelib.historic.process.to_disk_ferry") as mock_to_disk:
                process.process_ferry(
                    str(ferry_csv), self.temp_dir, nozip=True, start_date=start_date, end_date=end_date
                )

                # Get the dataframe
                df = mock_to_disk.call_args[0][0]

                # Should only have data from 2024-02-07 (within range)
                # Multiply by 2 because each trip generates ARR and DEP events
                self.assertEqual(len(df), 2)

    def test_process_ferry_missing_trip_ids(self):
        """Test process_ferry generates UUIDs for missing trip_ids."""
        ferry_data = pd.DataFrame(
            {
                "service_date": ["2024-02-07 00:00:00+00:00", "2024-02-07 00:00:00+00:00"],
                "route_id": ["F1", "F1"],
                "trip_id": [None, None],  # Missing trip IDs
                "travel_direction": ["To Boston", "To Boston"],
                "departure_terminal": ["Hingham", "Hingham"],
                "arrival_terminal": ["Boston", "Boston"],
                "mbta_sched_arrival": ["2024-02-07 08:00:00+00:00", "2024-02-07 09:00:00+00:00"],
                "mbta_sched_departure": ["2024-02-07 07:45:00+00:00", "2024-02-07 08:45:00+00:00"],
                "actual_arrival": ["2024-02-07 08:02:00", "2024-02-07 09:01:00"],
                "actual_departure": ["2024-02-07 07:46:00", "2024-02-07 08:46:00"],
                "vessel_time_slot": ["slot1", "slot1"],
            }
        )

        ferry_csv = pathlib.Path(self.temp_dir) / "ferry.csv"
        ferry_data.to_csv(ferry_csv, index=False)

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=lambda x: x):
            with mock.patch("chalicelib.historic.process.to_disk_ferry") as mock_to_disk:
                process.process_ferry(str(ferry_csv), self.temp_dir, nozip=True)

                # Get the dataframe
                df = mock_to_disk.call_args[0][0]

                # Verify trip_ids were generated (should not be null)
                self.assertFalse(df["trip_id"].isna().any())

    def test_load_bus_data(self):
        """Test load_bus_data reads and transforms bus data correctly."""
        bus_data = pd.DataFrame(
            {
                "service_date": ["2020-01-15", "2020-01-15"],
                "route_id": ["01", "01"],
                "direction": ["Inbound", "Outbound"],
                "half_trip_id": ["46374001", "46374002"],
                "stop_id": ["67", "110"],
                "time_point_id": ["maput", "hhgat"],
                "time_point_order": [2, 1],
                "point_type": ["Midpoint", "Startpoint"],
                "standard_type": ["Schedule", "Schedule"],
                "scheduled": ["1900-01-01 05:08:00", "1900-01-01 05:05:00"],
                "actual": ["1900-01-01 05:09:07", "1900-01-01 05:04:34"],
                "scheduled_headway": [-5, 26],
                "headway": [None, None],
            }
        )

        bus_csv = pathlib.Path(self.temp_dir) / "bus.csv"
        bus_data.to_csv(bus_csv, index=False)

        result = process.load_bus_data(str(bus_csv))

        # Verify route_id leading zeros stripped
        self.assertTrue(all(result["route_id"] == "1"))

        # Verify direction_id mapping
        self.assertIn("direction_id", result.columns)
        self.assertEqual(result["direction_id"].iloc[0], 1)  # Inbound -> 1
        self.assertEqual(result["direction_id"].iloc[1], 0)  # Outbound -> 0

        # Verify dates were adjusted from 1900 base
        self.assertTrue(all(result["scheduled"].dt.year == 2020))
        self.assertTrue(all(result["actual"].dt.year == 2020))

    def test_load_bus_data_with_route_filter(self):
        """Test load_bus_data filters routes correctly."""
        bus_data = pd.DataFrame(
            {
                "service_date": ["2020-01-15", "2020-01-15", "2020-01-15"],
                "route_id": ["01", "28", "39"],
                "direction": ["Inbound", "Inbound", "Inbound"],
                "half_trip_id": ["1", "2", "3"],
                "stop_id": ["67", "68", "69"],
                "time_point_id": ["stop1", "stop2", "stop3"],
                "time_point_order": [1, 1, 1],
                "point_type": ["Startpoint", "Startpoint", "Startpoint"],
                "standard_type": ["Schedule", "Schedule", "Schedule"],
                "scheduled": ["1900-01-01 05:08:00", "1900-01-01 05:08:00", "1900-01-01 05:08:00"],
                "actual": ["1900-01-01 05:09:07", "1900-01-01 05:09:07", "1900-01-01 05:09:07"],
                "scheduled_headway": [0, 0, 0],
                "headway": [None, None, None],
            }
        )

        bus_csv = pathlib.Path(self.temp_dir) / "bus.csv"
        bus_data.to_csv(bus_csv, index=False)

        result = process.load_bus_data(str(bus_csv), routes=["1", "28"])

        # Should only have routes 1 and 28
        self.assertEqual(len(result), 2)
        self.assertIn("1", result["route_id"].values)
        self.assertIn("28", result["route_id"].values)
        self.assertNotIn("39", result["route_id"].values)

    def test_process_bus_events_data(self):
        """Test process_bus_events_data transforms bus data correctly."""
        bus_df = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2020-01-15"), pd.Timestamp("2020-01-15"), pd.Timestamp("2020-01-15")],
                "route_id": ["1", "1", "1"],
                "half_trip_id": ["trip1", "trip1", "trip1"],
                "direction_id": [1, 1, 1],
                "stop_id": ["67", "68", "69"],
                "time_point_id": ["start", "mid", "end"],
                "time_point_order": [1, 2, 3],
                "point_type": ["Startpoint", "Midpoint", "Endpoint"],
                "standard_type": ["Schedule", "Schedule", "Schedule"],
                "scheduled": [
                    pd.Timestamp("2020-01-15 05:00:00"),
                    pd.Timestamp("2020-01-15 05:10:00"),
                    pd.Timestamp("2020-01-15 05:20:00"),
                ],
                "actual": [
                    pd.Timestamp("2020-01-15 05:01:00"),
                    pd.Timestamp("2020-01-15 05:11:00"),
                    pd.Timestamp("2020-01-15 05:21:00"),
                ],
                "scheduled_headway": [0, 600, 600],
                "headway": [None, None, None],
            }
        )

        # Mock add_gtfs_headways to add the required columns
        def mock_add_gtfs(df):
            df["scheduled_headway"] = 600
            df["scheduled_tt"] = 0
            return df

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=mock_add_gtfs):
            result = process.process_bus_events_data(bus_df)

            # Verify event_type was created
            self.assertIn("event_type", result.columns)

            # After processing, time_point_order is renamed to stop_sequence
            # Startpoint (stop_sequence 1) should have DEP
            startpoint = result[result["stop_sequence"] == 1]
            self.assertTrue(all(startpoint["event_type"] == "DEP"))

            # Midpoint (stop_sequence 2) should have ARR and DEP
            midpoint = result[result["stop_sequence"] == 2]
            self.assertEqual(len(midpoint), 2)
            self.assertIn("ARR", midpoint["event_type"].values)
            self.assertIn("DEP", midpoint["event_type"].values)

            # Endpoint (stop_sequence 3) should have ARR
            endpoint = result[result["stop_sequence"] == 3]
            self.assertTrue(all(endpoint["event_type"] == "ARR"))

    def test_to_disk_bus(self):
        """Test to_disk_bus creates proper bus directory structure."""
        events_df = pd.DataFrame(
            {
                "service_date": [pd.Timestamp("2024-02-07")],
                "stop_id": ["123"],
                "route_id": ["1"],
                "direction_id": [0],
                "trip_id": ["trip1"],
                "event_type": ["ARR"],
                "event_time": [pd.Timestamp("2024-02-07 08:30:00")],
            }
        )

        with mock.patch("pathlib.Path.mkdir") as mock_mkdir:
            with mock.patch("pandas.DataFrame.to_csv") as mock_to_csv:
                process.to_disk_bus(events_df, self.temp_dir, nozip=False)

                # Verify directory was created
                mock_mkdir.assert_called()

                # Verify CSV was written
                mock_to_csv.assert_called_once()

                # Verify correct path structure (route_id-direction_id-stop_id)
                call_args = mock_to_csv.call_args[0]
                path = str(call_args[0])
                self.assertIn("1-0-123", path)
                self.assertIn("monthly-bus-data", path)

    def test_process_bus_events(self):
        """Test process_bus_events end-to-end."""
        bus_data = pd.DataFrame(
            {
                "service_date": ["2020-01-15"],
                "route_id": ["01"],
                "direction": ["Inbound"],
                "half_trip_id": ["46374001"],
                "stop_id": ["67"],
                "time_point_id": ["maput"],
                "time_point_order": [2],
                "point_type": ["Midpoint"],
                "standard_type": ["Schedule"],
                "scheduled": ["1900-01-01 05:08:00"],
                "actual": ["1900-01-01 05:09:07"],
                "scheduled_headway": [-5],
                "headway": [None],
            }
        )

        bus_csv = pathlib.Path(self.temp_dir) / "bus.csv"
        bus_data.to_csv(bus_csv, index=False)

        # Mock add_gtfs_headways to add the required columns
        def mock_add_gtfs(df):
            df["scheduled_headway"] = 600
            df["scheduled_tt"] = 0
            return df

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=mock_add_gtfs):
            with mock.patch("chalicelib.historic.process.to_disk_bus") as mock_to_disk:
                process.process_bus_events(str(bus_csv), self.temp_dir, nozip=True)

                # Verify to_disk_bus was called
                mock_to_disk.assert_called_once()

    def test_load_bus_data_overnight_trip_offset(self):
        """Bus rows with a 1900-01-02 base date represent next-day (overnight) trips.

        The day offset (1900-01-0X where X-1 is the number of extra days) should be
        added to service_date so the output timestamp is on the following calendar day.
        """
        bus_data = pd.DataFrame(
            {
                "service_date": ["2020-01-15"],
                "route_id": ["01"],
                "direction": ["Inbound"],
                "half_trip_id": ["46374001"],
                "stop_id": ["67"],
                "time_point_id": ["maput"],
                "time_point_order": [2],
                "point_type": ["Midpoint"],
                "standard_type": ["Schedule"],
                # 1900-01-02 means +1 day from service_date
                "scheduled": ["1900-01-02 01:30:00"],
                "actual": ["1900-01-02 01:35:00"],
                "scheduled_headway": [0],
                "headway": [None],
            }
        )

        bus_csv = pathlib.Path(self.temp_dir) / "bus_overnight.csv"
        bus_data.to_csv(bus_csv, index=False)

        result = process.load_bus_data(str(bus_csv))

        # service_date 2020-01-15 + 1 day offset + 01:30:00 → 2020-01-16 01:30:00
        expected_scheduled = datetime.datetime(2020, 1, 16, 1, 30, 0)
        expected_actual = datetime.datetime(2020, 1, 16, 1, 35, 0)
        self.assertEqual(result.iloc[0]["scheduled"], expected_scheduled)
        self.assertEqual(result.iloc[0]["actual"], expected_actual)

    def test_load_bus_data_pre_june_2024_z_suffix_treated_as_eastern(self):
        """Pre-June 2024 bus data with a Z suffix is treated as Eastern Time, not UTC.

        Before June 2024 the MBTA labelled Eastern times with a Z suffix.
        The loader detects this (service_date < 2024-06-01) and does NOT apply
        a UTC→Eastern conversion, so the output time equals the wall-clock value
        from the raw data.
        """
        bus_data = pd.DataFrame(
            {
                "service_date": ["2024-01-15"],
                "route_id": ["01"],
                "direction": ["Inbound"],
                "half_trip_id": ["12345"],
                "stop_id": ["110"],
                "time_point_id": ["hhgat"],
                "time_point_order": [1],
                "point_type": ["Startpoint"],
                "standard_type": ["Schedule"],
                # Z suffix but service_date is before June 2024 → treat as Eastern, no tz shift
                "scheduled": ["1900-01-01T08:05:00Z"],
                "actual": ["1900-01-01T08:06:00Z"],
                "scheduled_headway": [0],
                "headway": [None],
            }
        )

        bus_csv = pathlib.Path(self.temp_dir) / "bus_pre_june.csv"
        bus_data.to_csv(bus_csv, index=False)

        result = process.load_bus_data(str(bus_csv))

        # Pre-June 2024: the "Z" is ignored as a UTC indicator.
        # 08:06 stays as 08:06 on the service date (no UTC→Eastern shift).
        expected_actual = datetime.datetime(2024, 1, 15, 8, 6, 0)
        self.assertEqual(result.iloc[0]["actual"], expected_actual)

    def test_process_ferry_all_rows_filtered(self):
        """process_ferry returns early when date filtering removes all rows."""
        ferry_data = pd.DataFrame(
            {
                "service_date": ["2024-02-07 00:00:00+00:00"],
                "route_id": ["F1"],
                "trip_id": ["trip1"],
                "travel_direction": ["To Boston"],
                "departure_terminal": ["Hingham"],
                "arrival_terminal": ["Boston"],
                "mbta_sched_arrival": ["2024-02-07 08:00:00+00:00"],
                "mbta_sched_departure": ["2024-02-07 07:45:00+00:00"],
                "actual_arrival": ["2024-02-07 08:02:00"],
                "actual_departure": ["2024-02-07 07:46:00"],
                "vessel_time_slot": ["slot1"],
            }
        )

        ferry_csv = pathlib.Path(self.temp_dir) / "ferry_filtered.csv"
        ferry_data.to_csv(ferry_csv, index=False)

        with mock.patch("chalicelib.historic.process.to_disk_ferry") as mock_to_disk:
            # start_date in March means all Feb data is filtered out
            process.process_ferry(
                str(ferry_csv),
                self.temp_dir,
                nozip=True,
                start_date=datetime.date(2024, 3, 1),
            )

            # to_disk_ferry should never be called — function returned early
            mock_to_disk.assert_not_called()

    def test_process_ferry_gtfs_failure(self):
        """process_ferry continues to write output even when add_gtfs_headways raises IndexError."""
        ferry_data = pd.DataFrame(
            {
                "service_date": ["2024-02-07 00:00:00+00:00"],
                "route_id": ["F1"],
                "trip_id": ["trip1"],
                "travel_direction": ["To Boston"],
                "departure_terminal": ["Hingham"],
                "arrival_terminal": ["Boston"],
                "mbta_sched_arrival": ["2024-02-07 08:00:00+00:00"],
                "mbta_sched_departure": ["2024-02-07 07:45:00+00:00"],
                "actual_arrival": ["2024-02-07 08:02:00"],
                "actual_departure": ["2024-02-07 07:46:00"],
                "vessel_time_slot": ["slot1"],
            }
        )

        ferry_csv = pathlib.Path(self.temp_dir) / "ferry_gtfs_fail.csv"
        ferry_data.to_csv(ferry_csv, index=False)

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=IndexError("GTFS error")):
            with mock.patch("chalicelib.historic.process.to_disk_ferry") as mock_to_disk:
                process.process_ferry(str(ferry_csv), self.temp_dir, nozip=True)

                # to_disk_ferry should still be called despite the GTFS failure
                mock_to_disk.assert_called_once()

    def test_load_bus_data_utc_conversion(self):
        """Test that post-June 2024 bus data is correctly converted from UTC to Eastern Time.

        MBTA changed their data format around June 2024:
        - Before: Times were Eastern Time (mislabeled with Z suffix)
        - After: Times are actual UTC

        This test verifies that UTC times are properly converted to Eastern.
        """
        # Post-June 2024 data with UTC times
        # 10:05:00Z UTC = 06:05:00 EDT (UTC-4 in summer)
        utc_bus_data = pd.DataFrame(
            {
                "service_date": ["2024-07-15", "2024-07-15"],
                "route_id": ["01", "01"],
                "direction_id": ["Inbound", "Inbound"],
                "half_trip_id": ["12345", "12345"],
                "stop_id": ["110", "67"],
                "time_point_id": ["hhgat", "maput"],
                "time_point_order": [1, 2],
                "point_type": ["Startpoint", "Midpoint"],
                "standard_type": ["Schedule", "Schedule"],
                "scheduled": ["1900-01-01T10:05:00Z", "1900-01-01T10:08:00Z"],
                "actual": ["1900-01-01T10:06:00Z", "1900-01-01T10:09:30Z"],
                "scheduled_headway": [None, None],
                "headway": [None, None],
            }
        )

        bus_csv = pathlib.Path(self.temp_dir) / "bus_utc.csv"
        utc_bus_data.to_csv(bus_csv, index=False)

        df = process.load_bus_data(str(bus_csv))

        # Verify times are converted to Eastern (EDT in July = UTC-4)
        # 10:06:00 UTC should become 06:06:00 EDT
        expected_time_1 = datetime.datetime(2024, 7, 15, 6, 6, 0)
        expected_time_2 = datetime.datetime(2024, 7, 15, 6, 9, 30)

        self.assertEqual(df.iloc[0]["actual"], expected_time_1)
        self.assertEqual(df.iloc[1]["actual"], expected_time_2)
