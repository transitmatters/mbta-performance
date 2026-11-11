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

    def test_whole_months(self):
        """Date ranges widen to whole calendar months, since each output file holds one month."""
        d = datetime.date
        self.assertEqual(process.whole_months(d(2026, 8, 15), d(2026, 8, 20)), (d(2026, 8, 1), d(2026, 8, 31)))
        self.assertEqual(process.whole_months(d(2024, 1, 31), d(2024, 2, 10)), (d(2024, 1, 1), d(2024, 2, 29)))
        self.assertEqual(process.whole_months(d(2025, 11, 1), d(2025, 12, 31)), (d(2025, 11, 1), d(2025, 12, 31)))
        self.assertEqual(process.whole_months(d(2023, 2, 3), None), (d(2023, 2, 1), None))
        self.assertEqual(process.whole_months(None, d(2023, 2, 3)), (None, d(2023, 2, 28)))
        self.assertEqual(process.whole_months(None, None), (None, None))

    def test_process_events_mid_month_range_writes_whole_month(self):
        """A mid-month range still writes the whole month, so uploading it can't replace S3's month with part of one."""
        service_dates = ["2026-07-31", "2026-08-03", "2026-08-10", "2026-08-17", "2026-08-24", "2026-09-01"]
        events = pd.DataFrame(
            {
                "service_date": service_dates,
                "route_id": "Red",
                "trip_id": [f"trip{i}" for i in range(len(service_dates))],
                "direction_id": 0,
                "stop_id": "70061",
                "stop_sequence": 1,
                "vehicle_id": "R-001",
                "vehicle_label": "1801",
                "event_type": "DEP",
                "event_time_sec": 28800,
            }
        )
        events.to_csv(self.test_csv, index=False)

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=lambda x: x):
            process.process_events(
                str(self.test_csv),
                self.temp_dir,
                nozip=True,
                start_date=datetime.date(2026, 8, 15),
                end_date=datetime.date(2026, 8, 20),
            )

        stop_dir = pathlib.Path(self.temp_dir, "Events", "monthly-data", "70061", "Year=2026")
        self.assertEqual(sorted(p.name for p in stop_dir.iterdir()), ["Month=8"])
        written = pd.read_csv(stop_dir / "Month=8" / "events.csv.gz", compression=None)
        self.assertEqual(
            sorted(written["service_date"].str[:10]), ["2026-08-03", "2026-08-10", "2026-08-17", "2026-08-24"]
        )

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

    def _process_ferry_rows(self, rows):
        n = len(rows["route_id"])
        ferry_data = pd.DataFrame(
            {
                "service_date": ["2025-07-15 00:00:00+00:00"] * n,
                "trip_id": [f"trip{i}" for i in range(n)],
                "mbta_sched_arrival": ["2025-07-15 12:30:00+00:00"] * n,
                "mbta_sched_departure": ["2025-07-15 12:00:00+00:00"] * n,
                "actual_arrival": ["2025-07-15 08:30:00"] * n,
                "actual_departure": ["2025-07-15 08:00:00"] * n,
                "vessel_time_slot": ["slot1"] * n,
                **rows,
            }
        )
        ferry_csv = pathlib.Path(self.temp_dir) / "ferry.csv"
        ferry_data.to_csv(ferry_csv, index=False)

        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=lambda x: x):
            with mock.patch("chalicelib.historic.process.to_disk_ferry") as mock_to_disk:
                process.process_ferry(str(ferry_csv), self.temp_dir, nozip=True)
                df = mock_to_disk.call_args[0][0]
        return df[df["event_type"] == "DEP"].set_index("trip_id")

    def test_process_ferry_infers_missing_direction(self):
        """Trips the MBTA labels "NA" get a direction from their departure terminal instead of being dropped."""
        deps = self._process_ferry_rows(
            {
                "route_id": ["F4", "F4"],
                "travel_direction": ["NA", "NA"],
                "departure_terminal": ["Central Whf", "Navy Yard"],
                "arrival_terminal": ["Navy Yard", "Central Whf"],
            }
        )

        self.assertEqual(deps.loc["trip0", "direction_id"], 0)
        self.assertEqual(deps.loc["trip0", "stop_id"], "Boat-Aquarium")
        self.assertEqual(deps.loc["trip1", "direction_id"], 1)
        self.assertEqual(deps.loc["trip1", "stop_id"], "Boat-Charlestown")

    def test_process_ferry_loop_routes_use_gtfs_direction(self):
        """Loop routes always use direction 1, whatever the MBTA labels them, to match live data."""
        deps = self._process_ferry_rows(
            {
                "route_id": ["F6", "F7", "F9"],
                "travel_direction": ["From Boston", "NA", "From Boston"],
                "departure_terminal": ["Aquarium", "Quincy", "Winthrop"],
                "arrival_terminal": ["Seaport", "Logan", "Quincy"],
            }
        )

        self.assertEqual(deps["direction_id"].tolist(), [1, 1, 1])
        self.assertEqual(deps["route_id"].tolist(), ["Boat-F6", "Boat-F7", "Boat-F8"])

    def test_process_ferry_keeps_f2h_separate_from_f1(self):
        deps = self._process_ferry_rows(
            {
                "route_id": ["F1", "F2H"],
                "travel_direction": ["To Boston", "To Boston"],
                "departure_terminal": ["Hingham", "Hull"],
                "arrival_terminal": ["Rowes", "Long Wharf N"],
            }
        )

        self.assertEqual(deps["route_id"].tolist(), ["Boat-F1", "Boat-F2H"])

    def test_process_ferry_with_date_filtering(self):
        """process_ferry keeps the whole months covering the date range, and drops other months."""
        days = ["2024-01-20", "2024-02-05", "2024-02-07", "2024-02-10", "2024-03-10"]
        ferry_data = pd.DataFrame(
            {
                "service_date": [f"{day} 12:00:00+00:00" for day in days],
                "route_id": "F1",
                "trip_id": [f"trip{i}" for i in range(len(days))],
                "travel_direction": "To Boston",
                "departure_terminal": "Hingham",
                "arrival_terminal": "Boston",
                "mbta_sched_arrival": [f"{day} 13:00:00+00:00" for day in days],
                "mbta_sched_departure": [f"{day} 12:45:00+00:00" for day in days],
                "actual_arrival": [f"{day} 08:02:00" for day in days],
                "actual_departure": [f"{day} 07:46:00" for day in days],
                "vessel_time_slot": "slot1",
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

                # All of February, not just Feb 6-8: each trip generates an ARR and a DEP event
                self.assertEqual(sorted(df["trip_id"].unique()), ["trip1", "trip2", "trip3"])
                self.assertEqual(len(df), 6)

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

    def _ferry_rows_missing_trip_ids(self):
        return pd.DataFrame(
            {
                "service_date": ["2024-02-07 05:00:00+00"] * 5,
                "route_id": ["F4"] * 5,
                "trip_id": [None] * 5,
                "travel_direction": ["To Boston"] + ["From Boston"] * 4,
                "departure_terminal": ["Navy Yard"] + ["Long Wharf N"] * 4,
                "arrival_terminal": ["Long Wharf N", "Navy Yard", "Navy Yard", "Navy Yard", None],
                "mbta_sched_departure": ["2024-02-07 12:00:00+00", "2024-02-07 12:15:00+00"]
                + ["2024-02-07 05:00:00+00"] * 3,
                "mbta_sched_arrival": ["2024-02-07 12:10:00+00", "2024-02-07 12:25:00+00"]
                + ["2024-02-07 05:00:00+00"] * 3,
                # The last three are midnight placeholders like the source has: identical but for the vessel,
                # and some without an arrival terminal
                "actual_departure": ["2024-02-07 07:01:00", "2024-02-07 07:16:00"] + ["2024-02-07 00:00"] * 3,
                "actual_arrival": ["2024-02-07 07:11:00", "2024-02-07 07:26:00"] + ["2024-02-07 00:00"] * 3,
                "vessel_time_slot": ["Vessel A", "Vessel A", "Vessel B", "Vessel C", "Vessel D"],
            }
        )

    def _ferry_trip_ids_by_departure(self, ferry_data):
        ferry_csv = pathlib.Path(self.temp_dir) / "ferry.csv"
        ferry_data.to_csv(ferry_csv, index=False)
        with mock.patch("chalicelib.historic.process.add_gtfs_headways", side_effect=lambda x: x):
            with mock.patch("chalicelib.historic.process.to_disk_ferry") as mock_to_disk:
                process.process_ferry(str(ferry_csv), self.temp_dir, nozip=True)
        df = mock_to_disk.call_args[0][0]
        deps = df[df["event_type"] == "DEP"]
        return dict(zip(zip(deps["event_time"], deps["vehicle_id"]), deps["trip_id"]))

    def test_process_ferry_missing_trip_ids_are_stable(self):
        """Trips without a trip_id get the same unique id on every run, whatever order the file lists them in."""
        ferry_data = self._ferry_rows_missing_trip_ids()

        first = self._ferry_trip_ids_by_departure(ferry_data)
        rerun = self._ferry_trip_ids_by_departure(ferry_data)
        reordered = self._ferry_trip_ids_by_departure(ferry_data.iloc[::-1])

        self.assertEqual(len(set(first.values())), 5)
        self.assertEqual(first, rerun)
        self.assertEqual(first, reordered)

    def test_process_ferry_actual_time_in_dst_changeover(self):
        """An actual time in the repeated hour when clocks fall back is kept as written, instead of crashing."""
        deps = self._process_ferry_rows(
            {
                "route_id": ["F1"],
                "travel_direction": ["From Boston"],
                "departure_terminal": ["Rowes"],
                "arrival_terminal": ["Hingham"],
                "service_date": ["2024-11-03 04:00:00+00:00"],
                "actual_departure": ["2024-11-03 01:30:00"],
            }
        )

        self.assertEqual(deps["event_time"].tolist(), [pd.Timestamp("2024-11-03 01:30:00")])

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

    def test_load_bus_data_pascal_case_headers(self):
        """The 2020 Q4 file's PascalCase headers get the same string IDs as the others, not floats like 67.0."""
        bus_csv = pathlib.Path(self.temp_dir) / "bus.csv"
        bus_csv.write_text(
            "ServiceDate,Route,Direction,HalfTripId,Stop,Timepoint,TimepointOrder,PointType,StandardType,"
            "Scheduled,Actual,ScheduledHeadway,Headway\n"
            "2020-10-01 00:00:00.000,01,Inbound,48885644.0,067,maput,2,Midpoint,Schedule,"
            "1900-01-01 05:07:00.000,1900-01-01 05:07:42.000,NULL,NULL\n"
            "2020-10-01 00:00:00.000,01,Inbound,48885644.0,,maput,3,Midpoint,Schedule,"
            "1900-01-01 05:09:00.000,1900-01-01 05:09:42.000,NULL,NULL\n"
        )

        result = process.load_bus_data(str(bus_csv))

        self.assertEqual(result["stop_id"].iloc[0], "067")
        self.assertListEqual(list(result["route_id"]), ["1", "1"])
        self.assertListEqual(list(result["direction_id"]), [1, 1])

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

        Before June 2024 the MBTA labelled Eastern times with a Z suffix. One row is too few
        for detection, so the loader falls back to BUS_SHIFTED_WINDOW, which starts at
        2024-06-01, and takes no hours off: the output equals the raw wall-clock value.
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
        # 08:06 stays as 08:06 on the service date (no 5-hour shift).
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

    def test_load_bus_data_shifted_times(self):
        """June to December 2024 bus data is Eastern + 5 hours all year, so 5 hours come off even under DST.

        Read as UTC, 10:06Z would be 06:06 EDT, an hour later than the bus ran (checked against gobble's
        recordings at route 1's stop 67 in June, August and October 2024).
        """
        shifted_bus_data = pd.DataFrame(
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

        bus_csv = pathlib.Path(self.temp_dir) / "bus_shifted.csv"
        shifted_bus_data.to_csv(bus_csv, index=False)

        df = process.load_bus_data(str(bus_csv))

        self.assertEqual(df.iloc[0]["actual"], datetime.datetime(2024, 7, 15, 5, 6, 0))
        self.assertEqual(df.iloc[1]["actual"], datetime.datetime(2024, 7, 15, 5, 9, 30))
        self.assertEqual(df.iloc[0]["scheduled"], datetime.datetime(2024, 7, 15, 5, 5, 0))

        # In winter Eastern + 5 hours is UTC, so the two readings agree
        shifted_bus_data["service_date"] = "2024-12-10"
        shifted_bus_data.to_csv(bus_csv, index=False)
        df = process.load_bus_data(str(bus_csv))
        self.assertEqual(df.iloc[0]["actual"], datetime.datetime(2024, 12, 10, 5, 6, 0))

    @staticmethod
    def _service_day_times(utc_offset_hours: int, n: int = 12_000) -> pd.Series:
        """A day of bus times spread over 5am-1am Eastern, expressed in a clock shifted by utc_offset_hours."""
        eastern_minutes = [(5 * 60 + i * (20 * 60) // n) for i in range(n)]
        return pd.to_datetime(
            [f"1900-01-01T{((m // 60) + utc_offset_hours) % 24:02d}:{m % 60:02d}:00Z" for m in eastern_minutes],
            utc=True,
        )

    def test_bus_times_are_shifted_detects_shift_from_data(self):
        """Shifted times (Eastern + 5h) have their overnight lull at 7-9, regardless of service date."""
        times = pd.Series(self._service_day_times(utc_offset_hours=5))
        self.assertTrue(process.bus_times_are_shifted(times, datetime.datetime(2026, 3, 1)))

    def test_bus_times_are_shifted_detects_eastern_labelled_z(self):
        """Eastern times with a Z suffix have their lull at 2-4, so they're not shifted."""
        times = pd.Series(self._service_day_times(utc_offset_hours=0))
        self.assertFalse(process.bus_times_are_shifted(times, datetime.datetime(2024, 7, 1)))

    def test_bus_times_are_shifted_small_input_falls_back_to_known_window(self):
        times = pd.Series(pd.to_datetime(["1900-01-01T10:06:00Z"], utc=True))
        self.assertFalse(process.bus_times_are_shifted(times, datetime.datetime(2024, 5, 31)))
        self.assertTrue(process.bus_times_are_shifted(times, datetime.datetime(2024, 6, 1)))
        self.assertTrue(process.bus_times_are_shifted(times, datetime.datetime(2024, 12, 31)))
        self.assertFalse(process.bus_times_are_shifted(times, datetime.datetime(2025, 1, 1)))
        self.assertFalse(process.bus_times_are_shifted(times, datetime.datetime(2026, 1, 31)))

    def test_load_bus_data_2026_z_suffix_treated_as_eastern(self):
        """Bus data from 2025 on is Eastern wall-clock time again despite the Z suffix: no hours come off."""
        bus_data = pd.DataFrame(
            {
                "service_date": ["2026-03-01"],
                "route_id": ["01"],
                "direction": ["Outbound"],
                "half_trip_id": ["70049836"],
                "stop_id": ["110"],
                "time_point_id": ["hhgat"],
                "time_point_order": [9],
                "point_type": ["Endpoint"],
                "standard_type": ["Schedule"],
                "scheduled": ["1900-01-01T06:21:00Z"],
                "actual": ["1900-01-01T06:21:12Z"],
                "scheduled_headway": [None],
                "headway": [None],
            }
        )
        bus_csv = pathlib.Path(self.temp_dir) / "bus_2026_03.csv"
        bus_data.to_csv(bus_csv, index=False)

        result = process.load_bus_data(str(bus_csv))

        self.assertEqual(result.iloc[0]["actual"], datetime.datetime(2026, 3, 1, 6, 21, 12))
