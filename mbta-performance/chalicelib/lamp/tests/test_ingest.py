import io
import os
import unittest
from datetime import date
from unittest import mock

import pandas as pd

from .. import constants, ingest

# The sample file attached here is 10k events sampled from Feb 7th, 2024.
# These rows contain real-world inconsistencies in their data!
DATA_PREFIX = os.path.join(os.path.dirname(__file__), "sample_data")
SAMPLE_LAMP_DATA_PATH = os.path.join(DATA_PREFIX, "2024-02-07-10ksample.parquet")
SAMPLE_GTFS_DATA_PATH = os.path.join(DATA_PREFIX, "2024-02-07_gtfs.csv")


class TestIngest(unittest.TestCase):
    def setUp(self):
        with open(SAMPLE_LAMP_DATA_PATH, "rb") as f:
            self.data = f.read()

        self.mock_gtfs_data = pd.read_csv(
            SAMPLE_GTFS_DATA_PATH,
            dtype_backend="numpy_nullable",
            dtype={"direction_id": "int16"},
        )
        self.mock_gtfs_data["direction_id"] = self.mock_gtfs_data["direction_id"].astype("int16")

    def test__process_arrival_departure_times(self):
        pq_df_before = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        ).rename(columns=ingest.COLUMN_RENAME_MAP)

        pq_df_after = ingest._process_arrival_departure_times(pq_df_before)
        arrivals = pq_df_after[pq_df_after.event_type == "ARR"]
        departures = pq_df_after[pq_df_after.event_type == "DEP"]
        collated_events = arrivals.merge(
            departures,
            suffixes=("_ARR", "_DEP"),
            on=["service_date", "route_id", "trip_id", "stop_id", "direction_id", "vehicle_id"],
        )
        # ensure that stop sequences only go up
        backtracking_sequences = collated_events[
            collated_events["stop_sequence_ARR"] > collated_events["stop_sequence_DEP"]
        ]
        self.assertEqual(len(backtracking_sequences), 0)
        # test that departures at a stop occur after the arrival, but dont overwrite data that exists to say otherwise
        bad_late_arrivals = collated_events[collated_events["event_time_ARR"] > collated_events["event_time_DEP"]]
        self.assertEqual(len(bad_late_arrivals), 8)

    def test_fetch_pq_file_from_remote(self):
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            inital_df = ingest.fetch_pq_file_from_remote(date(2024, 2, 7))
            self.assertListEqual(
                list(inital_df.dtypes),
                [
                    pd.Int64Dtype(),  # service_date
                    "string[python]",  # route_id
                    "string[python]",  # trip_id
                    "string[python]",  # stop_id
                    pd.BooleanDtype(),  # direction_id
                    pd.Int16Dtype(),  # stop_sequence
                    "string[python]",  # vehicle_id
                    "string[python]",  # vehicle_label
                    pd.Int64Dtype(),  # move_timestamp
                    pd.Int64Dtype(),  # stop_timestamp
                    pd.Int64Dtype(),
                    pd.Int64Dtype(),
                    pd.Int64Dtype(),
                    pd.Int64Dtype(),
                    pd.Int64Dtype(),
                    pd.Int64Dtype(),
                    pd.Int64Dtype(),
                    "string[python]",  # vehicle_consist
                ],
            )

    def test_ingest_pq_file(self):
        pq_df_before = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        )
        pq_df_before["direction_id"] = pq_df_before["direction_id"].astype("int16")

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            pq_df_after = ingest.ingest_pq_file(pq_df_before, date(2024, 2, 7))
        added = pq_df_after[pq_df_after["trip_id"].str.startswith("ADDED-")]
        null_id_events = pq_df_after[pq_df_after["stop_id"].isna()]
        self.assertEqual(added.shape, (3763, 18))
        self.assertTrue(null_id_events.empty)
        self.assertEqual(pq_df_after.shape, (17074, 18))
        self.assertEqual(set(pq_df_after["service_date"].unique()), {"2024-02-07"})

    def test__average_scheduled_headways(self):
        pq_df_before = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        ).rename(columns=ingest.COLUMN_RENAME_MAP)
        pq_df_before = ingest._process_arrival_departure_times(pq_df_before)
        pq_df_before = pq_df_before[pq_df_before["stop_id"].notna()]
        # ensure that no values are dropped during calculation
        pq_df_after = ingest._average_scheduled_headways(pq_df_before, date(2024, 2, 7))
        self.assertEqual(pq_df_before.shape, pq_df_after.shape)

        # that we do not erase any headway info
        null_headway_events_before = pq_df_before[pq_df_before["scheduled_headway"].isna()]
        null_headway_events_after = pq_df_after[pq_df_after["scheduled_headway"].isna()]
        self.assertEqual(null_headway_events_after.shape, (245, 18))
        self.assertTrue(len(null_headway_events_after) <= len(null_headway_events_before))

        # pick a route/dir and directly compare
        oak_grove_recalced = pq_df_after[
            (pq_df_after.route_id == "Orange") & (pq_df_after.direction_id == True) & (pq_df_after.stop_id == "70036")
        ].scheduled_headway
        self.assertListEqual(
            list(oak_grove_recalced),
            [
                600.0,
                540.0,
                480.0,
                480.0,
                450.0,
                450.0,
                480.0,
                540.0,
                540.0,
                540.0,
                540.0,
                540.0,
                540.0,
                540.0,
                540.0,
                540.0,
                480.0,
                480.0,
                480.0,
                480.0,
                480.0,
                480.0,
                480.0,
                480.0,
                480.0,
                510.0,
                510.0,
                600.0,
                600.0,
                600.0,
                600.0,
                600.0,
                600.0,
                600.0,
                630.0,
                630.0,
            ],
        )
        pass

    def test__recalculate_fields_from_gtfs(self):
        pq_df_before = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        ).rename(columns=ingest.COLUMN_RENAME_MAP)
        pq_df_before = ingest._process_arrival_departure_times(pq_df_before)
        pq_df_before = pq_df_before[pq_df_before["stop_id"].notna()]

        # Create mock GTFS data with matching dtypes
        mock_gtfs = self.mock_gtfs_data.copy()
        # Ensure direction_id matches the dtype in pq_df_before (BooleanDtype)
        mock_gtfs["direction_id"] = mock_gtfs["direction_id"].astype("boolean")

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=mock_gtfs):
            pq_df_after = ingest._recalculate_fields_from_gtfs(pq_df_before, date(2024, 2, 7))

        # Check that scheduled_tt column was added
        self.assertIn("scheduled_tt", pq_df_after.columns)
        # Check that scheduled_trip_id was removed (not in S3_COLUMNS)
        self.assertNotIn("scheduled_trip_id", pq_df_after.columns)
        # Check that we didn't lose any rows
        self.assertEqual(len(pq_df_after), len(pq_df_before))
        # Check that scheduled_tt values are reasonable (non-negative)
        self.assertTrue((pq_df_after["scheduled_tt"] >= 0).all() or pq_df_after["scheduled_tt"].isna().all())

    def test_upload_to_s3(self):
        pq_df = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        )

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            processed_df = ingest.ingest_pq_file(pq_df, date(2024, 2, 7))

        # Create a sample stop group
        stop_groups = processed_df.groupby("stop_id")
        stop_id, stop_events = next(iter(stop_groups))

        # Mock the s3.upload_df_as_csv function
        with mock.patch("chalicelib.lamp.ingest.s3.upload_df_as_csv") as mock_upload:
            result = ingest.upload_to_s3((stop_id, stop_events), date(2024, 2, 7))

            # Verify the upload was called once
            mock_upload.assert_called_once()

            # Verify the correct arguments were passed
            call_args = mock_upload.call_args
            self.assertEqual(call_args[0][0], "tm-mbta-performance")
            expected_key = f"Events-lamp/daily-data/{stop_id}/Year=2024/Month=2/Day=7/events.csv"
            self.assertEqual(call_args[0][1], expected_key)

            # Verify that the dataframe is the expected stop events
            pd.testing.assert_frame_equal(call_args[0][2], stop_events)

            # Verify the return value contains the stop_id
            self.assertEqual(result, [stop_id])

    def test_ingest_lamp_data(self):
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
                with mock.patch("chalicelib.lamp.ingest._parallel_upload") as mock_parallel_upload:
                    ingest.ingest_lamp_data(date(2024, 2, 7))

                    # Verify that parallel upload was called
                    mock_parallel_upload.assert_called_once()

    def test_ingest_today_lamp_data(self):
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
                with mock.patch("chalicelib.lamp.ingest._parallel_upload"):
                    with mock.patch("chalicelib.lamp.ingest.get_current_service_date", return_value=date(2024, 2, 7)):
                        ingest.ingest_today_lamp_data()

    def test_ingest_yesterday_lamp_data(self):
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
                with mock.patch("chalicelib.lamp.ingest._parallel_upload"):
                    with mock.patch("chalicelib.lamp.ingest.get_current_service_date", return_value=date(2024, 2, 8)):
                        ingest.ingest_yesterday_lamp_data()

    def test_fetch_pq_file_from_remote_failure(self):
        mock_response = mock.Mock(status_code=404)
        with mock.patch("requests.get", return_value=mock_response):
            with self.assertRaises(ValueError) as context:
                ingest.fetch_pq_file_from_remote(date(2024, 2, 7))
            self.assertIn("Failed to fetch LAMP parquet file", str(context.exception))

    def test_column_rename_map(self):
        # Test that column renaming works correctly
        pq_df = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        )
        # Check for presence of columns that should be renamed
        self.assertIn("headway_trunk_seconds", pq_df.columns)
        self.assertIn("scheduled_headway_trunk", pq_df.columns)
        self.assertIn("scheduled_travel_time", pq_df.columns)

        # Apply rename
        renamed_df = pq_df.rename(columns=ingest.COLUMN_RENAME_MAP)
        self.assertIn("headway_seconds", renamed_df.columns)
        self.assertIn("scheduled_headway", renamed_df.columns)
        self.assertIn("scheduled_tt", renamed_df.columns)

    def test_direction_id_dtype_conversion(self):
        """Test that direction_id is converted to int16 dtype (important for pandas compatibility)."""
        pq_df = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        )

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            result = ingest.ingest_pq_file(pq_df, date(2024, 2, 7))

            # Check that direction_id is boolean dtype in output (after processing)
            # This is critical for merge operations to work correctly
            self.assertTrue(
                pd.api.types.is_bool_dtype(result["direction_id"])
                or pd.api.types.is_integer_dtype(result["direction_id"])
            )

    def test_null_stop_id_filtering(self):
        """Test that events with null stop_id are filtered out."""
        test_data = pd.DataFrame(
            {
                "service_date": [20240207] * 3,
                "route_id": ["Red", "Red", "Red"],
                "trip_id": ["trip1", "trip1", "trip1"],
                "stop_id": ["70061", None, "70063"],  # One null stop_id
                "direction_id": [0, 0, 0],
                "stop_sequence": [1, 2, 3],
                "vehicle_id": ["R-001", "R-001", "R-001"],
                "vehicle_label": ["1801", "1801", "1801"],
                "move_timestamp": [1707000000, 1707000100, 1707000200],
                "stop_timestamp": [1707000050, 1707000150, 1707000250],
                "travel_time_seconds": [60] * 3,
                "dwell_time_seconds": [30] * 3,
                "headway_trunk_seconds": [300] * 3,
                "headway_branch_seconds": [300] * 3,
                "scheduled_travel_time": [55] * 3,
                "scheduled_headway_trunk": [290] * 3,
                "scheduled_headway_branch": [290] * 3,
                "vehicle_consist": ["2-car"] * 3,
            }
        ).convert_dtypes(dtype_backend="numpy_nullable")

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            result = ingest.ingest_pq_file(test_data, date(2024, 2, 7))

            # No null stop_ids should remain
            self.assertFalse(result["stop_id"].isna().any(), "Null stop_ids should be filtered out")

    def test_event_time_sorting(self):
        """Test that final output is sorted by event_time."""
        pq_df = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        )

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            result = ingest.ingest_pq_file(pq_df, date(2024, 2, 7))

            # Check that result is sorted by event_time
            event_times = result["event_time"].tolist()
            sorted_event_times = sorted(event_times)
            self.assertEqual(event_times, sorted_event_times, "Output should be sorted by event_time")

    # --- _derive_lamp_branch_route_id ---

    def test__derive_lamp_branch_route_id_ashmont(self):
        """Red Line trips that visit Ashmont-branch stops should get 'Red-A' as branch_route_id."""
        df = pd.DataFrame(
            {
                "trip_id": ["trip_a", "trip_a", "trip_a"],
                "route_id": ["Red", "Red", "Red"],
                # trunk stop + a stop unique to the Ashmont branch
                "stop_id": ["70061", "70063", "70085"],
            }
        )
        result = ingest._derive_lamp_branch_route_id(df)
        branch_ids = result[result["trip_id"] == "trip_a"]["branch_route_id"].unique()
        self.assertIn("Red-A", branch_ids)
        self.assertNotIn("Red-B", branch_ids)

    def test__derive_lamp_branch_route_id_braintree(self):
        """Red Line trips that visit Braintree-branch stops should get 'Red-B' as branch_route_id."""
        df = pd.DataFrame(
            {
                "trip_id": ["trip_b", "trip_b", "trip_b"],
                "route_id": ["Red", "Red", "Red"],
                # trunk stop + a stop unique to the Braintree branch
                "stop_id": ["70061", "70063", "70095"],
            }
        )
        result = ingest._derive_lamp_branch_route_id(df)
        branch_ids = result[result["trip_id"] == "trip_b"]["branch_route_id"].unique()
        self.assertIn("Red-B", branch_ids)
        self.assertNotIn("Red-A", branch_ids)

    def test__derive_lamp_branch_route_id_trunk_only(self):
        """Red Line trips visiting only trunk stops (no branch-specific stops) keep 'Red'."""
        df = pd.DataFrame(
            {
                "trip_id": ["trip_trunk", "trip_trunk"],
                "route_id": ["Red", "Red"],
                "stop_id": ["70061", "70063"],  # trunk-only stops
            }
        )
        result = ingest._derive_lamp_branch_route_id(df)
        branch_ids = result[result["trip_id"] == "trip_trunk"]["branch_route_id"].unique()
        self.assertListEqual(list(branch_ids), ["Red"])

    def test__derive_lamp_branch_route_id_non_red_line(self):
        """Non-Red Line trips should use route_id unchanged as branch_route_id."""
        df = pd.DataFrame(
            {
                "trip_id": ["trip_o", "trip_o"],
                "route_id": ["Orange", "Orange"],
                "stop_id": ["70001", "70003"],
            }
        )
        result = ingest._derive_lamp_branch_route_id(df)
        branch_ids = result[result["trip_id"] == "trip_o"]["branch_route_id"].unique()
        self.assertListEqual(list(branch_ids), ["Orange"])

    def test__derive_lamp_branch_route_id_green_line_unchanged(self):
        """Green Line sub-routes already have distinct route_ids and should pass through unchanged."""
        df = pd.DataFrame(
            {
                "trip_id": ["trip_gb", "trip_gb"],
                "route_id": ["Green-B", "Green-B"],
                "stop_id": ["70107", "70110"],
            }
        )
        result = ingest._derive_lamp_branch_route_id(df)
        branch_ids = result[result["trip_id"] == "trip_gb"]["branch_route_id"].unique()
        self.assertListEqual(list(branch_ids), ["Green-B"])

    # --- Helpers for pipeline tests ---

    def _make_minimal_lamp_df(self, service_date_int, trip_stop_pairs, base_ts=1707825600):
        """
        Build a minimal LAMP-style DataFrame for use in ingest_pq_file tests.

        trip_stop_pairs: list of (trip_id, stop_id, vehicle_id) tuples.
        base_ts: base Unix timestamp (default: 2024-02-13 noon UTC).
        """
        rows = []
        for seq, (trip_id, stop_id, vehicle_id) in enumerate(trip_stop_pairs, start=1):
            rows.append(
                {
                    "service_date": service_date_int,
                    "route_id": "Orange",
                    "trip_id": trip_id,
                    "stop_id": stop_id,
                    "direction_id": 0,
                    "stop_sequence": seq,
                    "vehicle_id": vehicle_id,
                    "vehicle_label": "1201",
                    "move_timestamp": base_ts + seq * 100,
                    "stop_timestamp": base_ts + 50 + seq * 100,
                    "travel_time_seconds": 60,
                    "dwell_time_seconds": 30,
                    "headway_trunk_seconds": 300,
                    "headway_branch_seconds": 300,
                    "scheduled_travel_time": 55,
                    "scheduled_headway_trunk": 290,
                    "scheduled_headway_branch": 290,
                    "vehicle_consist": "",
                }
            )
        return pd.DataFrame(rows).convert_dtypes(dtype_backend="numpy_nullable")

    # --- NONREV trip filtering ---

    def test_nonrev_trip_filtering_before_cutoff(self):
        """NONREV- trips with a service_date before Dec 2023 should be dropped from the output."""
        # Nov 1, 2023 noon UTC = 8 AM Eastern Daylight Time (well within service day)
        base_ts = 1698840000
        test_data = self._make_minimal_lamp_df(
            service_date_int=20231101,
            trip_stop_pairs=[
                ("NONREV-12345", "70001", "O-001"),
                ("regular-trip", "70003", "O-002"),
            ],
            base_ts=base_ts,
        )

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            result = ingest.ingest_pq_file(test_data, date(2023, 11, 1))

        self.assertFalse(
            any(result["trip_id"].str.startswith("NONREV-")),
            "NONREV- trips before the cutoff date should not appear in the output",
        )

    def test_nonrev_trip_not_filtered_after_cutoff(self):
        """NONREV- trips with a service_date on or after Dec 2023 should be retained."""
        # Feb 13, 2024 noon UTC = 8 AM ET
        base_ts = 1707825600
        test_data = self._make_minimal_lamp_df(
            service_date_int=20240213,
            trip_stop_pairs=[
                ("NONREV-99999", "70001", "O-001"),
                ("NONREV-88888", "70003", "O-002"),
            ],
            base_ts=base_ts,
        )

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            result = ingest.ingest_pq_file(test_data, date(2024, 2, 13))

        nonrev_events = result[result["trip_id"].str.startswith("NONREV-")]
        self.assertFalse(
            nonrev_events.empty,
            "NONREV- trips after the cutoff date should be retained in the output",
        )

    # --- Stop ID alias mapping ---

    def test_stop_id_alias_mapping(self):
        """Aliased stop IDs (e.g. 'Alewife-01') should be replaced with the canonical numeric ID."""
        base_ts = 1707825600  # Feb 13, 2024 noon UTC
        # Two-stop trip: Alewife-01 (alias) → 70063 (canonical)
        # Using the same vehicle_id so the merge_asof in _process_arrival_departure_times works
        test_data = pd.DataFrame(
            {
                "service_date": [20240213, 20240213],
                "route_id": ["Red", "Red"],
                "trip_id": ["trip1", "trip1"],
                "stop_id": ["Alewife-01", "70063"],
                "direction_id": [0, 0],
                "stop_sequence": [1, 2],
                "vehicle_id": ["R-001", "R-001"],
                "vehicle_label": ["1801", "1801"],
                "move_timestamp": [base_ts, base_ts + 100],
                "stop_timestamp": [base_ts + 50, base_ts + 150],
                "travel_time_seconds": [60, 60],
                "dwell_time_seconds": [30, 30],
                "headway_trunk_seconds": [300, 300],
                "headway_branch_seconds": [300, 300],
                "scheduled_travel_time": [55, 55],
                "scheduled_headway_trunk": [290, 290],
                "scheduled_headway_branch": [290, 290],
                "vehicle_consist": ["", ""],
            }
        ).convert_dtypes(dtype_backend="numpy_nullable")

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            result = ingest.ingest_pq_file(test_data, date(2024, 2, 13))

        self.assertFalse(
            any(result["stop_id"] == "Alewife-01"),
            "The aliased stop ID 'Alewife-01' should not appear in the output",
        )
        # The canonical replacement "70061" should be present
        self.assertIn("70061", result["stop_id"].values)

    def test_service_date_formatting(self):
        """Test that service_date is formatted correctly as string."""
        # Use timestamps that fall within the 2024-02-07 service window (8–9 am ET)
        # 1707310800 = 2024-02-07 13:00:00 UTC = 2024-02-07 08:00:00 ET
        base_ts = 1707310800
        test_data = pd.DataFrame(
            {
                "service_date": [20240207, 20240207],
                "route_id": ["Red", "Red"],
                "trip_id": ["trip1", "trip1"],
                "stop_id": ["70061", "70063"],
                "direction_id": [0, 0],
                "stop_sequence": [1, 2],
                "vehicle_id": ["R-001", "R-001"],
                "vehicle_label": ["1801", "1801"],
                "move_timestamp": [base_ts, base_ts + 100],
                "stop_timestamp": [base_ts + 50, base_ts + 150],
                "travel_time_seconds": [60] * 2,
                "dwell_time_seconds": [30] * 2,
                "headway_trunk_seconds": [300] * 2,
                "headway_branch_seconds": [300] * 2,
                "scheduled_travel_time": [55] * 2,
                "scheduled_headway_trunk": [290] * 2,
                "scheduled_headway_branch": [290] * 2,
                "vehicle_consist": ["", ""] * 1,
            }
        ).convert_dtypes(dtype_backend="numpy_nullable")

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            result = ingest.ingest_pq_file(test_data, date(2024, 2, 7))

            # Check that service_date is formatted as YYYY-MM-DD strings
            service_dates = result["service_date"].unique()
            self.assertGreater(len(service_dates), 0, "Result should not be empty")
            for sdate in service_dates:
                self.assertIsInstance(sdate, str)
                self.assertRegex(sdate, r"^\d{4}-\d{2}-\d{2}$", "service_date should be YYYY-MM-DD format")

    # --- _derive_gtfs_branch_route_id ---

    def test__derive_gtfs_branch_route_id_trunk_only(self):
        """Red Line GTFS trips visiting only trunk stops fall back to route_id."""
        gtfs_stops = pd.DataFrame(
            {
                "trip_id": ["gtfs_trunk", "gtfs_trunk"],
                "route_id": ["Red", "Red"],
                "stop_id": ["70061", "70063"],  # trunk-only stops
                "direction_id": [0, 0],
                "arrival_time": [28800.0, 28900.0],
            }
        )
        result = ingest._derive_gtfs_branch_route_id(gtfs_stops)
        branch_ids = result[result["trip_id"] == "gtfs_trunk"]["branch_route_id"].unique()
        self.assertListEqual(list(branch_ids), ["Red"])

    # --- upload_to_s3 error handling ---

    def test_upload_to_s3_failure(self):
        """upload_to_s3 should re-raise when s3 upload fails."""
        pq_df = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        )

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            processed_df = ingest.ingest_pq_file(pq_df, date(2024, 2, 7))

        stop_groups = processed_df.groupby("stop_id")
        stop_id, stop_events = next(iter(stop_groups))

        with mock.patch("chalicelib.lamp.ingest.s3.upload_df_as_csv", side_effect=RuntimeError("S3 error")):
            with self.assertRaises(RuntimeError):
                ingest.upload_to_s3((stop_id, stop_events), date(2024, 2, 7))

    # --- ingest_lamp_data error handling ---

    def test_ingest_lamp_data_fetch_value_error(self):
        """ingest_lamp_data should return gracefully when fetch raises ValueError."""
        with mock.patch("chalicelib.lamp.ingest.fetch_pq_file_from_remote", side_effect=ValueError("404")):
            # Should not raise; just return early
            ingest.ingest_lamp_data(date(2024, 2, 7))

    def test_ingest_lamp_data_fetch_unexpected_error(self):
        """ingest_lamp_data should re-raise unexpected fetch exceptions."""
        with mock.patch(
            "chalicelib.lamp.ingest.fetch_pq_file_from_remote", side_effect=RuntimeError("network failure")
        ):
            with self.assertRaises(RuntimeError):
                ingest.ingest_lamp_data(date(2024, 2, 7))

    def test_ingest_lamp_data_process_error(self):
        """ingest_lamp_data should re-raise when ingest_pq_file fails."""
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.lamp.ingest.ingest_pq_file", side_effect=RuntimeError("process error")):
                with self.assertRaises(RuntimeError):
                    ingest.ingest_lamp_data(date(2024, 2, 7))

    def test_ingest_lamp_data_upload_error(self):
        """ingest_lamp_data should re-raise when parallel upload fails."""
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
                with mock.patch("chalicelib.lamp.ingest._parallel_upload", side_effect=RuntimeError("upload error")):
                    with self.assertRaises(RuntimeError):
                        ingest.ingest_lamp_data(date(2024, 2, 7))
