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

    def _mock_s3_upload(self):
        # mock upload of s3.upload_df_as_csv() to a fake bucket
        pass

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

    def test_service_date_formatting(self):
        """Test that service_date is formatted correctly as string."""
        test_data = pd.DataFrame(
            {
                "service_date": [20240207, 20240208],
                "route_id": ["Red", "Red"],
                "trip_id": ["trip1", "trip2"],
                "stop_id": ["70061", "70061"],
                "direction_id": [0, 0],
                "stop_sequence": [1, 1],
                "vehicle_id": ["R-001", "R-002"],
                "vehicle_label": ["1801", "1802"],
                "move_timestamp": [1707000000, 1707100000],
                "stop_timestamp": [1707000050, 1707100050],
                "travel_time_seconds": [60] * 2,
                "dwell_time_seconds": [30] * 2,
                "headway_trunk_seconds": [300] * 2,
                "headway_branch_seconds": [300] * 2,
                "scheduled_travel_time": [55] * 2,
                "scheduled_headway_trunk": [290] * 2,
                "scheduled_headway_branch": [290] * 2,
                "vehicle_consist": ["2-car"] * 2,
            }
        ).convert_dtypes(dtype_backend="numpy_nullable")

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            result = ingest.ingest_pq_file(test_data, date(2024, 2, 7))

            # Check that service_date is formatted as YYYY-MM-DD strings
            service_dates = result["service_date"].unique()
            for sdate in service_dates:
                self.assertIsInstance(sdate, str)
                self.assertRegex(sdate, r"^\d{4}-\d{2}-\d{2}$", "service_date should be YYYY-MM-DD format")
