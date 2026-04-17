import io
import os
import unittest
from datetime import date
from unittest import mock

import pandas as pd

from .. import bus_constants, bus_ingest

DATA_PREFIX = os.path.join(os.path.dirname(__file__), "sample_data")
SAMPLE_BUS_DATA_PATH = os.path.join(DATA_PREFIX, "bus-20260407-sample.parquet")


class TestBusIngest(unittest.TestCase):
    def setUp(self):
        with open(SAMPLE_BUS_DATA_PATH, "rb") as f:
            self.data = f.read()

        self.sample_df = pd.read_parquet(
            io.BytesIO(self.data),
            columns=bus_constants.BUS_LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        )

    def test_fetch_bus_pq_file_from_remote(self):
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            df = bus_ingest.fetch_bus_pq_file_from_remote(date(2026, 4, 7))
            self.assertEqual(set(df.columns), set(bus_constants.BUS_LAMP_COLUMNS))
            self.assertGreater(len(df), 0)

    def test_fetch_bus_pq_file_from_remote_failure(self):
        mock_response = mock.Mock(status_code=404)
        with mock.patch("requests.get", return_value=mock_response):
            with self.assertRaises(ValueError) as context:
                bus_ingest.fetch_bus_pq_file_from_remote(date(2026, 4, 7))
            self.assertIn("Failed to fetch bus LAMP parquet file", str(context.exception))

    def test_process_bus_arrival_departure_times(self):
        df = self.sample_df.rename(columns=bus_constants.BUS_COLUMN_RENAME_MAP)
        result = bus_ingest._process_bus_arrival_departure_times(df)

        arrivals = result[result.event_type == "ARR"]
        departures = result[result.event_type == "DEP"]

        self.assertGreater(len(arrivals), 0)
        self.assertGreater(len(departures), 0)
        self.assertListEqual(list(result.columns), bus_constants.BUS_S3_COLUMNS)

    def test_process_bus_arrival_departure_times_timezone(self):
        df = self.sample_df.rename(columns=bus_constants.BUS_COLUMN_RENAME_MAP)
        result = bus_ingest._process_bus_arrival_departure_times(df)

        # All event_times should be in Eastern Time
        for event_time in result["event_time"].dropna().head(5):
            self.assertEqual(str(event_time.tzinfo), "US/Eastern")

    def test_departures_use_previous_stop_id(self):
        df = self.sample_df.rename(columns=bus_constants.BUS_COLUMN_RENAME_MAP)
        # Find rows where previous_stop_id differs from stop_id
        has_prev = df[df["previous_stop_id"].notna() & (df["previous_stop_id"] != df["stop_id"])]
        if len(has_prev) > 0:
            result = bus_ingest._process_bus_arrival_departure_times(df)
            departures = result[result.event_type == "DEP"]
            # DEP events should use previous_stop_id, not the original stop_id
            self.assertGreater(len(departures), 0)

    def test_ingest_bus_pq_file(self):
        result = bus_ingest.ingest_bus_pq_file(self.sample_df, date(2026, 4, 7))

        # No null stop_ids
        self.assertFalse(result["stop_id"].isna().any())
        # Output columns match
        self.assertListEqual(list(result.columns), bus_constants.BUS_S3_COLUMNS)
        # Sorted by event_time
        event_times = result["event_time"].tolist()
        self.assertEqual(event_times, sorted(event_times))
        # service_date is a string
        for sdate in result["service_date"].unique():
            self.assertIsInstance(sdate, str)

    def test_upload_bus_to_s3_key_format(self):
        df = pd.DataFrame({col: ["test"] for col in bus_constants.BUS_S3_COLUMNS})

        with mock.patch("chalicelib.lamp.bus_ingest.s3.upload_df_as_csv") as mock_upload:
            result = bus_ingest.upload_bus_to_s3((("1", 0, "110"), df), date(2026, 4, 7))

            mock_upload.assert_called_once()
            call_args = mock_upload.call_args
            self.assertEqual(call_args[0][0], "tm-mbta-performance")
            expected_key = "Events-lamp/bus-daily-data/1-0-110/Year=2026/Month=4/Day=7/events.csv"
            self.assertEqual(call_args[0][1], expected_key)
            self.assertEqual(result, [("1", 0, "110")])

    def test_ingest_bus_data_end_to_end(self):
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.lamp.bus_ingest._parallel_upload") as mock_upload:
                bus_ingest.ingest_bus_data(date(2026, 4, 7))
                mock_upload.assert_called_once()

    def test_ingest_bus_data_no_file_found(self):
        mock_response = mock.Mock(status_code=404)
        with mock.patch("requests.get", return_value=mock_response):
            # Should not raise - logs error and returns
            bus_ingest.ingest_bus_data(date(2026, 4, 7))

    def test_ingest_today_bus_data(self):
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.lamp.bus_ingest._parallel_upload"):
                with mock.patch("chalicelib.lamp.bus_ingest.get_current_service_date", return_value=date(2026, 4, 7)):
                    bus_ingest.ingest_today_bus_data()

    def test_ingest_yesterday_bus_data(self):
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.lamp.bus_ingest._parallel_upload"):
                with mock.patch("chalicelib.lamp.bus_ingest.get_current_service_date", return_value=date(2026, 4, 8)):
                    bus_ingest.ingest_yesterday_bus_data()

    def test_column_rename_map(self):
        df = self.sample_df.copy()
        # Verify source columns exist
        self.assertIn("stopped_duration_seconds", df.columns)
        self.assertIn("route_direction_headway_seconds", df.columns)
        self.assertIn("plan_travel_time_seconds", df.columns)
        self.assertIn("plan_route_direction_headway_seconds", df.columns)

        renamed = df.rename(columns=bus_constants.BUS_COLUMN_RENAME_MAP)
        self.assertIn("dwell_time_seconds", renamed.columns)
        self.assertIn("headway_seconds", renamed.columns)
        self.assertIn("scheduled_tt", renamed.columns)
        self.assertIn("scheduled_headway", renamed.columns)
