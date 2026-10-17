import io
import os
import unittest
from datetime import date
from unittest import mock

import pandas as pd

from .. import bus_constants, bus_ingest

DATA_PREFIX = os.path.join(os.path.dirname(__file__), "sample_data")
SAMPLE_BUS_DATA_PATH = os.path.join(DATA_PREFIX, "bus-20260407-sample.parquet")


def _empty_gtfs_mock() -> pd.DataFrame:
    """Empty GTFS stop_times mock matching fetch_stop_times_from_gtfs's schema."""
    return pd.DataFrame(
        {
            "trip_id": pd.array([], dtype="string"),
            "stop_id": pd.array([], dtype="string"),
            "arrival_time": pd.array([], dtype="Int64"),
            "stop_sequence": pd.array([], dtype="Int64"),
            "checkpoint_id": pd.array([], dtype="string"),
            "route_id": pd.array([], dtype="string"),
            "direction_id": pd.array([], dtype="int16"),
        }
    )


def _checkpoint_gtfs_mock(df: pd.DataFrame) -> pd.DataFrame:
    """GTFS stop_times mock marking every route/direction/stop in df as a checkpoint."""
    stops = df[["route_id", "direction_id", "stop_id"]].dropna().drop_duplicates().reset_index(drop=True)
    return pd.DataFrame(
        {
            "trip_id": pd.array(["sched-1"] * len(stops), dtype="string"),
            "stop_id": stops["stop_id"].astype("string"),
            "arrival_time": pd.array(range(len(stops)), dtype="Int64"),
            "stop_sequence": pd.array(range(len(stops)), dtype="Int64"),
            "checkpoint_id": pd.array([f"cp{i}" for i in range(len(stops))], dtype="string"),
            "route_id": stops["route_id"].astype("string"),
            "direction_id": stops["direction_id"].astype("int16"),
        }
    )


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
        self.mock_gtfs_data = _empty_gtfs_mock()

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
        self.assertListEqual(list(result.columns), bus_constants.BUS_S3_COLUMNS + ["visit_rank"])

    def test_process_bus_arrival_departure_times_timezone(self):
        df = self.sample_df.rename(columns=bus_constants.BUS_COLUMN_RENAME_MAP)
        result = bus_ingest._process_bus_arrival_departure_times(df)

        # All event_times should be in Eastern Time
        for event_time in result["event_time"].dropna().head(5):
            self.assertEqual(str(event_time.tzinfo), "US/Eastern")

    def test_departures_use_previous_stop_id(self):
        """DEP rows must be keyed by previous_stop_id, not the row's own stop_id."""
        df = pd.DataFrame(
            {
                "service_date": ["20260407", "20260407"],
                "route_id": ["1", "1"],
                "trip_id": ["trip-1", "trip-1"],
                "stop_id": ["stop-A", "stop-B"],
                "direction_id": [0, 0],
                "stop_sequence": [1, 2],
                "vehicle_label": ["y0001", "y0001"],
                "previous_stop_id": [None, "stop-A"],
                "stop_arrival_dt": pd.to_datetime(["2026-04-07T11:55:00Z", "2026-04-07T12:00:00Z"]),
                "stop_departure_dt": pd.to_datetime([pd.NaT, "2026-04-07T12:00:30Z"]),
                "travel_time_seconds": [0, 300],
                "dwell_time_seconds": [30, 30],
                "headway_seconds": [600, 600],
                "scheduled_tt": [0, 280],
                "scheduled_headway": [600, 600],
            }
        )
        result = bus_ingest._process_bus_arrival_departure_times(df)
        departures = result[result.event_type == "DEP"]
        self.assertEqual(len(departures), 1)
        self.assertEqual(departures.iloc[0]["stop_id"], "stop-A")
        self.assertEqual(departures.iloc[0]["visit_rank"], 0)

    def test_process_bus_arrival_departure_times_unique_index(self):
        """Output index must be unique.

        route_starts = pq_df.loc[pq_df.groupby("trip_id").event_time.idxmin()] in
        _recalculate_bus_fields_from_gtfs returns every row matching a duplicated index label,
        not just the one idxmin selected. A trip whose earliest captured row already has
        previous_stop_id set (LAMP's daily window starting mid-trip) produces an ARR row and a
        DEP row from that same source row, which previously shared its index after concat.
        """
        df = pd.DataFrame(
            {
                "service_date": ["20260407"],
                "route_id": ["1"],
                "trip_id": ["trip-1"],
                "stop_id": ["stop-B"],
                "direction_id": [0],
                "stop_sequence": [5],
                "vehicle_label": ["y0001"],
                "previous_stop_id": ["stop-A"],
                "stop_arrival_dt": pd.to_datetime(["2026-04-07T12:00:00Z"]),
                "stop_departure_dt": pd.to_datetime(["2026-04-07T12:00:30Z"]),
                "travel_time_seconds": [300],
                "dwell_time_seconds": [30],
                "headway_seconds": [600],
                "scheduled_tt": [280],
                "scheduled_headway": [600],
            }
        )
        result = bus_ingest._process_bus_arrival_departure_times(df)
        self.assertEqual(len(result), 2)  # ARR-B and DEP-A, both derived from the one raw row
        self.assertTrue(result.index.is_unique)

    def test_process_bus_arrival_departure_times_loop_route_visit_rank(self):
        """A loop/circulator trip that revisits a stop_id gets a distinct visit_rank per visit."""
        df = pd.DataFrame(
            {
                "service_date": ["20260407"] * 4,
                "route_id": ["1"] * 4,
                "trip_id": ["trip-1"] * 4,
                "stop_id": ["stop-A", "stop-B", "stop-A", "stop-C"],
                "direction_id": [0] * 4,
                "stop_sequence": [1, 2, 3, 4],
                "vehicle_label": ["y0001"] * 4,
                "previous_stop_id": [None, "stop-A", "stop-B", "stop-A"],
                "stop_arrival_dt": pd.to_datetime(
                    [
                        "2026-04-07T12:00:00Z",
                        "2026-04-07T12:05:00Z",
                        "2026-04-07T12:15:00Z",
                        "2026-04-07T12:20:00Z",
                    ]
                ),
                "stop_departure_dt": pd.to_datetime(
                    [
                        pd.NaT,
                        "2026-04-07T12:00:30Z",
                        "2026-04-07T12:05:30Z",
                        "2026-04-07T12:15:30Z",
                    ]
                ),
                "travel_time_seconds": [0, 300, 600, 300],
                "dwell_time_seconds": [30, 30, 30, 30],
                "headway_seconds": [600, 600, 600, 600],
                "scheduled_tt": [0, 300, 600, 900],
                "scheduled_headway": [600, 600, 600, 600],
            }
        )
        result = bus_ingest._process_bus_arrival_departure_times(df)
        # 4 raw rows -> 4 ARR + 3 DEP (first row has no previous_stop_id) = 7, no fan-out
        self.assertEqual(len(result), 7)
        stop_a_events = result[result.stop_id == "stop-A"]
        self.assertEqual(sorted(stop_a_events["visit_rank"].tolist()), [0, 0, 1, 1])

    def test_recalculate_bus_fields_from_gtfs_loop_route_no_fanout(self):
        """Revisiting a stop_id doesn't fan out rows, and each visit gets its own scheduled_tt."""
        raw = pd.DataFrame(
            {
                "service_date": ["20260407"] * 4,
                "route_id": ["1"] * 4,
                "trip_id": ["trip-1"] * 4,
                "stop_id": ["stop-A", "stop-B", "stop-A", "stop-C"],
                "direction_id": [0] * 4,
                "stop_sequence": [1, 2, 3, 4],
                "vehicle_label": ["y0001"] * 4,
                "previous_stop_id": [None, "stop-A", "stop-B", "stop-A"],
                "stop_arrival_dt": pd.to_datetime(
                    [
                        "2026-04-07T12:00:00Z",
                        "2026-04-07T12:05:00Z",
                        "2026-04-07T12:15:00Z",
                        "2026-04-07T12:20:00Z",
                    ]
                ),
                "stop_departure_dt": pd.to_datetime(
                    [
                        pd.NaT,
                        "2026-04-07T12:00:30Z",
                        "2026-04-07T12:05:30Z",
                        "2026-04-07T12:15:30Z",
                    ]
                ),
                "travel_time_seconds": [0, 300, 600, 300],
                "dwell_time_seconds": [30, 30, 30, 30],
                "headway_seconds": [600, 600, 600, 600],
                "scheduled_tt": [0, 300, 600, 900],
                "scheduled_headway": [600, 600, 600, 600],
            }
        )
        processed = bus_ingest._process_bus_arrival_departure_times(raw)

        mock_gtfs = pd.DataFrame(
            {
                "trip_id": ["sched-1"] * 4,
                "stop_id": ["stop-A", "stop-B", "stop-A", "stop-C"],
                "stop_sequence": [1, 2, 3, 4],
                "arrival_time": pd.array([0, 300, 600, 900], dtype="Int64"),
                "checkpoint_id": ["cpa", "cpb", "cpa", "cpc"],
                "route_id": ["1"] * 4,
                "direction_id": pd.array([0] * 4, dtype="int16"),
            }
        )
        with mock.patch("chalicelib.lamp.bus_ingest.fetch_stop_times_from_gtfs", return_value=mock_gtfs):
            result = bus_ingest._recalculate_bus_fields_from_gtfs(processed, date(2026, 4, 7))

        self.assertEqual(len(result), len(processed))  # no fan-out
        stop_a_tts = result[result["stop_id"] == "stop-A"]["scheduled_tt"]
        # first visit to A (visit_rank 0) should differ from the second (visit_rank 1)
        self.assertEqual(set(stop_a_tts.tolist()), {0.0, 600.0})

    def test_ingest_bus_pq_file(self):
        with mock.patch("chalicelib.lamp.bus_ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
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

    def test_ingest_bus_pq_file_drops_partial_trips(self):
        """Rows with is_full_trip=False (deadhead/partial trips) must not reach the output.

        The sample data is 100% is_full_trip=True, so this can only be caught by forcing one
        row to False and comparing against the unmodified baseline.
        """
        df_partial = self.sample_df.copy()
        df_partial.loc[df_partial.index[0], "is_full_trip"] = False
        mock_gtfs = _checkpoint_gtfs_mock(self.sample_df)

        with mock.patch("chalicelib.lamp.bus_ingest.fetch_stop_times_from_gtfs", return_value=mock_gtfs):
            result_full = bus_ingest.ingest_bus_pq_file(self.sample_df, date(2026, 4, 7))
            result_partial = bus_ingest.ingest_bus_pq_file(df_partial, date(2026, 4, 7))

        self.assertLess(len(result_partial), len(result_full))

    def test_filter_to_checkpoints_is_per_route_direction(self):
        """A stop is kept only on the route/direction whose GTFS trips mark it a checkpoint."""
        events = pd.DataFrame(
            {
                "route_id": pd.array(["1", "1", "1", "47", "1"], dtype="string"),
                "direction_id": pd.array([0, 0, 0, 0, 1], dtype="int16"),
                "stop_id": pd.array(["A", "B", "C", "A", "A"], dtype="string"),
            }
        )
        gtfs_stops = pd.DataFrame(
            {
                "route_id": pd.array(["1", "1", "1", "1", "47", "1"], dtype="string"),
                "direction_id": pd.array([0, 0, 0, 0, 0, 1], dtype="int16"),
                "stop_id": pd.array(["A", "B", "B", "C", "A", "A"], dtype="string"),
                # B is a checkpoint on only one trip variant; C has an empty checkpoint_id
                "checkpoint_id": pd.array(["cpa", None, "cpb", "", None, None], dtype="string"),
            }
        )
        result = bus_ingest._filter_to_checkpoints(events, gtfs_stops)
        self.assertEqual(
            list(result[["route_id", "direction_id", "stop_id"]].itertuples(index=False, name=None)),
            [("1", 0, "A"), ("1", 0, "B")],
        )

    def test_ingest_bus_pq_file_keeps_only_checkpoints(self):
        stops = self.sample_df[["route_id", "direction_id", "stop_id"]].dropna().drop_duplicates()
        mock_gtfs = _checkpoint_gtfs_mock(stops.iloc[::3])
        expected_stops = set(mock_gtfs["stop_id"])

        with mock.patch("chalicelib.lamp.bus_ingest.fetch_stop_times_from_gtfs", return_value=mock_gtfs):
            result = bus_ingest.ingest_bus_pq_file(self.sample_df, date(2026, 4, 7))

        self.assertGreater(len(result), 0)
        self.assertLessEqual(set(result["stop_id"]), expected_stops)

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
                with mock.patch(
                    "chalicelib.lamp.bus_ingest.fetch_stop_times_from_gtfs",
                    return_value=self.mock_gtfs_data,
                ):
                    bus_ingest.ingest_bus_data(date(2026, 4, 7))
                    mock_upload.assert_called_once()

    def test_ingest_bus_data_no_file_found(self):
        mock_response = mock.Mock(status_code=404)
        with mock.patch("requests.get", return_value=mock_response):
            # Should not raise - logs error and returns
            bus_ingest.ingest_bus_data(date(2026, 4, 7))

    def test_ingest_yesterday_bus_data(self):
        mock_response = mock.Mock(status_code=200, content=self.data)
        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.lamp.bus_ingest._parallel_upload"):
                with mock.patch(
                    "chalicelib.lamp.bus_ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data
                ):
                    with mock.patch(
                        "chalicelib.lamp.bus_ingest.get_current_service_date", return_value=date(2026, 4, 8)
                    ):
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
