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


def _lamp_trip(stop_ids: list, arrivals: list, departures: list) -> pd.DataFrame:
    """One trip's bus LAMP rows, columns already renamed, built the way LAMP builds them.

    Each row is one stop visit: arrival at and departure from that row's own stop_id. LAMP's
    stopped_duration_seconds is departure - arrival on the row, and travel_time_seconds is the
    row's arrival minus the previous row's departure.
    """
    n = len(stop_ids)
    arr = pd.Series(pd.to_datetime(arrivals, utc=True))
    dep = pd.Series(pd.to_datetime(departures, utc=True))
    return pd.DataFrame(
        {
            "service_date": ["20260407"] * n,
            "route_id": ["1"] * n,
            "trip_id": ["trip-1"] * n,
            "stop_id": stop_ids,
            "direction_id": [0] * n,
            "stop_sequence": list(range(1, n + 1)),
            "vehicle_label": ["y0001"] * n,
            "stop_arrival_dt": arr,
            "stop_departure_dt": dep,
            "travel_time_seconds": (arr - dep.shift(1)).dt.total_seconds(),
            "dwell_time_seconds": (dep - arr).dt.total_seconds(),
            "headway_seconds": [600] * n,
            "scheduled_tt": [300 * i for i in range(n)],
            "scheduled_headway": [600] * n,
        }
    )


def _loop_trip() -> pd.DataFrame:
    """A loop/circulator trip A -> B -> A -> C that visits stop-A twice."""
    return _lamp_trip(
        ["stop-A", "stop-B", "stop-A", "stop-C"],
        arrivals=["2026-04-07T12:00:00Z", "2026-04-07T12:05:00Z", "2026-04-07T12:15:00Z", "2026-04-07T12:20:00Z"],
        departures=["2026-04-07T12:00:30Z", "2026-04-07T12:05:30Z", "2026-04-07T12:15:30Z", None],
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

    def test_departures_keep_their_own_stop(self):
        """A DEP row is the bus leaving that row's own stop, not the stop before it (#129).

        The trip arrives at its origin A at 11:55, lays over and leaves at 12:00, reaches B at
        12:05, leaves B at 12:05:30, and ends at C.
        """
        df = _lamp_trip(
            ["stop-A", "stop-B", "stop-C"],
            arrivals=["2026-04-07T11:55:00Z", "2026-04-07T12:05:00Z", "2026-04-07T12:10:00Z"],
            departures=["2026-04-07T12:00:00Z", "2026-04-07T12:05:30Z", None],
        )
        result = bus_ingest._process_bus_arrival_departure_times(df)
        events = result.set_index(["stop_id", "event_type"])
        times = events["event_time"]

        self.assertEqual(times["stop-A", "DEP"], pd.Timestamp("2026-04-07T12:00:00Z"))  # origin departure kept
        self.assertEqual(times["stop-B", "DEP"], pd.Timestamp("2026-04-07T12:05:30Z"))
        self.assertNotIn(("stop-C", "DEP"), events.index)
        self.assertEqual(events.loc[("stop-A", "DEP"), "stop_sequence"], 1)
        # What the dashboard derives from these events matches LAMP's own fields on B's row.
        travel_time = (times["stop-B", "ARR"] - times["stop-A", "DEP"]).total_seconds()
        dwell = (times["stop-B", "DEP"] - times["stop-B", "ARR"]).total_seconds()
        self.assertEqual(travel_time, df.loc[1, "travel_time_seconds"])
        self.assertEqual(dwell, df.loc[1, "dwell_time_seconds"])

    def test_process_bus_arrival_departure_times_sample_dwell_matches_lamp(self):
        """On real LAMP rows, DEP - ARR at the same stop visit is LAMP's stopped_duration_seconds."""
        df = self.sample_df.rename(columns=bus_constants.BUS_COLUMN_RENAME_MAP)
        result = bus_ingest._process_bus_arrival_departure_times(df)

        visit = ["trip_id", "stop_id", "visit_rank"]
        arr = result[result.event_type == "ARR"].set_index(visit)
        dep = result[result.event_type == "DEP"].set_index(visit)
        self.assertEqual(len(dep), df["stop_departure_dt"].notna().sum())  # includes each origin's departure

        both = arr.index.intersection(dep.index)
        dwell = (dep.loc[both, "event_time"] - arr.loc[both, "event_time"]).dt.total_seconds()
        lamp_dwell = arr.loc[both, "dwell_time_seconds"].astype(float)
        has_dwell = lamp_dwell.notna()
        self.assertGreater(has_dwell.sum(), 0)
        pd.testing.assert_series_equal(dwell[has_dwell], lamp_dwell[has_dwell], check_names=False)

    def test_process_bus_arrival_departure_times_unique_index(self):
        """Output index must be unique.

        route_starts = pq_df.loc[pq_df.groupby("trip_id").event_time.idxmin()] in
        _recalculate_bus_fields_from_gtfs returns every row matching a duplicated index label,
        not just the one idxmin selected. A raw row with both an arrival and a departure produces
        an ARR row and a DEP row, which would otherwise share its index after concat.
        """
        df = _lamp_trip(["stop-B"], arrivals=["2026-04-07T12:00:00Z"], departures=["2026-04-07T12:00:30Z"])
        result = bus_ingest._process_bus_arrival_departure_times(df)
        self.assertEqual(len(result), 2)  # ARR-B and DEP-B, both derived from the one raw row
        self.assertTrue(result.index.is_unique)

    def test_process_bus_arrival_departure_times_loop_route_visit_rank(self):
        """A loop/circulator trip that revisits a stop_id gets a distinct visit_rank per visit."""
        df = _loop_trip()
        result = bus_ingest._process_bus_arrival_departure_times(df)
        # 4 raw rows -> 4 ARR + 3 DEP (the last stop has no departure) = 7, no fan-out
        self.assertEqual(len(result), 7)
        stop_a_events = result[result.stop_id == "stop-A"].sort_values("event_time")
        self.assertEqual(stop_a_events["event_type"].tolist(), ["ARR", "DEP", "ARR", "DEP"])
        self.assertEqual(stop_a_events["visit_rank"].tolist(), [0, 0, 1, 1])

    def test_recalculate_bus_fields_from_gtfs_loop_route_no_fanout(self):
        """Revisiting a stop_id doesn't fan out rows, and each visit gets its own scheduled_tt."""
        processed = bus_ingest._process_bus_arrival_departure_times(_loop_trip())

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

    def test_fallback_scheduled_tt_with_a_half_second_median(self):
        """As in rail: a half-second fallback median used to be rejected by the Int64 scheduled_tt."""
        raw = pd.DataFrame(
            {
                "service_date": ["20260407"] * 3,
                "route_id": ["1"] * 3,
                "trip_id": ["trip-1"] * 3,
                "stop_id": ["stop-A", "stop-B", "stop-C"],
                "direction_id": [0] * 3,
                "stop_sequence": [1, 2, 3],
                "vehicle_label": ["y0001"] * 3,
                "previous_stop_id": [None, "stop-A", "stop-B"],
                # 8:00, 8:05 and 8:10am Eastern
                "stop_arrival_dt": pd.to_datetime(
                    ["2026-04-07T12:00:00Z", "2026-04-07T12:05:00Z", "2026-04-07T12:10:00Z"]
                ),
                "stop_departure_dt": pd.to_datetime([pd.NaT, "2026-04-07T12:00:30Z", "2026-04-07T12:05:30Z"]),
                "travel_time_seconds": [0, 300, 300],
                "dwell_time_seconds": [30] * 3,
                "headway_seconds": [600] * 3,
                "scheduled_tt": [0, 300, 600],
                "scheduled_headway": [600] * 3,
            }
        )
        processed = bus_ingest._process_bus_arrival_departure_times(raw)

        # trip-1 matches sched-1, which skips stop-C, so stop-C falls back to the median of 60s and 61s
        rows = [
            ("sched-1", "stop-A", 28800, 1),
            ("sched-1", "stop-B", 29100, 2),
            ("sched-2", "stop-A", 30000, 1),
            ("sched-2", "stop-C", 30060, 3),
            ("sched-3", "stop-A", 30100, 1),
            ("sched-3", "stop-C", 30161, 3),
        ]
        mock_gtfs = pd.DataFrame(
            {
                "trip_id": pd.array([r[0] for r in rows], dtype="string"),
                "stop_id": pd.array([r[1] for r in rows], dtype="string"),
                "arrival_time": pd.array([r[2] for r in rows], dtype="Int64"),
                "stop_sequence": pd.array([r[3] for r in rows], dtype="Int64"),
                "checkpoint_id": pd.array(["cp"] * len(rows), dtype="string"),
                "route_id": pd.array(["1"] * len(rows), dtype="string"),
                "direction_id": pd.array([0] * len(rows), dtype="int16"),
            }
        )
        with mock.patch("chalicelib.lamp.bus_ingest.fetch_stop_times_from_gtfs", return_value=mock_gtfs):
            result = bus_ingest._recalculate_bus_fields_from_gtfs(processed, date(2026, 4, 7))

        scheduled_tt = result.set_index(["stop_id", "event_type"])["scheduled_tt"]
        self.assertEqual(scheduled_tt[("stop-B", "ARR")], 300)
        self.assertEqual(scheduled_tt[("stop-C", "ARR")], 60)  # 60.5, rounded half to even

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

        with mock.patch("chalicelib.lamp.bus_ingest.s3.upload_df_as_csv_if_changed") as mock_upload:
            result = bus_ingest.upload_bus_to_s3((("1", 0, "110"), df), date(2026, 4, 7))

            mock_upload.assert_called_once()
            call_args = mock_upload.call_args
            self.assertEqual(call_args[0][0], "tm-mbta-performance")
            expected_key = "Events-lamp/bus-daily-data/1-0-110/Year=2026/Month=4/Day=7/events.csv"
            self.assertEqual(call_args[0][1], expected_key)
            self.assertEqual(result, [("1", 0, "110")])

    def test_upload_bus_to_s3_skips_unchanged_when_given_remote_etags(self):
        df = pd.DataFrame({col: ["test"] for col in bus_constants.BUS_S3_COLUMNS})
        key = "Events-lamp/bus-daily-data/1-0-110/Year=2026/Month=4/Day=7/events.csv"

        with mock.patch("chalicelib.lamp.bus_ingest.s3.upload_df_as_csv_if_changed", return_value=False) as mock_upload:
            result = bus_ingest.upload_bus_to_s3((("1", 0, "110"), df), date(2026, 4, 7), remote_etags={key: "abc"})

        self.assertEqual(mock_upload.call_args[0][1], key)
        self.assertEqual(result, [])

        with mock.patch("chalicelib.lamp.bus_ingest.s3.upload_df_as_csv_if_changed", return_value=True):
            result = bus_ingest.upload_bus_to_s3((("1", 0, "110"), df), date(2026, 4, 7), remote_etags={})
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
