from datetime import date
import io
import os
import unittest
from unittest import mock

import pandas as pd

from .. import ingest
from .. import constants

# The sample file attached here is 10k events sampled from Feb 7th, 2024.
# These rows contain real-world inconsistencies in their data!
DATA_PREFIX = "mbta-performance/chalicelib/lamp/tests/sample_data/"
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
                ],
            )

    # Before December 2023, LAMP will include trips both revenue and not.
    # We should expect to see a few non-revenue trips in the output, and filter them out.
    def test_ingest_pq_file_nonrevenue(self):
        pq_df_before = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        )
        pq_df_before["direction_id"] = pq_df_before["direction_id"].astype("int16")

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            pq_df_after = ingest.ingest_pq_file(pq_df_before, date(2023, 10, 7))
        nonrev = pq_df_after[pq_df_after["trip_id"].str.startswith("NONREV-")]
        added = pq_df_after[pq_df_after["trip_id"].str.startswith("ADDED-")]
        null_id_events = pq_df_after[pq_df_after["stop_id"].isna()]
        self.assertTrue(nonrev.empty)
        self.assertEqual(added.shape, (3763, 17))
        self.assertTrue(null_id_events.empty)
        self.assertEqual(pq_df_after.shape, (16700, 17))
        self.assertEqual(set(pq_df_after["service_date"].unique()), {"2023-10-07"})

    # After December 2023, LAMP will only include trips that are properly revenue.
    # Anything labeled as NONREV- or ADDED- after December 2023 are actually considered revenue
    def test_ingest_pq_file_revenue(self):
        pq_df_before = pd.read_parquet(
            io.BytesIO(self.data),
            columns=constants.LAMP_COLUMNS,
            engine="pyarrow",
            dtype_backend="numpy_nullable",
        )
        pq_df_before["direction_id"] = pq_df_before["direction_id"].astype("int16")

        with mock.patch("chalicelib.lamp.ingest.fetch_stop_times_from_gtfs", return_value=self.mock_gtfs_data):
            pq_df_after = ingest.ingest_pq_file(pq_df_before, date(2024, 4, 13))
        nonrev = pq_df_after[pq_df_after["trip_id"].str.startswith("NONREV-")]
        added = pq_df_after[pq_df_after["trip_id"].str.startswith("ADDED-")]
        null_id_events = pq_df_after[pq_df_after["stop_id"].isna()]
        self.assertFalse(nonrev.empty)
        self.assertEqual(added.shape, (3763, 17))
        self.assertTrue(null_id_events.empty)
        self.assertEqual(pq_df_after.shape, (16700, 17))
        self.assertEqual(set(pq_df_after["service_date"].unique()), {"2024-04-13"})

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
        self.assertEqual(null_headway_events_after.shape, (245, 17))
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

    def test_upload_to_s3(self):
        pass

    def test_ingest_today_lamp_data(self):
        pass
