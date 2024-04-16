from datetime import date
import unittest
from unittest import mock

import pandas as pd

from .. import ingest


DATA_PATH = "mbta-performance/chalicelib/lamp/tests/sample_data/2024-02-07-subway-on-time-performance-v1.parquet"


class TestIngest(unittest.TestCase):
    def setUp(self):
        with open(DATA_PATH, "rb") as f:
            self.data = f.read()

    def _mock_s3_upload(self):
        # mock upload of s3.upload_df_as_csv() to a fake bucket
        pass

    def test__process_arrival_departure_times(self):
        pass

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
                ],
            )

    def test_ingest_pq_file(self):
        pass

    def test_upload_to_s3(self):
        pass

    def test_ingest_today_lamp_data(self):
        pass
