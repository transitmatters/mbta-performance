import io
import unittest
from datetime import date
from unittest import mock

import pandas as pd
import pyarrow.parquet as pq

from .. import geoparquet, s3_writer


def _segments() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "route_id": "1",
                "direction_id": 0,
                "from_stop_id": "s1",
                "to_stop_id": "s2",
                "service_date": date(2026, 9, 3),
                "time_band": "am_peak",
                "n_traversals": 12,
                "segment_length_m": 500.0,
                "p50_speed_mph": 11.5,
                "coordinates": [(-71.05, 42.36), (-71.06, 42.37)],
            }
        ]
    )


class TestS3Key(unittest.TestCase):
    def test_month_and_day_are_not_zero_padded(self):
        # Matches the existing Events-lamp/ layout, which the dashboard parses.
        key = s3_writer.s3_key_for(date(2026, 9, 3))

        self.assertEqual(key, "BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments.parquet")

    def test_double_digit_month_and_day(self):
        self.assertEqual(
            s3_writer.s3_key_for(date(2026, 12, 24)),
            "BusSpeedSegments/daily/Year=2026/Month=12/Day=24/segments.parquet",
        )


class TestPmtilesKey(unittest.TestCase):
    def test_month_and_day_are_not_zero_padded(self):
        key = s3_writer.pmtiles_key_for(date(2026, 9, 3))

        self.assertEqual(key, "BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments.pmtiles")


class TestUploadPmtiles(unittest.TestCase):
    def test_uploads_built_pmtiles_bytes_to_the_dated_key(self):
        with (
            mock.patch.object(s3_writer, "build_pmtiles_bytes", return_value=b"PMTiles\x03fake") as build,
            mock.patch.object(s3_writer.s3, "upload_pmtiles") as upload,
        ):
            key = s3_writer.upload_pmtiles(_segments(), date(2026, 9, 3))

        build.assert_called_once()
        upload.assert_called_once()
        bucket, uploaded_key, data = upload.call_args[0]
        self.assertEqual(bucket, "tm-mbta-performance")
        self.assertEqual(uploaded_key, key)
        self.assertEqual(uploaded_key, "BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments.pmtiles")
        self.assertEqual(data, b"PMTiles\x03fake")


class TestUpload(unittest.TestCase):
    def test_uploads_parquet_bytes_to_the_dated_key(self):
        with mock.patch.object(s3_writer.s3, "upload_parquet") as upload:
            key = s3_writer.upload_speed_segments(_segments(), date(2026, 9, 3))

        upload.assert_called_once()
        bucket, uploaded_key, data = upload.call_args[0]
        self.assertEqual(bucket, "tm-mbta-performance")
        self.assertEqual(uploaded_key, key)
        self.assertEqual(uploaded_key, "BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments.parquet")
        # PAR1 is the parquet magic number at the head of the file.
        self.assertTrue(data.startswith(b"PAR1"))

    def test_uploaded_bytes_are_readable_geoparquet(self):
        with mock.patch.object(s3_writer.s3, "upload_parquet") as upload:
            s3_writer.upload_speed_segments(_segments(), date(2026, 9, 3))
        data = upload.call_args[0][2]

        table = pq.read_table(io.BytesIO(data))

        self.assertIn(b"geo", table.schema.metadata)
        self.assertIn("geometry", table.column_names)
        # The raw coordinate lists must not survive into the published file.
        self.assertNotIn("coordinates", table.column_names)
        self.assertEqual(table.num_rows, 1)

    def test_bytes_and_file_paths_produce_identical_output(self):
        frame = _segments()
        data = geoparquet.build_geoparquet_bytes(frame)

        from_bytes = pq.read_table(io.BytesIO(data))

        self.assertEqual(from_bytes.column_names, list(from_bytes.schema.names))
        self.assertIn(b"geo", from_bytes.schema.metadata)


class TestWeeklyMonthlyKeys(unittest.TestCase):
    def test_weekly_keys_are_keyed_by_year_and_week_number(self):
        self.assertEqual(
            s3_writer.weekly_s3_key_for(2026, 6), "BusSpeedSegments/weekly/Year=2026/Week=6/segments.parquet"
        )
        self.assertEqual(
            s3_writer.weekly_pmtiles_key_for(2026, 6), "BusSpeedSegments/weekly/Year=2026/Week=6/segments.pmtiles"
        )

    def test_monthly_keys_are_keyed_by_year_and_month_number(self):
        self.assertEqual(
            s3_writer.monthly_s3_key_for(2026, 3), "BusSpeedSegments/monthly/Year=2026/Month=3/segments.parquet"
        )
        self.assertEqual(
            s3_writer.monthly_pmtiles_key_for(2026, 3), "BusSpeedSegments/monthly/Year=2026/Month=3/segments.pmtiles"
        )


class TestUploadWeeklyMonthly(unittest.TestCase):
    def test_upload_weekly_speed_segments_writes_to_the_week_key(self):
        with mock.patch.object(s3_writer.s3, "upload_parquet") as upload:
            key = s3_writer.upload_weekly_speed_segments(_segments(), 2026, 6)

        self.assertEqual(key, "BusSpeedSegments/weekly/Year=2026/Week=6/segments.parquet")
        upload.assert_called_once()
        self.assertEqual(upload.call_args[0][1], key)

    def test_upload_weekly_pmtiles_writes_to_the_week_key(self):
        with (
            mock.patch.object(s3_writer, "build_pmtiles_bytes", return_value=b"PMTiles\x03fake"),
            mock.patch.object(s3_writer.s3, "upload_pmtiles") as upload,
        ):
            key = s3_writer.upload_weekly_pmtiles(_segments(), 2026, 6)

        self.assertEqual(key, "BusSpeedSegments/weekly/Year=2026/Week=6/segments.pmtiles")
        upload.assert_called_once()
        self.assertEqual(upload.call_args[0][1], key)

    def test_upload_monthly_speed_segments_writes_to_the_month_key(self):
        with mock.patch.object(s3_writer.s3, "upload_parquet") as upload:
            key = s3_writer.upload_monthly_speed_segments(_segments(), 2026, 3)

        self.assertEqual(key, "BusSpeedSegments/monthly/Year=2026/Month=3/segments.parquet")
        upload.assert_called_once()
        self.assertEqual(upload.call_args[0][1], key)

    def test_upload_monthly_pmtiles_writes_to_the_month_key(self):
        with (
            mock.patch.object(s3_writer, "build_pmtiles_bytes", return_value=b"PMTiles\x03fake"),
            mock.patch.object(s3_writer.s3, "upload_pmtiles") as upload,
        ):
            key = s3_writer.upload_monthly_pmtiles(_segments(), 2026, 3)

        self.assertEqual(key, "BusSpeedSegments/monthly/Year=2026/Month=3/segments.pmtiles")
        upload.assert_called_once()
        self.assertEqual(upload.call_args[0][1], key)


class TestUploadIsOptIn(unittest.TestCase):
    def test_generate_does_not_upload_unless_asked(self):
        from .. import ingest

        with (
            mock.patch.object(ingest, "read_service_date") as read,
            mock.patch.object(ingest, "upload_speed_segments") as upload,
            mock.patch.object(ingest, "upload_pmtiles") as upload_pmtiles,
            mock.patch.object(ingest, "build_pattern_geometry"),
        ):
            read.return_value = pd.DataFrame()
            with self.assertRaises(ValueError):
                ingest.generate_speed_segments(date(2026, 9, 3))

        upload.assert_not_called()
        upload_pmtiles.assert_not_called()
