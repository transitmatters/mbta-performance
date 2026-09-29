import io
import json
import unittest
from datetime import date
from unittest import mock

import pandas as pd
import pyarrow.parquet as pq

from .. import geoparquet, s3_writer


def _segments(time_bands: tuple[str, ...] = ("am_peak",)) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "route_id": "1",
                "direction_id": 0,
                "from_stop_id": "s1",
                "to_stop_id": "s2",
                "service_date": date(2026, 9, 3),
                "time_band": time_band,
                "n_traversals": 12,
                "segment_length_m": 500.0,
                "p50_speed_mph": 11.5,
                "coordinates": [(-71.05, 42.36), (-71.06, 42.37)],
            }
            for time_band in time_bands
        ]
    )


def _fake_build(frame) -> bytes:
    """Stands in for tippecanoe: encodes which bands a tileset was built from."""
    return ("PMTiles:" + ",".join(sorted(frame.time_band))).encode()


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

    def test_all_day_key_sits_beside_the_band_key(self):
        self.assertEqual(
            s3_writer.all_day_pmtiles_key_for(date(2026, 9, 3)),
            "BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments_all_day.pmtiles",
        )


class TestUploadPmtiles(unittest.TestCase):
    def test_bands_and_all_day_are_built_and_uploaded_as_separate_archives(self):
        with (
            mock.patch.object(s3_writer, "build_pmtiles_bytes", side_effect=_fake_build),
            mock.patch.object(s3_writer.s3, "upload_pmtiles") as upload,
        ):
            keys = s3_writer.upload_pmtiles(_segments(("am_peak", "midday", "all_day")), date(2026, 9, 3))

        uploaded = {call.args[1]: call.args[2] for call in upload.call_args_list}
        self.assertEqual({call.args[0] for call in upload.call_args_list}, {"tm-mbta-performance"})
        self.assertEqual(
            uploaded,
            {
                "BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments.pmtiles": b"PMTiles:am_peak,midday",
                "BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments_all_day.pmtiles": b"PMTiles:all_day",
            },
        )
        self.assertEqual(keys, tuple(uploaded))


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
        self.assertEqual(
            s3_writer.weekly_all_day_pmtiles_key_for(2026, 6),
            "BusSpeedSegments/weekly/Year=2026/Week=6/segments_all_day.pmtiles",
        )

    def test_monthly_keys_are_keyed_by_year_and_month_number(self):
        self.assertEqual(
            s3_writer.monthly_s3_key_for(2026, 3), "BusSpeedSegments/monthly/Year=2026/Month=3/segments.parquet"
        )
        self.assertEqual(
            s3_writer.monthly_pmtiles_key_for(2026, 3), "BusSpeedSegments/monthly/Year=2026/Month=3/segments.pmtiles"
        )
        self.assertEqual(
            s3_writer.monthly_all_day_pmtiles_key_for(2026, 3),
            "BusSpeedSegments/monthly/Year=2026/Month=3/segments_all_day.pmtiles",
        )


class TestUploadWeeklyMonthly(unittest.TestCase):
    def test_upload_weekly_speed_segments_writes_to_the_week_key(self):
        with mock.patch.object(s3_writer.s3, "upload_parquet") as upload:
            key = s3_writer.upload_weekly_speed_segments(_segments(), 2026, 6)

        self.assertEqual(key, "BusSpeedSegments/weekly/Year=2026/Week=6/segments.parquet")
        upload.assert_called_once()
        self.assertEqual(upload.call_args[0][1], key)

    def test_upload_weekly_pmtiles_writes_to_the_week_keys(self):
        with (
            mock.patch.object(s3_writer, "build_pmtiles_bytes", side_effect=_fake_build),
            mock.patch.object(s3_writer.s3, "upload_pmtiles") as upload,
        ):
            keys = s3_writer.upload_weekly_pmtiles(_segments(("am_peak", "all_day")), 2026, 6)

        self.assertEqual(
            keys,
            (
                "BusSpeedSegments/weekly/Year=2026/Week=6/segments.pmtiles",
                "BusSpeedSegments/weekly/Year=2026/Week=6/segments_all_day.pmtiles",
            ),
        )
        self.assertEqual(
            [(call.args[1], call.args[2]) for call in upload.call_args_list],
            list(zip(keys, [b"PMTiles:am_peak", b"PMTiles:all_day"])),
        )

    def test_upload_monthly_speed_segments_writes_to_the_month_key(self):
        with mock.patch.object(s3_writer.s3, "upload_parquet") as upload:
            key = s3_writer.upload_monthly_speed_segments(_segments(), 2026, 3)

        self.assertEqual(key, "BusSpeedSegments/monthly/Year=2026/Month=3/segments.parquet")
        upload.assert_called_once()
        self.assertEqual(upload.call_args[0][1], key)

    def test_upload_monthly_pmtiles_writes_to_the_month_keys(self):
        with (
            mock.patch.object(s3_writer, "build_pmtiles_bytes", side_effect=_fake_build),
            mock.patch.object(s3_writer.s3, "upload_pmtiles") as upload,
        ):
            keys = s3_writer.upload_monthly_pmtiles(_segments(("am_peak", "all_day")), 2026, 3)

        self.assertEqual(
            keys,
            (
                "BusSpeedSegments/monthly/Year=2026/Month=3/segments.pmtiles",
                "BusSpeedSegments/monthly/Year=2026/Month=3/segments_all_day.pmtiles",
            ),
        )
        self.assertEqual(
            [(call.args[1], call.args[2]) for call in upload.call_args_list],
            list(zip(keys, [b"PMTiles:am_peak", b"PMTiles:all_day"])),
        )


class TestLeaderboardKeys(unittest.TestCase):
    def test_daily_key_is_month_and_day_not_zero_padded(self):
        self.assertEqual(
            s3_writer.leaderboard_key_for(date(2026, 9, 3)),
            "BusSpeedSegments/daily/Year=2026/Month=9/Day=3/leaderboard.json",
        )

    def test_weekly_and_monthly_keys_are_keyed_by_year_and_period_number(self):
        self.assertEqual(
            s3_writer.weekly_leaderboard_key_for(2026, 6), "BusSpeedSegments/weekly/Year=2026/Week=6/leaderboard.json"
        )
        self.assertEqual(
            s3_writer.monthly_leaderboard_key_for(2026, 3),
            "BusSpeedSegments/monthly/Year=2026/Month=3/leaderboard.json",
        )


class TestUploadLeaderboard(unittest.TestCase):
    _leaderboard = {"am_peak": [{"route_id": "1", "p50_speed_mph": 5.0}]}

    def test_uploads_json_bytes_to_the_dated_key(self):
        with mock.patch.object(s3_writer.s3, "upload_json") as upload:
            key = s3_writer.upload_leaderboard(self._leaderboard, date(2026, 9, 3))

        self.assertEqual(key, "BusSpeedSegments/daily/Year=2026/Month=9/Day=3/leaderboard.json")
        upload.assert_called_once()
        bucket, uploaded_key, data = upload.call_args[0]
        self.assertEqual(bucket, "tm-mbta-performance")
        self.assertEqual(uploaded_key, key)
        self.assertEqual(json.loads(data), self._leaderboard)

    def test_upload_weekly_leaderboard_writes_to_the_week_key(self):
        with mock.patch.object(s3_writer.s3, "upload_json") as upload:
            key = s3_writer.upload_weekly_leaderboard(self._leaderboard, 2026, 6)

        self.assertEqual(key, "BusSpeedSegments/weekly/Year=2026/Week=6/leaderboard.json")
        upload.assert_called_once()
        self.assertEqual(upload.call_args[0][1], key)

    def test_upload_monthly_leaderboard_writes_to_the_month_key(self):
        with mock.patch.object(s3_writer.s3, "upload_json") as upload:
            key = s3_writer.upload_monthly_leaderboard(self._leaderboard, 2026, 3)

        self.assertEqual(key, "BusSpeedSegments/monthly/Year=2026/Month=3/leaderboard.json")
        upload.assert_called_once()
        self.assertEqual(upload.call_args[0][1], key)


class TestUploadIsOptIn(unittest.TestCase):
    def test_generate_does_not_upload_unless_asked(self):
        from .. import ingest

        with (
            mock.patch.object(ingest, "read_service_date") as read,
            mock.patch.object(ingest, "upload_speed_segments") as upload,
            mock.patch.object(ingest, "upload_pmtiles") as upload_pmtiles,
            mock.patch.object(ingest, "upload_leaderboard") as upload_leaderboard,
            mock.patch.object(ingest, "build_pattern_geometry"),
        ):
            read.return_value = pd.DataFrame()
            with self.assertRaises(ValueError):
                ingest.generate_speed_segments(date(2026, 9, 3))

        upload.assert_not_called()
        upload_pmtiles.assert_not_called()
        upload_leaderboard.assert_not_called()
