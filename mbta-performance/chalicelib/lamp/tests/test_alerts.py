import os
import tempfile
import unittest
from datetime import date, datetime
from unittest import mock
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from .. import alerts

ET = ZoneInfo("US/Eastern")


# Explicit schema so an all-None column (e.g. a row with no closed_timestamp)
# still gets a concrete nullable type -- pa.Table.from_pylist would otherwise infer
# it as an untyped "null" column, which pyarrow's Acero join engine rejects.
_SYNTHETIC_ALERTS_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("cause", pa.string()),
        ("effect_detail", pa.string()),
        ("severity", pa.int8()),
        ("alert_lifecycle", pa.string()),
        ("header_text.translation.text", pa.string()),
        ("service_effect_text.translation.text", pa.string()),
        ("created_timestamp", pa.int64()),
        ("last_modified_timestamp", pa.int64()),
        ("closed_timestamp", pa.int64()),
        ("active_period.start_timestamp", pa.int64()),
        ("active_period.end_timestamp", pa.int64()),
        ("informed_entity.route_id", pa.string()),
        ("informed_entity.route_type", pa.int8()),
        ("informed_entity.direction_id", pa.int8()),
        ("informed_entity.stop_id", pa.string()),
        ("informed_entity.activities", pa.string()),
    ]
)


def _write_alerts_parquet(rows: list[dict]) -> str:
    """Write a minimal synthetic alerts parquet (ALERT_COLUMNS + closed_timestamp)
    for tests that need to craft a specific timestamp/version scenario rather than
    rely on finding one in the real-data fixture."""
    fd, path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    pq.write_table(pa.Table.from_pylist(rows, schema=_SYNTHETIC_ALERTS_SCHEMA), path)
    return path


# 6 real alerts sliced (all versions, all rows) from the live LAMP_RT_ALERTS.parquet,
# chosen to cover the edge cases in the module docstring:
#   293631 - null route_id; final version has active_period=null + closed_timestamp set
#   332786 - simple bounded period, single entity, no stop (a bus-route alert)
#   351440 - open-ended (null end) with NO closed_timestamp anywhere -> falls back
#             to last_modified_timestamp, producing a single-day active period
#   691095 - 12 versions; the winning (latest active-period-bearing) version's end
#             was shortened by a later revision relative to earlier versions
#   1028254 - many duplicate informed_entity rows across versions -> dedup
#   1030402 - simple bus detour, 3 distinct entities sharing one stop
DATA_PREFIX = os.path.join(os.path.dirname(__file__), "sample_data")
SAMPLE_ALERTS_PATH = os.path.join(DATA_PREFIX, "lamp_alerts_sample.parquet")

FULL_HISTORY_WINDOW = (date(2018, 1, 1), date(2028, 1, 1))


class TestAlerts(unittest.TestCase):
    def setUp(self):
        self.last_seen = alerts.last_seen_by_alert(SAMPLE_ALERTS_PATH)

    def _read(self, window=FULL_HISTORY_WINDOW):
        return alerts.read_latest_versions(SAMPLE_ALERTS_PATH, window, self.last_seen)

    def test_last_seen_by_alert_covers_every_id(self):
        self.assertEqual(len(self.last_seen), 6)
        # 293631 and 332786 both have a closed_timestamp on their final version
        self.assertAlmostEqual(self.last_seen.loc[293631, "closed_timestamp"], 1689769653)
        self.assertAlmostEqual(self.last_seen.loc[332786, "closed_timestamp"], 1568680543)
        # 351440 was never closed
        self.assertTrue(
            self.last_seen.loc[351440, "closed_timestamp"] != self.last_seen.loc[351440, "closed_timestamp"]
        )

    def test_read_latest_versions_skips_the_closed_final_version(self):
        # Regression: naively taking max(last_modified_timestamp) per id would pick
        # 293631's and 332786's *closed* final version, which has no active_period.
        df = self._read()
        self.assertEqual(set(df.id.unique()), {293631, 332786, 351440, 691095, 1028254, 1030402})
        for _, row in df.iterrows():
            self.assertFalse(row.isna()["active_period.start_timestamp"])

    def test_read_latest_versions_picks_shortened_version_not_first_or_closed(self):
        # 691095 has 12 versions: an original schedule (end 2026-03-09), a later
        # shortened schedule (end 2026-03-07), then a final closed version with a
        # null active_period. We must land on the shortened one.
        df = self._read()
        sub = df[df.id == 691095]
        self.assertEqual(sub["active_period.end_timestamp"].nunique(), 1)
        end = sub["active_period.end_timestamp"].iloc[0]
        self.assertEqual(alerts._iso(end), "2026-03-07T02:59:00-05:00")

    def test_window_excludes_alerts_entirely_outside_it(self):
        # 1030402's only period is 2026-09-12; a window far in the past should not
        # pick it up via any fallback.
        df = self._read(window=(date(2019, 1, 1), date(2019, 12, 31)))
        self.assertNotIn(1030402, set(df.id.unique()))
        self.assertIn(293631, set(df.id.unique()))  # open-ended since 2019, still overlaps

    def test_build_v3_alerts_shape(self):
        df = self._read()
        built, _ = alerts.build_v3_alerts(df, window=FULL_HISTORY_WINDOW)
        alert = built["332786"]
        self.assertEqual(alert["id"], "332786")
        self.assertEqual(alert["type"], "alert")
        self.assertEqual(alert["attributes"]["effect"], "DELAY")  # effect_detail, not the coarse effect column
        self.assertIsNone(alert["attributes"]["short_header"])
        self.assertEqual(
            alert["attributes"]["active_period"],
            [{"start": "2019-09-16T17:02:04-04:00", "end": "2019-09-16T22:00:24-04:00"}],
        )

    def test_null_end_falls_back_to_closed_timestamp(self):
        df = self._read()
        built, _ = alerts.build_v3_alerts(df, window=FULL_HISTORY_WINDOW)
        period = built["293631"]["attributes"]["active_period"][0]
        # The emitted "end" stays null (matching what the v3 API itself would show
        # for a still-open alert) -- closed_timestamp only affects day bucketing.
        self.assertIsNone(period["end"])

    def test_null_end_with_no_closed_timestamp_falls_back_to_last_modified(self):
        # 351440 has one version, no closed_timestamp ever -- its effective end for
        # bucketing purposes should be its own last_modified_timestamp, producing a
        # single-day active period rather than an unbounded one.
        df = self._read()
        built, day_index = alerts.build_v3_alerts(df, window=FULL_HISTORY_WINDOW)
        days_with_351440 = [d for d, ids in day_index.items() if "351440" in ids]
        self.assertEqual(days_with_351440, [date(2020, 1, 2)])

    def test_informed_entity_omits_null_route_and_stop_keys(self):
        # Matches the v3 API's own convention, and avoids routes_for_alert-style
        # consumers adding a bare `None` into a route set via `"route" in entity`.
        df = self._read()
        built, _ = alerts.build_v3_alerts(df, window=FULL_HISTORY_WINDOW)
        entities = built["293631"]["attributes"]["informed_entity"]
        self.assertEqual(len(entities), 2)
        for entity in entities:
            self.assertNotIn("route", entity)
            self.assertIn("stop", entity)

    def test_informed_entity_activities_split_from_pipe_delimited_string(self):
        df = self._read()
        built, _ = alerts.build_v3_alerts(df, window=FULL_HISTORY_WINDOW)
        entity = built["332786"]["attributes"]["informed_entity"][0]
        self.assertEqual(entity["activities"], ["BOARD", "EXIT", "RIDE"])

    def test_duplicate_entity_rows_across_versions_are_deduplicated(self):
        # 1028254 has 5 versions each repeating the same 20 informed entities
        # (100 raw rows total) -- we should end up with exactly the 20 distinct ones.
        df = self._read()
        built, _ = alerts.build_v3_alerts(df, window=FULL_HISTORY_WINDOW)
        self.assertEqual(len(built["1028254"]["attributes"]["informed_entity"]), 20)

    def test_service_dates_for_rolls_over_at_3am_eastern(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        et = ZoneInfo("US/Eastern")
        # A period starting 04:31 ET is on-service that same calendar day.
        ts = datetime(2020, 1, 2, 4, 31, 31, tzinfo=et).timestamp()
        self.assertEqual(list(alerts.service_dates_for(ts, ts)), [date(2020, 1, 2)])
        # But 01:00 ET is still yesterday's service date.
        early_ts = datetime(2020, 1, 2, 1, 0, 0, tzinfo=et).timestamp()
        self.assertEqual(list(alerts.service_dates_for(early_ts, early_ts)), [date(2020, 1, 1)])

    def test_iso_formats_dst_offsets_correctly(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        et = ZoneInfo("US/Eastern")
        winter = datetime(2026, 1, 15, 12, 0, tzinfo=et).timestamp()
        summer = datetime(2026, 7, 15, 12, 0, tzinfo=et).timestamp()
        self.assertTrue(alerts._iso(winter).endswith("-05:00"))
        self.assertTrue(alerts._iso(summer).endswith("-04:00"))

    def test_build_v3_alerts_clips_day_index_to_window(self):
        # 293631's true active period runs 2019-02-01 through its 2023-07-19 close,
        # but a caller-supplied window should only get day files it actually owns.
        df = self._read()
        window = (date(2023, 1, 1), date(2023, 12, 31))
        _, day_index = alerts.build_v3_alerts(df, window=window)
        self.assertTrue(all(window[0] <= d <= window[1] for d in day_index))
        self.assertIn(date(2023, 7, 1), day_index)  # 293631 still open at this point
        self.assertIn("293631", day_index[date(2023, 7, 1)])

    def test_window_end_aligns_with_service_date_rollover_not_midnight(self):
        # Regression: an alert starting between midnight and 3am ET the day after
        # window[1] belongs, by service date, to window[1] itself -- it must not
        # be excluded just because its wall-clock start falls on the next calendar day.
        start = datetime(2020, 1, 2, 1, 0, 0, tzinfo=ET).timestamp()  # service date 2020-01-01
        end = datetime(2020, 1, 2, 1, 30, 0, tzinfo=ET).timestamp()
        row = {
            "id": 999001,
            "cause": "UNKNOWN_CAUSE",
            "effect_detail": "DELAY",
            "severity": 3,
            "alert_lifecycle": "NEW",
            "header_text.translation.text": "Boundary test alert",
            "service_effect_text.translation.text": "Test",
            "created_timestamp": int(start),
            "last_modified_timestamp": int(start),
            "closed_timestamp": None,
            "active_period.start_timestamp": int(start),
            "active_period.end_timestamp": int(end),
            "informed_entity.route_id": "Red",
            "informed_entity.route_type": 1,
            "informed_entity.direction_id": None,
            "informed_entity.stop_id": None,
            "informed_entity.activities": "BOARD|EXIT|RIDE",
        }
        path = _write_alerts_parquet([row])
        try:
            last_seen = alerts.last_seen_by_alert(path)
            df = alerts.read_latest_versions(path, (date(2020, 1, 1), date(2020, 1, 1)), last_seen)
            self.assertEqual(set(df.id.unique()), {999001})
            _, day_index = alerts.build_v3_alerts(df, window=(date(2020, 1, 1), date(2020, 1, 1)))
            self.assertIn("999001", day_index[date(2020, 1, 1)])
        finally:
            os.remove(path)

    def test_eff_end_is_clamped_to_active_period_start(self):
        # Regression: a pre-announced alert (created long before an active period
        # that starts later, never revised or closed) would otherwise have
        # eff_end == last_modified_timestamp, which precedes active_period.start.
        # That can make the window filter reject the row, or make
        # service_dates_for's day range come out empty. Both must be guarded against.
        created = datetime(2020, 1, 1, 12, 0, 0, tzinfo=ET).timestamp()
        start = datetime(2020, 6, 1, 12, 0, 0, tzinfo=ET).timestamp()
        row = {
            "id": 999002,
            "cause": "UNKNOWN_CAUSE",
            "effect_detail": "DETOUR",
            "severity": 3,
            "alert_lifecycle": "UPCOMING",
            "header_text.translation.text": "Pre-announced alert",
            "service_effect_text.translation.text": "Test",
            "created_timestamp": int(created),
            "last_modified_timestamp": int(created),  # never revised since creation
            "closed_timestamp": None,
            "active_period.start_timestamp": int(start),
            "active_period.end_timestamp": None,
            "informed_entity.route_id": "1",
            "informed_entity.route_type": 3,
            "informed_entity.direction_id": None,
            "informed_entity.stop_id": None,
            "informed_entity.activities": "BOARD|EXIT|RIDE",
        }
        path = _write_alerts_parquet([row])
        try:
            last_seen = alerts.last_seen_by_alert(path)
            window = (date(2020, 6, 1), date(2020, 6, 1))
            df = alerts.read_latest_versions(path, window, last_seen)
            self.assertEqual(set(df.id.unique()), {999002})
            self.assertGreaterEqual(df.iloc[0]["eff_end"], df.iloc[0]["active_period.start_timestamp"])
            _, day_index = alerts.build_v3_alerts(df, window=window)
            self.assertIn("999002", day_index[date(2020, 6, 1)])
        finally:
            os.remove(path)

    def test_select_winning_versions_is_reusable_across_calls(self):
        # backfill/alerts.py computes this once and passes it into every per-year
        # read_latest_versions call rather than recomputing it each time.
        winners = alerts.select_winning_versions(SAMPLE_ALERTS_PATH)
        df = alerts.read_latest_versions(SAMPLE_ALERTS_PATH, FULL_HISTORY_WINDOW, self.last_seen, winners=winners)
        self.assertEqual(set(df.id.unique()), {293631, 332786, 351440, 691095, 1028254, 1030402})

    def test_upload_day_alerts_writes_expected_bucket_and_key(self):
        alerts_by_id = {"1": {"id": "1", "type": "alert", "attributes": {}}}
        day_index = {date(2026, 9, 8): ["1"]}
        with mock.patch("chalicelib.lamp.alerts.s3.upload") as mock_upload:
            result = alerts.upload_day_alerts(date(2026, 9, 8), alerts_by_id, day_index)
        self.assertEqual(result, [date(2026, 9, 8)])
        mock_upload.assert_called_once()
        call_args = mock_upload.call_args
        self.assertEqual(call_args[0][0], "tm-mbta-performance")
        self.assertEqual(call_args[0][1], "Alerts/lamp/2026-09-08.json.gz")
        self.assertTrue(call_args[1].get("compress", True))


if __name__ == "__main__":
    unittest.main()
