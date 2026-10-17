import copy
import unittest
from datetime import date
from decimal import Decimal
from unittest import mock

import numpy as np
import pandas as pd
from boto3.dynamodb.types import TypeSerializer

from .. import daily_metrics
from ..constants import METERS_PER_MILE

SUMMABLE_FIELDS = ("count", "n_traversals", "n_interpolated", "miles_covered", "total_time")


def _traversals() -> pd.DataFrame:
    return pd.DataFrame(
        [
            # Route 1, trip A: two segments (direction doesn't matter -- both are combined).
            # The first departs at 8:50 (am_peak) and the second at 9:05 (midday), so trip A
            # crosses a band boundary.
            {
                "route_id": "1",
                "service_date": date(2026, 9, 3),
                "day_type": "business_day",
                "trip_id": "A",
                "depart_seconds": 8 * 3600 + 50 * 60,
                "total_time_seconds": 60.0,
                "is_interpolated": False,
                "segment_length_m": 500.0,
                "speed_mph": 18.64,
            },
            {
                "route_id": "1",
                "service_date": date(2026, 9, 3),
                "day_type": "business_day",
                "trip_id": "A",
                "depart_seconds": 9 * 3600 + 5 * 60,
                "total_time_seconds": 90.0,
                "is_interpolated": True,
                "segment_length_m": 400.0,
                "speed_mph": 9.95,
            },
            # Route 1, trip B: one segment, midday.
            {
                "route_id": "1",
                "service_date": date(2026, 9, 3),
                "day_type": "business_day",
                "trip_id": "B",
                "depart_seconds": 12 * 3600,
                "total_time_seconds": 30.0,
                "is_interpolated": False,
                "segment_length_m": 200.0,
                "speed_mph": 14.91,
            },
            # A different route, same day -- must not be folded into route 1's row.
            {
                "route_id": "2",
                "service_date": date(2026, 9, 3),
                "day_type": "business_day",
                "trip_id": "C",
                "depart_seconds": 17 * 3600,
                "total_time_seconds": 45.0,
                "is_interpolated": False,
                "segment_length_m": 300.0,
                "speed_mph": 14.91,
            },
        ]
    )


def _route(daily: pd.DataFrame, route: str) -> pd.Series:
    return daily[daily.route == route].iloc[0]


class TestBuildDailyRouteMetrics(unittest.TestCase):
    def test_one_row_per_route_and_date(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())

        self.assertEqual(len(daily), 2)
        self.assertEqual(sorted(daily.route), ["1", "2"])

    def test_counts_distinct_trips_not_raw_traversals(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())
        route_1 = daily[daily.route == "1"].iloc[0]

        self.assertEqual(route_1["count"], 2)
        self.assertEqual(route_1.n_traversals, 3)
        self.assertEqual(route_1.n_interpolated, 1)

    def test_miles_and_time_are_summed_from_observed_traversals(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())
        route_1 = daily[daily.route == "1"].iloc[0]

        self.assertAlmostEqual(route_1.miles_covered, (500.0 + 400.0 + 200.0) / METERS_PER_MILE)
        self.assertEqual(route_1.total_time, 180.0)

    def test_median_and_mean_speed_are_taken_across_traversals(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())
        route_1 = daily[daily.route == "1"].iloc[0]

        self.assertAlmostEqual(route_1.median_speed_mph, 14.91, places=2)
        self.assertAlmostEqual(route_1.mean_speed_mph, (18.64 + 9.95 + 14.91) / 3, places=2)

    def test_routes_do_not_leak_into_each_other(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())
        route_2 = daily[daily.route == "2"].iloc[0]

        self.assertEqual(route_2["count"], 1)
        self.assertEqual(route_2.total_time, 45.0)

    def test_day_type_is_carried_on_each_row(self):
        traversals = _traversals()
        traversals.loc[traversals.route_id == "2", "day_type"] = "weekend_or_holiday"

        daily = daily_metrics.build_daily_route_metrics(traversals)

        self.assertEqual(_route(daily, "1").day_type, "business_day")
        self.assertEqual(_route(daily, "2").day_type, "weekend_or_holiday")


class TestTimeBands(unittest.TestCase):
    def test_only_bands_with_traversals_are_present(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())

        self.assertEqual(set(_route(daily, "1").time_bands), {"am_peak", "midday"})
        self.assertEqual(set(_route(daily, "2").time_bands), {"pm_peak"})
        for totals in _route(daily, "1").time_bands.values():
            self.assertEqual(set(totals), set(SUMMABLE_FIELDS))

    def test_band_totals_come_from_the_traversals_departing_in_that_band(self):
        midday = _route(daily_metrics.build_daily_route_metrics(_traversals()), "1").time_bands["midday"]

        # Trip A's second segment and trip B's only one.
        self.assertEqual(midday["n_traversals"], 2)
        self.assertEqual(midday["n_interpolated"], 1)
        self.assertAlmostEqual(midday["miles_covered"], (400.0 + 200.0) / METERS_PER_MILE)
        self.assertEqual(midday["total_time"], 120.0)

    def test_a_trip_crossing_a_band_boundary_counts_in_both_bands(self):
        route_1 = _route(daily_metrics.build_daily_route_metrics(_traversals()), "1")

        self.assertEqual(route_1.time_bands["am_peak"]["count"], 1)
        self.assertEqual(route_1.time_bands["midday"]["count"], 2)
        # So band counts are not a partition of the day's trips.
        self.assertEqual(route_1["count"], 2)
        self.assertGreater(sum(band["count"] for band in route_1.time_bands.values()), route_1["count"])

    def test_band_totals_add_up_to_the_top_level_when_every_traversal_is_banded(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())

        for route in ("1", "2"):
            row = _route(daily, route)
            for field in ("n_traversals", "n_interpolated", "miles_covered", "total_time"):
                self.assertAlmostEqual(sum(band[field] for band in row.time_bands.values()), row[field], msg=field)

    def test_a_traversal_outside_every_band_is_only_in_the_top_level(self):
        # 33h into the service date is past late_night's end: assign_time_band gives it no band.
        traversals = _traversals()
        stray = traversals.iloc[[2]].assign(trip_id="D", depart_seconds=33 * 3600, total_time_seconds=100.0)
        traversals = pd.concat([traversals, stray], ignore_index=True)

        row = _route(daily_metrics.build_daily_route_metrics(traversals), "1")

        self.assertEqual(row.total_time, 60.0 + 90.0 + 30.0 + 100.0)
        self.assertEqual(sum(band["total_time"] for band in row.time_bands.values()), 60.0 + 90.0 + 30.0)
        self.assertEqual(row.n_traversals - sum(band["n_traversals"] for band in row.time_bands.values()), 1)


class TestPrepareDynamoItems(unittest.TestCase):
    def test_converts_numbers_to_decimal_and_date_to_iso_string(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())

        items = daily_metrics.prepare_dynamo_items(daily)
        route_1 = next(item for item in items if item["route"] == "1")

        self.assertEqual(route_1["date"], "2026-09-03")
        for field in SUMMABLE_FIELDS:
            self.assertIsInstance(route_1[field], Decimal, field)

    def test_day_type_and_time_bands_are_on_the_item(self):
        items = daily_metrics.prepare_dynamo_items(daily_metrics.build_daily_route_metrics(_traversals()))
        route_1 = next(item for item in items if item["route"] == "1")

        self.assertEqual(route_1["day_type"], "business_day")
        self.assertEqual(set(route_1["time_bands"]), {"am_peak", "midday"})
        self.assertEqual(
            route_1["time_bands"]["midday"],
            {
                "count": Decimal(2),
                "n_traversals": Decimal(2),
                "n_interpolated": Decimal(1),
                "miles_covered": Decimal(str(round(600.0 / METERS_PER_MILE, 3))),
                "total_time": Decimal("120.0"),
            },
        )

    def test_every_nested_value_is_a_type_dynamodb_accepts(self):
        items = daily_metrics.prepare_dynamo_items(daily_metrics.build_daily_route_metrics(_traversals()))

        for item in items:
            for band in item["time_bands"].values():
                for field, value in band.items():
                    self.assertIsInstance(value, Decimal, field)
            # boto3's own serializer is what update_item runs values through.
            TypeSerializer().serialize(item)

    def test_the_serializer_check_would_catch_a_float_or_numpy_value(self):
        # Guards the test above: boto3 rejects these, so a leaked float would fail there.
        for bad in (1.5, np.int64(3), np.float64(2.5)):
            with self.assertRaises(TypeError):
                TypeSerializer().serialize({"time_bands": {"am_peak": {"total_time": bad}}})

    def test_a_date_already_stored_as_a_string_passes_through_unchanged(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())
        daily["date"] = daily["date"].astype(str)

        items = daily_metrics.prepare_dynamo_items(daily)

        self.assertTrue(all(item["date"] == "2026-09-03" for item in items))


class TestWriteDailyRouteMetrics(unittest.TestCase):
    def test_upserts_one_item_per_route_to_the_bus_table(self):
        with mock.patch.object(daily_metrics.dynamo, "dynamo_update_items") as write:
            written = daily_metrics.write_daily_route_metrics(_traversals())

        self.assertEqual(written, 2)
        write.assert_called_once()
        items, table_name = write.call_args[0]
        self.assertEqual(table_name, "DeliveredTripMetricsBus")
        self.assertEqual(len(items), 2)


class _FakeTable:
    """Applies `SET #fN = :vN, ...` UpdateItem calls to in-memory rows, the way DynamoDB does.

    Values go through boto3's TypeSerializer first, so anything DynamoDB would reject (a
    float, a numpy scalar) fails here too.
    """

    def __init__(self, rows: dict):
        self.rows = rows

    def update_item(self, Key, UpdateExpression, ExpressionAttributeNames, ExpressionAttributeValues):
        assert UpdateExpression.startswith("SET "), UpdateExpression
        row = self.rows.setdefault((Key["route"], Key["date"]), dict(Key))
        for assignment in UpdateExpression.removeprefix("SET ").split(", "):
            name, value = assignment.split(" = ")
            TypeSerializer().serialize(ExpressionAttributeValues[value])
            row[ExpressionAttributeNames[name]] = copy.deepcopy(ExpressionAttributeValues[value])


class TestUpsertLeavesOtherFieldsAlone(unittest.TestCase):
    """Runs the real dynamo.dynamo_update_items against a fake table seeded like production:
    fleet stats from data-ingestion on the same row, and a system-wide route="all" row."""

    FLEET_FIELDS = {
        "avg_bus_age": Decimal("9.7"),
        "fleet_mix_battery": Decimal(0),
        "fleet_mix_cng": Decimal("18.7"),
        "fleet_mix_diesel": Decimal(0),
        "fleet_mix_hybrid": Decimal("81.3"),
        "fleet_trips": Decimal(214),
        "pct_battery_trips": Decimal(0),
    }

    def test_sets_speed_fields_and_leaves_fleet_fields_and_other_rows_untouched(self):
        rows = {
            ("1", "2026-09-03"): {
                "route": "1",
                "date": "2026-09-03",
                **self.FLEET_FIELDS,
                # From an earlier run, including a band this run no longer has.
                "time_bands": {"late_night": {"count": Decimal(9)}},
                "count": Decimal(999),
            },
            ("all", "2026-09-03"): {"route": "all", "date": "2026-09-03", **self.FLEET_FIELDS},
        }
        before_all_row = copy.deepcopy(rows[("all", "2026-09-03")])
        resource = mock.MagicMock()
        resource.Table.return_value = _FakeTable(rows)

        with mock.patch.object(daily_metrics.dynamo, "dynamodb", resource):
            daily_metrics.write_daily_route_metrics(_traversals())

        route_1 = rows[("1", "2026-09-03")]
        for field, value in self.FLEET_FIELDS.items():
            self.assertEqual(route_1[field], value, field)
        self.assertEqual((route_1["route"], route_1["date"]), ("1", "2026-09-03"))
        self.assertEqual(route_1["count"], Decimal(2))
        self.assertEqual(route_1["day_type"], "business_day")
        # The whole map is replaced, so the stale late_night band is gone.
        self.assertEqual(set(route_1["time_bands"]), {"am_peak", "midday"})
        self.assertEqual(rows[("all", "2026-09-03")], before_all_row)
        # Route 2 had no row yet: the upsert creates it.
        self.assertEqual(rows[("2", "2026-09-03")]["day_type"], "business_day")


class TestIngestPassesDayType(unittest.TestCase):
    def _traversals_and_segments(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        traversals = _traversals().drop(columns=["day_type"])
        traversals = traversals.assign(
            direction_id=0,
            route_pattern_id=traversals.route_id + "-0",
            from_stop_id="s1",
            to_stop_id="s2",
            moving_time_seconds=traversals.total_time_seconds,
            dwell_seconds=0.0,
        )
        segments = pd.DataFrame(
            [
                {
                    "route_pattern_id": f"{route}-0",
                    "route_id": route,
                    "direction_id": 0,
                    "from_stop_id": "s1",
                    "to_stop_id": "s2",
                    "from_stop_name": "A",
                    "to_stop_name": "B",
                    "coordinates": [(-71.05, 42.36), (-71.06, 42.37)],
                }
                for route in ("1", "2")
            ]
        )
        return traversals, segments

    def test_dynamo_rows_get_day_type_but_the_segment_frame_does_not(self):
        from .. import ingest

        traversals, segments = self._traversals_and_segments()
        with (
            mock.patch.object(ingest, "build_traversals_for_date", return_value=(traversals, segments)),
            mock.patch.object(ingest, "day_type_for", return_value="weekend_or_holiday") as day_type_for,
            mock.patch.object(ingest, "write_daily_route_metrics") as write,
        ):
            result = ingest.generate_speed_segments(date(2026, 9, 7), write_to_dynamo=True)

        day_type_for.assert_called_once_with(date(2026, 9, 7))
        written = write.call_args[0][0]
        self.assertEqual(set(written.day_type), {"weekend_or_holiday"})
        # day_type would otherwise leak into the daily PMTiles via TILE_PROPERTIES.
        self.assertNotIn("day_type", result.columns)
        self.assertNotIn("day_type", traversals.columns)

    def test_day_type_is_not_looked_up_without_write_to_dynamo(self):
        from .. import ingest

        with (
            mock.patch.object(ingest, "build_traversals_for_date", return_value=self._traversals_and_segments()),
            mock.patch.object(ingest, "day_type_for") as day_type_for,
        ):
            ingest.generate_speed_segments(date(2026, 9, 7))

        day_type_for.assert_not_called()


class TestWriteToDynamoIsOptIn(unittest.TestCase):
    def test_generate_does_not_write_to_dynamo_unless_asked(self):
        from .. import ingest

        with (
            mock.patch.object(ingest, "read_service_date") as read,
            mock.patch.object(ingest, "write_daily_route_metrics") as write,
            mock.patch.object(ingest, "build_pattern_geometry"),
        ):
            read.return_value = pd.DataFrame()
            with self.assertRaises(ValueError):
                ingest.generate_speed_segments(date(2026, 9, 3))

        write.assert_not_called()
