import unittest
from datetime import date
from decimal import Decimal
from unittest import mock

import pandas as pd

from .. import daily_metrics
from ..constants import METERS_PER_MILE


def _traversals() -> pd.DataFrame:
    return pd.DataFrame(
        [
            # Route 1, trip A: two segments (direction doesn't matter -- both are combined).
            {
                "route_id": "1",
                "service_date": date(2026, 9, 3),
                "trip_id": "A",
                "total_time_seconds": 60.0,
                "is_interpolated": False,
                "segment_length_m": 500.0,
                "speed_mph": 18.64,
            },
            {
                "route_id": "1",
                "service_date": date(2026, 9, 3),
                "trip_id": "A",
                "total_time_seconds": 90.0,
                "is_interpolated": True,
                "segment_length_m": 400.0,
                "speed_mph": 9.95,
            },
            # Route 1, trip B: one segment.
            {
                "route_id": "1",
                "service_date": date(2026, 9, 3),
                "trip_id": "B",
                "total_time_seconds": 30.0,
                "is_interpolated": False,
                "segment_length_m": 200.0,
                "speed_mph": 14.91,
            },
            # A different route, same day -- must not be folded into route 1's row.
            {
                "route_id": "2",
                "service_date": date(2026, 9, 3),
                "trip_id": "C",
                "total_time_seconds": 45.0,
                "is_interpolated": False,
                "segment_length_m": 300.0,
                "speed_mph": 14.91,
            },
        ]
    )


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


class TestPrepareDynamoItems(unittest.TestCase):
    def test_converts_numbers_to_decimal_and_date_to_iso_string(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())

        items = daily_metrics.prepare_dynamo_items(daily)
        route_1 = next(item for item in items if item["route"] == "1")

        self.assertEqual(route_1["date"], "2026-09-03")
        for field in ("count", "n_traversals", "n_interpolated", "miles_covered", "total_time"):
            self.assertIsInstance(route_1[field], Decimal, field)

    def test_a_date_already_stored_as_a_string_passes_through_unchanged(self):
        daily = daily_metrics.build_daily_route_metrics(_traversals())
        daily["date"] = daily["date"].astype(str)

        items = daily_metrics.prepare_dynamo_items(daily)

        self.assertTrue(all(item["date"] == "2026-09-03" for item in items))


class TestWriteDailyRouteMetrics(unittest.TestCase):
    def test_batch_writes_one_item_per_route_to_the_bus_table(self):
        with mock.patch.object(daily_metrics.dynamo, "dynamo_batch_write") as write:
            written = daily_metrics.write_daily_route_metrics(_traversals())

        self.assertEqual(written, 2)
        write.assert_called_once()
        items, table_name = write.call_args[0]
        self.assertEqual(table_name, "DeliveredTripMetricsBus")
        self.assertEqual(len(items), 2)


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
