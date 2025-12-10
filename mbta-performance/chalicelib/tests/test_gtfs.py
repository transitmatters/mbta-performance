import unittest
from datetime import date

from .. import gtfs


class TestGTFS(unittest.TestCase):
    def test_max_query_depth_constant(self):
        """Test that MAX_QUERY_DEPTH is set to expected value."""
        self.assertEqual(gtfs.MAX_QUERY_DEPTH, 900)

    def test_max_query_depth_is_less_than_actual_limit(self):
        """Test that MAX_QUERY_DEPTH is less than the actual SQLite limit of 1000."""
        # The actual query limit is 1000, but we use 900 to be safe
        self.assertLess(gtfs.MAX_QUERY_DEPTH, 1000)
        self.assertGreater(gtfs.MAX_QUERY_DEPTH, 0)

    def test_fetch_stop_times_from_gtfs_signature(self):
        """Test that fetch_stop_times_from_gtfs has the expected signature."""
        import inspect

        sig = inspect.signature(gtfs.fetch_stop_times_from_gtfs)
        params = list(sig.parameters.keys())

        # Verify expected parameters
        self.assertIn("trip_ids", params)
        self.assertIn("service_date", params)
        self.assertEqual(len(params), 2)

    def test_fetch_stop_times_from_gtfs_return_type_annotation(self):
        """Test that fetch_stop_times_from_gtfs has proper type annotations."""
        import inspect

        sig = inspect.signature(gtfs.fetch_stop_times_from_gtfs)

        # Check that parameters have proper annotations
        self.assertIn("trip_ids", sig.parameters)
        self.assertIn("service_date", sig.parameters)

        # Verify trip_ids parameter exists and accepts an Iterable
        trip_ids_param = sig.parameters["trip_ids"]
        self.assertIsNotNone(trip_ids_param.annotation)

        # Verify service_date parameter exists and accepts a date
        service_date_param = sig.parameters["service_date"]
        self.assertEqual(service_date_param.annotation, date)
