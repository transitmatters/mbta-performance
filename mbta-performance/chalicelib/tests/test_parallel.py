import unittest

import pandas as pd

from .. import parallel


class TestMakeParallel(unittest.TestCase):
    def test_calls_function_for_each_item(self):
        """Each item in the iterable should be passed to the function exactly once."""
        call_tracker = []

        def my_func(item):
            call_tracker.append(item)
            return [item]

        parallel_func = parallel.make_parallel(my_func)
        parallel_func([1, 2, 3])

        self.assertEqual(sorted(call_tracker), [1, 2, 3])

    def test_returns_flattened_results(self):
        """Results from all function calls should be collected into a single flat list."""

        def my_func(item):
            return [item * 2]

        parallel_func = parallel.make_parallel(my_func)
        result = parallel_func([1, 2, 3])

        self.assertEqual(sorted(result), [2, 4, 6])

    def test_empty_iterable(self):
        """Empty input should return an empty list without error."""
        from unittest import mock

        my_func = mock.MagicMock(return_value=[])
        parallel_func = parallel.make_parallel(my_func)
        result = parallel_func([])
        self.assertEqual(result, [])
        my_func.assert_not_called()

    def test_passes_extra_positional_and_keyword_args(self):
        """Extra *args and **kwargs should be forwarded to the underlying function."""
        collected = []

        def my_func(item, multiplier, offset=0):
            result = item * multiplier + offset
            collected.append(result)
            return [result]

        parallel_func = parallel.make_parallel(my_func)
        parallel_func([1, 2, 3], 10, offset=5)

        self.assertEqual(sorted(collected), [15, 25, 35])

    def test_multi_value_return_is_flattened(self):
        """If each call returns multiple values, they should all appear in the output list."""

        def my_func(item):
            return [item, item + 100]

        parallel_func = parallel.make_parallel(my_func)
        result = parallel_func([1, 2])

        self.assertEqual(sorted(result), [1, 2, 101, 102])


class TestDateRange(unittest.TestCase):
    def test_basic_range(self):
        result = parallel.date_range("2024-01-01", "2024-01-03")
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], pd.Timestamp("2024-01-01"))
        self.assertEqual(result[-1], pd.Timestamp("2024-01-03"))

    def test_single_day(self):
        result = parallel.date_range("2024-06-15", "2024-06-15")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], pd.Timestamp("2024-06-15"))

    def test_returns_datetimeindex(self):
        result = parallel.date_range("2024-01-01", "2024-01-05")
        self.assertIsInstance(result, pd.DatetimeIndex)


class TestMonthRange(unittest.TestCase):
    def test_includes_each_month(self):
        result = parallel.month_range("2024-01-01", "2024-03-01")
        self.assertEqual(len(result), 3)

    def test_single_month(self):
        result = parallel.month_range("2024-06-01", "2024-06-15")
        self.assertEqual(len(result), 1)

    def test_spans_year_boundary(self):
        result = parallel.month_range("2023-11-01", "2024-02-01")
        # Nov, Dec, Jan, Feb = 4 months
        self.assertEqual(len(result), 4)

    def test_start_equals_end_on_month_boundary(self):
        result = parallel.month_range("2024-03-01", "2024-03-01")
        self.assertEqual(len(result), 1)
