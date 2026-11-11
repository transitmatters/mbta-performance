import unittest
from unittest import mock

from .. import tm_benchmarks


class TestGenerateTravelTimeBenchmarks(unittest.TestCase):
    def test_all_colors_succeed(self):
        with mock.patch.object(tm_benchmarks, "generate_travel_time_benchmarks_for_color") as per_color:
            tm_benchmarks.generate_travel_time_benchmarks(("Red", "Blue"))
        self.assertEqual([c.args[0] for c in per_color.call_args_list], ["Red", "Blue"])

    def test_one_failure_still_runs_the_rest_then_raises(self):
        def per_color(color):
            if color == "Blue":
                raise RuntimeError("AccessDenied")

        with mock.patch.object(
            tm_benchmarks, "generate_travel_time_benchmarks_for_color", side_effect=per_color
        ) as mocked:
            with self.assertRaisesRegex(RuntimeError, r"1 of 3 colors: Blue$"):
                tm_benchmarks.generate_travel_time_benchmarks(("Red", "Blue", "Orange"))
        self.assertEqual([c.args[0] for c in mocked.call_args_list], ["Red", "Blue", "Orange"])

    def test_every_color_failing_raises(self):
        with mock.patch.object(
            tm_benchmarks,
            "generate_travel_time_benchmarks_for_color",
            side_effect=RuntimeError("AccessDenied"),
        ):
            with self.assertRaisesRegex(RuntimeError, r"5 of 5 colors: Red, Blue, Orange, Green, Mattapan$"):
                tm_benchmarks.generate_travel_time_benchmarks()
