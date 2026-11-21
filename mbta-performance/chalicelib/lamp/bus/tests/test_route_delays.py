import json
import unittest
from datetime import date

import pandas as pd

from .. import route_delays
from ..constants import ALL_DAY_BAND

# Three 400m segments in a row. Every test's quiet-hour trips run the first two, so those have a
# free flow; C -> D has one only where a test adds quiet-hour trips over it.
STOPS = ["A", "B", "C", "D"]


def _trip(
    trip_id, depart_hour, running_times, route_id="1", direction_id=0, vehicle="y1", stops=STOPS, **overrides
) -> list[dict]:
    """A trip's traversals, from the first of `stops` onward, one per running time, a minute apart."""
    return [
        {
            "route_id": route_id,
            "direction_id": direction_id,
            "service_date": date(2026, 11, 4),
            "trip_id": trip_id,
            "vehicle_label": vehicle,
            "from_stop_id": stops[index],
            "to_stop_id": stops[index + 1],
            "depart_seconds": depart_hour * 3600 + index * 60,
            "running_time_seconds": running_time,
            "segment_length_m": 400.0,
            **overrides,
        }
        for index, running_time in enumerate(running_times)
    ]


def _quiet_trips(count=10, running_times=(60.0, 60.0), **overrides) -> list[dict]:
    """Late-night trips, enough to give their segments a free flow of 60s each."""
    return [row for n in range(count) for row in _trip(f"night-{n}", 23, running_times, **overrides)]


def _pm_trips(running_times_per_trip, **overrides) -> list[dict]:
    return [row for n, times in enumerate(running_times_per_trip) for row in _trip(f"pm-{n}", 17, times, **overrides)]


def _entry(result, band, route_id="1", direction_id=0):
    return next(
        entry for entry in result[band] if entry["route_id"] == route_id and entry["direction_id"] == direction_id
    )


FIVE_PM_TRIPS = [(100.0, 100.0), (110.0, 110.0), (120.0, 120.0), (130.0, 130.0), (140.0, 140.0)]


class TestBuildRouteDelaysDaily(unittest.TestCase):
    def test_median_trip_against_its_free_flow(self):
        traversals = pd.DataFrame(_quiet_trips() + _pm_trips(FIVE_PM_TRIPS))

        result = route_delays.build_route_delays(traversals)

        self.assertEqual(
            _entry(result, "pm_peak"),
            {
                "route_id": "1",
                "direction_id": 0,
                "n_trips": 5,
                "p50_running_time_seconds": 240,
                "free_flow_running_time_seconds": 120,
            },
        )
        # The quiet-hour trips are late_night trips too, at free flow.
        self.assertEqual(_entry(result, "late_night")["p50_running_time_seconds"], 120)

    def test_all_day_takes_every_band_trip(self):
        traversals = pd.DataFrame(_quiet_trips() + _pm_trips(FIVE_PM_TRIPS))

        result = route_delays.build_route_delays(traversals)

        self.assertEqual(_entry(result, ALL_DAY_BAND)["n_trips"], 15)

    def test_the_result_is_json_safe(self):
        traversals = pd.DataFrame(_quiet_trips() + _pm_trips(FIVE_PM_TRIPS))

        result = route_delays.build_route_delays(traversals)

        json.dumps(result)

    def test_too_few_trips_leave_a_band_out(self):
        traversals = pd.DataFrame(_quiet_trips() + _pm_trips(FIVE_PM_TRIPS[:4]))

        result = route_delays.build_route_delays(traversals)

        self.assertNotIn("pm_peak", result)

    def test_a_trip_takes_the_band_it_departed_in(self):
        # Leaves A at 15:58 and B at 16:00: midday all the way, though it runs into the PM peak.
        midday = [row for n in range(5) for row in _trip(f"midday-{n}", 15 + 58 / 60, (100.0, 100.0))]
        for row in midday[1::2]:
            row["depart_seconds"] = 16 * 3600 + 30
        traversals = pd.DataFrame(_quiet_trips() + midday)

        result = route_delays.build_route_delays(traversals)

        self.assertEqual(_entry(result, "midday")["n_trips"], 5)
        self.assertNotIn("pm_peak", result)

    def test_a_trip_mostly_without_free_flow_isnt_counted(self):
        # C -> D has no quiet-hour trips, so these trips have a free flow over 2 of 3 segments.
        traversals = pd.DataFrame(_quiet_trips() + _pm_trips([(100.0, 100.0, 100.0)] * 5))

        result = route_delays.build_route_delays(traversals)

        self.assertNotIn("pm_peak", result)

    def test_a_trip_counts_only_the_segments_with_free_flow(self):
        # Ten segments, one with no usable running time: covered over 90% of the trip, so counted,
        # and timed over the other nine on both clocks.
        stops = [f"s{n}" for n in range(11)]
        traversals = pd.DataFrame(
            _quiet_trips(running_times=[60.0] * 10, stops=stops) + _pm_trips([[100.0] * 9 + [None]] * 5, stops=stops)
        )

        entry = _entry(route_delays.build_route_delays(traversals), "pm_peak")

        self.assertEqual(entry["p50_running_time_seconds"], 900)
        self.assertEqual(entry["free_flow_running_time_seconds"], 540)

    def test_a_trip_handed_to_another_bus_is_two_pieces(self):
        # A to B on one bus, then B to C on another.
        handed_over = [
            row
            for n in range(5)
            for row in _trip(f"pm-{n}", 17, (100.0,))
            + _trip(f"pm-{n}", 17.1, (100.0,), vehicle="y2", from_stop_id="B", to_stop_id="C")
        ]
        traversals = pd.DataFrame(_quiet_trips() + handed_over)

        result = route_delays.build_route_delays(traversals)

        self.assertEqual(_entry(result, "pm_peak")["n_trips"], 10)

    def test_routes_and_directions_are_kept_apart(self):
        traversals = pd.DataFrame(
            _quiet_trips()
            + _quiet_trips(direction_id=1, vehicle="y2")
            + _pm_trips(FIVE_PM_TRIPS)
            + _pm_trips([(200.0, 200.0)] * 5, direction_id=1, vehicle="y2")
        )

        result = route_delays.build_route_delays(traversals)

        self.assertEqual(_entry(result, "pm_peak", direction_id=0)["p50_running_time_seconds"], 240)
        self.assertEqual(_entry(result, "pm_peak", direction_id=1)["p50_running_time_seconds"], 400)


class TestBuildRouteDelaysByDayType(unittest.TestCase):
    def test_weekly_and_monthly_files_are_keyed_by_day_type_first(self):
        weekday = _quiet_trips(day_type="business_day") + _pm_trips(FIVE_PM_TRIPS, day_type="business_day")
        weekend = _pm_trips([(70.0, 70.0)] * 5, day_type="weekend_or_holiday", service_date=date(2026, 11, 7))
        traversals = pd.DataFrame(weekday + weekend)

        result = route_delays.build_route_delays(traversals)

        self.assertEqual(set(result), {"business_day", "weekend_or_holiday"})
        self.assertEqual(_entry(result["business_day"], "pm_peak")["p50_running_time_seconds"], 240)
        # Free flow is taken across every day type, so the weekend has one without quiet trips.
        self.assertEqual(
            _entry(result["weekend_or_holiday"], "pm_peak"),
            {
                "route_id": "1",
                "direction_id": 0,
                "n_trips": 5,
                "p50_running_time_seconds": 140,
                "free_flow_running_time_seconds": 120,
            },
        )


if __name__ == "__main__":
    unittest.main()
