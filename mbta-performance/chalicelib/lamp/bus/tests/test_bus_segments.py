import unittest

import numpy as np
import pandas as pd
from shapely.geometry import LineString

from .. import geoparquet, gtfs_geo, segments


class TestProjection(unittest.TestCase):
    """Locating stops along a shape, including shapes that double back on themselves."""

    def test_projects_onto_a_straight_shape(self):
        shape = np.array([[0.0, 0.0], [100.0, 0.0], [200.0, 0.0], [300.0, 0.0]])
        stops = np.array([[10.0, 5.0], [150.0, -5.0], [290.0, 0.0]])

        distances, offsets = gtfs_geo.project_stops_onto_shape(stops, shape)

        np.testing.assert_allclose(distances, [10.0, 150.0, 290.0], atol=1e-6)
        np.testing.assert_allclose(offsets, [5.0, 5.0, 0.0], atol=1e-6)

    def test_out_and_back_shape_assigns_stops_to_the_correct_pass(self):
        # Out along y=0, back along y=20. A stop on the return leg sits near both passes.
        shape = np.array([[0.0, 0.0], [300.0, 0.0], [300.0, 20.0], [0.0, 20.0]])
        stops = np.array([[150.0, 1.0], [150.0, 19.0]])

        distances, _ = gtfs_geo.project_stops_onto_shape(stops, shape)

        self.assertAlmostEqual(distances[0], 150.0, places=4)
        # 300 out + 20 across + 150 back down the return leg.
        self.assertAlmostEqual(distances[1], 470.0, places=4)

    def test_tight_out_and_back_orders_every_stop_correctly(self):
        # The return leg runs 20m from the outbound leg, and stops sit on opposite sides of
        # the street. Picking each stop's nearest point independently cannot order these;
        # this is the shape of the loop routes (216, 220, 112) in the real feed.
        shape = np.array([[0.0, 0.0], [300.0, 0.0], [300.0, 20.0], [0.0, 20.0]])
        stops = np.array([[50.0, 8.0], [150.0, 8.0], [250.0, 8.0], [250.0, 12.0], [150.0, 12.0], [50.0, 12.0]])

        distances, _ = gtfs_geo.project_stops_onto_shape(stops, shape)

        self.assertTrue(np.all(np.diff(distances) > 0), f"expected strictly increasing distances, got {distances}")
        # Outbound stops keep their own distances; return stops land past the 320m turn.
        np.testing.assert_allclose(distances[:3], [50.0, 150.0, 250.0], atol=1e-6)
        np.testing.assert_allclose(distances[3:], [370.0, 470.0, 570.0], atol=1e-6)


class TestCutSegment(unittest.TestCase):
    def test_cuts_between_two_distances_and_interpolates_the_ends(self):
        line = LineString([(0.0, 0.0), (100.0, 0.0), (200.0, 0.0), (300.0, 0.0)])

        cut = gtfs_geo.cut_segment(line, 50.0, 250.0)

        self.assertAlmostEqual(cut.coords[0][0], 50.0, places=6)
        self.assertAlmostEqual(cut.coords[-1][0], 250.0, places=6)
        # The interior vertices at 100 and 200 are retained between the two cuts.
        self.assertEqual(len(cut.coords), 4)

    def test_returns_none_for_a_non_advancing_slice(self):
        line = LineString([(0.0, 0.0), (100.0, 0.0)])

        self.assertIsNone(gtfs_geo.cut_segment(line, 50.0, 50.0))

    def test_returns_none_for_a_degenerate_zero_length_line(self):
        # A shape with a duplicated point has nothing to cut, even with an advancing range.
        line = LineString([(0.0, 0.0), (0.0, 0.0)])

        self.assertIsNone(gtfs_geo.cut_segment(line, 0.0, 10.0))


def _events(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame["tm_pullout_id"] = frame.get("tm_pullout_id", "pullout")
    return frame


class TestInterpolation(unittest.TestCase):
    def test_interior_gap_is_filled_in_proportion_to_distance(self):
        events = _events(
            [
                {
                    "trip_id": "t1",
                    "stop_sequence": 1,
                    "shape_distance_m": 0.0,
                    "stop_arrival_seconds": 90.0,
                    "stop_departure_seconds": 100.0,
                },
                {
                    "trip_id": "t1",
                    "stop_sequence": 2,
                    "shape_distance_m": 300.0,
                    "stop_arrival_seconds": np.nan,
                    "stop_departure_seconds": np.nan,
                },
                {
                    "trip_id": "t1",
                    "stop_sequence": 3,
                    "shape_distance_m": 400.0,
                    "stop_arrival_seconds": 200.0,
                    "stop_departure_seconds": 210.0,
                },
            ]
        )

        result = segments.interpolate_missing_times(events)
        middle = result[result.stop_sequence == 2].iloc[0]

        # 300/400 of the way along, across a 100s gap from departure(100) to arrival(200).
        self.assertAlmostEqual(middle.stop_arrival_seconds, 175.0, places=6)
        # A stop the bus was never recorded at is passed through, so no dwell is invented.
        self.assertAlmostEqual(middle.stop_departure_seconds, 175.0, places=6)
        self.assertTrue(bool(middle.is_interpolated))

    def test_leading_and_trailing_gaps_are_left_alone(self):
        events = _events(
            [
                {
                    "trip_id": "t1",
                    "stop_sequence": 1,
                    "shape_distance_m": 0.0,
                    "stop_arrival_seconds": np.nan,
                    "stop_departure_seconds": np.nan,
                },
                {
                    "trip_id": "t1",
                    "stop_sequence": 2,
                    "shape_distance_m": 100.0,
                    "stop_arrival_seconds": 100.0,
                    "stop_departure_seconds": 110.0,
                },
                {
                    "trip_id": "t1",
                    "stop_sequence": 3,
                    "shape_distance_m": 200.0,
                    "stop_arrival_seconds": 200.0,
                    "stop_departure_seconds": 210.0,
                },
                {
                    "trip_id": "t1",
                    "stop_sequence": 4,
                    "shape_distance_m": 300.0,
                    "stop_arrival_seconds": np.nan,
                    "stop_departure_seconds": np.nan,
                },
            ]
        )

        result = segments.interpolate_missing_times(events).sort_values("stop_sequence")

        self.assertTrue(pd.isna(result.iloc[0].stop_arrival_seconds))
        self.assertTrue(pd.isna(result.iloc[3].stop_arrival_seconds))
        self.assertFalse(result.is_interpolated.any())

    def test_does_not_interpolate_across_a_trip_boundary(self):
        events = _events(
            [
                {
                    "trip_id": "t1",
                    "stop_sequence": 1,
                    "shape_distance_m": 0.0,
                    "stop_arrival_seconds": 100.0,
                    "stop_departure_seconds": 100.0,
                },
                {
                    "trip_id": "t2",
                    "stop_sequence": 1,
                    "shape_distance_m": 100.0,
                    "stop_arrival_seconds": np.nan,
                    "stop_departure_seconds": np.nan,
                },
                {
                    "trip_id": "t3",
                    "stop_sequence": 1,
                    "shape_distance_m": 200.0,
                    "stop_arrival_seconds": 300.0,
                    "stop_departure_seconds": 300.0,
                },
            ]
        )

        result = segments.interpolate_missing_times(events)

        self.assertFalse(result.is_interpolated.any())


class TestTraversals(unittest.TestCase):
    def _trip(self, trip_id: str, times: list[tuple[float, float]]) -> list[dict]:
        return [
            {
                "trip_id": trip_id,
                "tm_pullout_id": trip_id,
                "route_id": "1",
                "direction_id": 0,
                "route_pattern_id": "1-0",
                "service_date": "2026-09-03",
                "vehicle_label": "v1",
                "stop_id": f"s{index + 1}",
                "stop_sequence": index + 1,
                "stop_arrival_seconds": arrival,
                "stop_departure_seconds": departure,
                "is_interpolated": False,
            }
            for index, (arrival, departure) in enumerate(times)
        ]

    def _segments(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"route_pattern_id": "1-0", "from_stop_id": "s1", "to_stop_id": "s2", "segment_length_m": 500.0},
                {"route_pattern_id": "1-0", "from_stop_id": "s2", "to_stop_id": "s3", "segment_length_m": 500.0},
            ]
        )

    def test_layover_at_the_first_stop_is_not_charged_to_the_first_segment(self):
        # 600s sitting at the terminal, then two ordinary 60s hops.
        events = pd.DataFrame(self._trip("t1", [(0.0, 600.0), (660.0, 690.0), (750.0, 760.0)]))

        traversals = segments.build_traversals(events, self._segments()).sort_values("from_stop_id")

        first, second = traversals.iloc[0], traversals.iloc[1]
        self.assertEqual(first.dwell_seconds, 0.0)
        self.assertEqual(first.total_time_seconds, 60.0)
        # The mid-route dwell is real time on board and is kept.
        self.assertEqual(second.dwell_seconds, 30.0)
        self.assertEqual(second.total_time_seconds, 90.0)

    def test_moving_time_ignores_dwell_while_total_time_includes_it(self):
        events = pd.DataFrame(self._trip("t1", [(0.0, 0.0), (660.0, 690.0), (750.0, 760.0)]))

        traversals = segments.build_traversals(events, self._segments()).sort_values("from_stop_id")
        second = traversals.iloc[1]

        self.assertEqual(second.moving_time_seconds, 60.0)
        self.assertEqual(second.total_time_seconds, 90.0)
        self.assertGreater(second.moving_speed_mph, second.speed_mph)

    def test_segments_are_not_stitched_across_two_trips(self):
        events = pd.DataFrame(
            self._trip("t1", [(0.0, 0.0), (60.0, 60.0)]) + self._trip("t2", [(500.0, 500.0), (560.0, 560.0)])
        )

        traversals = segments.build_traversals(events, self._segments())

        # Two trips of two stops each yield one segment apiece, never a t1 -> t2 join.
        self.assertEqual(len(traversals), 2)
        self.assertEqual(set(traversals.from_stop_id), {"s1"})

    def test_implausible_speeds_are_dropped(self):
        # 500m covered in 1s is roughly 1100mph.
        events = pd.DataFrame(self._trip("t1", [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)]))

        traversals = segments.build_traversals(events, self._segments())

        self.assertEqual(len(traversals), 0)


class TestTimeBands(unittest.TestCase):
    def test_assigns_bands_by_departure(self):
        departures = pd.Series([6 * 3600, 8 * 3600, 12 * 3600, 17 * 3600, 20 * 3600, 23 * 3600])

        bands = segments.assign_time_band(departures)

        self.assertEqual(list(bands), ["early_am", "am_peak", "midday", "pm_peak", "evening", "late_night"])

    def test_after_midnight_trips_stay_on_the_service_date(self):
        # 25:30 into the service date is 1:30am the next calendar morning.
        self.assertEqual(segments.assign_time_band(pd.Series([25.5 * 3600])).iloc[0], "late_night")


class TestGeoParquet(unittest.TestCase):
    def test_encodes_coordinates_as_linestring_geometry(self):
        frame = pd.DataFrame({"coordinates": [[(-71.05, 42.36), (-71.06, 42.37)]]})

        geo_frame = geoparquet._to_geodataframe(frame)

        self.assertNotIn("coordinates", geo_frame.columns)
        self.assertEqual(geo_frame.crs.to_epsg(), 4326)
        self.assertEqual(list(geo_frame.geometry.iloc[0].coords), [(-71.05, 42.36), (-71.06, 42.37)])

    def test_drops_rows_with_degenerate_geometry(self):
        frame = pd.DataFrame({"coordinates": [[(-71.05, 42.36), (-71.06, 42.37)], [(-71.05, 42.36)], [], None]})

        geo_frame = geoparquet._to_geodataframe(frame)

        self.assertEqual(len(geo_frame), 1)
