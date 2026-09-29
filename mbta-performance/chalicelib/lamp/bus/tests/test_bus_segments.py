import unittest
from datetime import date

import numpy as np
import pandas as pd
from shapely.geometry import LineString

from .. import geoparquet, gtfs_geo, segments
from ..constants import ALL_DAY_BAND, TIME_BANDS


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


class TestAggregateSegments(unittest.TestCase):
    def _traversals(self) -> pd.DataFrame:
        rows = []
        for service_date, times in [(date(2026, 9, 1), [100.0, 100.0]), (date(2026, 9, 2), [200.0, 200.0])]:
            for total_time in times:
                rows.append(
                    {
                        "route_id": "1",
                        "direction_id": 0,
                        "from_stop_id": "s1",
                        "to_stop_id": "s2",
                        "service_date": service_date,
                        "depart_seconds": 8 * 3600,
                        "total_time_seconds": total_time,
                        "moving_time_seconds": total_time,
                        "dwell_seconds": 0.0,
                        "is_interpolated": False,
                        "segment_length_m": 500.0,
                    }
                )
        return pd.DataFrame(rows)

    def test_default_groups_by_service_date(self):
        aggregated = segments.aggregate_segments(self._traversals())
        am_peak = aggregated[aggregated.time_band == "am_peak"]

        self.assertEqual(len(am_peak), 2)
        self.assertEqual(set(am_peak.service_date), {date(2026, 9, 1), date(2026, 9, 2)})
        # Each date also gets its own all_day row.
        self.assertEqual(len(aggregated), 4)

    def test_empty_extra_group_columns_rolls_every_date_together(self):
        # For the weekly/monthly trend rollups in trends.py: percentiles are recomputed
        # across every traversal in the period, not averaged from the per-date rows above.
        aggregated = segments.aggregate_segments(self._traversals(), extra_group_columns=())

        self.assertEqual(len(aggregated), 2)
        self.assertNotIn("service_date", aggregated.columns)
        row = aggregated[aggregated.time_band == "am_peak"].iloc[0]
        self.assertEqual(row.n_traversals, 4)
        self.assertAlmostEqual(row.p50_total_time_seconds, 150.0)


class TestAllDayBand(unittest.TestCase):
    """The all_day rows the map's "All day" filter reads (see ALL_DAY_BAND in constants.py)."""

    def _traversal(self, depart_hour: float, total_time: float, is_interpolated: bool = False) -> dict:
        return {
            "route_id": "1",
            "direction_id": 0,
            "from_stop_id": "s1",
            "to_stop_id": "s2",
            "service_date": date(2026, 9, 3),
            "depart_seconds": depart_hour * 3600,
            "total_time_seconds": total_time,
            "moving_time_seconds": total_time * 0.8,
            "dwell_seconds": total_time * 0.2,
            "is_interpolated": is_interpolated,
            "segment_length_m": 500.0,
        }

    def _traversals(self) -> pd.DataFrame:
        # Skewed on purpose: am_peak has 3 fast traversals, midday 2 slow ones. The band p50s
        # are 60 and 300, so the median of band medians (180) and the traversal-weighted mean
        # of them (156) are both wrong -- the true all-day p50 of the 5 traversals is 70.
        return pd.DataFrame(
            [
                self._traversal(8, 60.0),
                self._traversal(8, 50.0, is_interpolated=True),
                self._traversal(8.5, 70.0),
                self._traversal(12, 200.0, is_interpolated=True),
                self._traversal(13, 400.0),
            ]
        )

    def test_all_day_is_aggregated_from_every_traversal_not_from_the_band_rows(self):
        traversals = self._traversals()

        aggregated = segments.aggregate_segments(traversals)
        all_day = aggregated[aggregated.time_band == ALL_DAY_BAND]
        bands = aggregated[aggregated.time_band != ALL_DAY_BAND]

        self.assertEqual(len(all_day), 1)
        row = all_day.iloc[0]
        for percentile in (50, 90):
            for measure in ("total_time_seconds", "moving_time_seconds"):
                self.assertAlmostEqual(
                    row[f"p{percentile}_{measure}"], np.percentile(traversals[measure], percentile), msg=measure
                )
        self.assertAlmostEqual(row.p50_total_time_seconds, 70.0)
        self.assertNotAlmostEqual(row.p50_total_time_seconds, bands.p50_total_time_seconds.median())
        self.assertAlmostEqual(row.p50_speed_mph, 500.0 / 70.0 * 2.2369362920544)
        self.assertEqual(row.n_traversals, 5)
        self.assertEqual(row.n_interpolated, 2)
        self.assertAlmostEqual(row.median_dwell_seconds, np.median(traversals.dwell_seconds))

    def test_all_day_rows_carry_the_same_columns_as_band_rows(self):
        aggregated = segments.aggregate_segments(self._traversals())

        all_day = aggregated[aggregated.time_band == ALL_DAY_BAND]
        self.assertFalse(all_day.drop(columns=["time_band"]).isna().any().any())
        self.assertEqual(set(aggregated.time_band), {"am_peak", "midday", ALL_DAY_BAND})

    def test_a_traversal_outside_every_band_is_in_neither_the_bands_nor_all_day(self):
        # 33h into the service date is past late_night's 32h end, so assign_time_band leaves it
        # unlabelled -- it must not sneak into all_day through a separate path.
        traversals = pd.concat([self._traversals(), pd.DataFrame([self._traversal(33, 9999.0)])], ignore_index=True)

        aggregated = segments.aggregate_segments(traversals)
        all_day = aggregated[aggregated.time_band == ALL_DAY_BAND].iloc[0]

        self.assertEqual(all_day.n_traversals, 5)
        self.assertEqual(aggregated[aggregated.time_band != ALL_DAY_BAND].n_traversals.sum(), 5)

    def test_all_day_splits_by_extra_group_columns(self):
        # trends.py groups by day_type, so each day type gets its own all_day row.
        traversals = self._traversals()
        traversals["day_type"] = ["business_day"] * 3 + ["weekend_or_holiday"] * 2

        aggregated = segments.aggregate_segments(traversals, extra_group_columns=("day_type",))
        all_day = aggregated[aggregated.time_band == ALL_DAY_BAND].set_index("day_type")

        self.assertEqual(all_day.loc["business_day"].n_traversals, 3)
        self.assertEqual(all_day.loc["weekend_or_holiday"].n_traversals, 2)

    def test_all_day_is_not_a_departure_window(self):
        self.assertNotIn(ALL_DAY_BAND, [name for name, _, _ in TIME_BANDS])
        departures = pd.Series(np.arange(0, 32 * 3600, 900))
        self.assertNotIn(ALL_DAY_BAND, set(segments.assign_time_band(departures)))


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
