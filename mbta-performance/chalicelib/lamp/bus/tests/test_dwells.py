import unittest
from datetime import date

import numpy as np
import pandas as pd

from .. import dwells
from ..constants import ALL_DAY_BAND


def _traversal(from_stop_id: str, depart_hour: float, dwell: float, **overrides) -> dict:
    return {
        "route_id": "1",
        "direction_id": 0,
        "from_stop_id": from_stop_id,
        "to_stop_id": "next",
        "service_date": date(2026, 11, 4),
        "depart_seconds": depart_hour * 3600,
        "dwell_seconds": dwell,
        "is_trip_start": False,
        "from_interpolated": False,
        **overrides,
    }


def _result(pattern_traversals: dict[str, int] | None = None) -> pd.DataFrame:
    """Segment rows with geometry, as ingest.py merges them, for stops s1 and s2."""
    pattern_traversals = pattern_traversals or {"s1": 5, "s2": 5}
    return pd.DataFrame(
        [
            {
                "route_id": "1",
                "direction_id": 0,
                "from_stop_id": stop_id,
                "from_stop_name": f"Stop {stop_id}",
                "pattern_traversals": traversals,
                "coordinates": [(-71.0 - index, 42.0), (-71.5, 42.5)],
            }
            for index, (stop_id, traversals) in enumerate(pattern_traversals.items())
        ]
    )


class TestAggregateDwells(unittest.TestCase):
    def test_one_row_per_stop_and_band_plus_all_day(self):
        traversals = pd.DataFrame(
            [_traversal("s1", 8, dwell) for dwell in (10.0, 20.0, 30.0)] + [_traversal("s1", 12, 60.0)]
        )

        result = dwells.aggregate_dwells(traversals, _result()).set_index("time_band")

        self.assertEqual(set(result.index), {"am_peak", "midday", ALL_DAY_BAND})
        self.assertEqual(result.loc["am_peak"].n_dwells, 3)
        self.assertEqual(result.loc["am_peak"].median_dwell_seconds, 20.0)
        self.assertAlmostEqual(result.loc["am_peak"].p90_dwell_seconds, np.percentile([10, 20, 30], 90))
        # All day from every dwell, not from the band rows.
        self.assertEqual(result.loc[ALL_DAY_BAND].n_dwells, 4)
        self.assertEqual(result.loc[ALL_DAY_BAND].median_dwell_seconds, 25.0)

    def test_layover_and_interpolated_stops_are_not_dwells(self):
        traversals = pd.DataFrame(
            [
                _traversal("s1", 8, 20.0),
                _traversal("s1", 8, 0.0, is_trip_start=True),
                _traversal("s1", 8, 0.0, from_interpolated=True),
            ]
        )

        result = dwells.aggregate_dwells(traversals, _result())

        self.assertEqual(result.set_index("time_band").loc["am_peak"].n_dwells, 1)
        self.assertEqual(result.set_index("time_band").loc["am_peak"].median_dwell_seconds, 20.0)

    def test_located_at_the_start_of_the_busiest_segment_leaving_the_stop(self):
        traversals = pd.DataFrame([_traversal("s1", 8, 20.0)])
        result_rows = pd.concat(
            [
                _result({"s1": 1}).assign(coordinates=[[(-70.0, 41.0), (-70.5, 41.5)]]),
                _result({"s1": 9}).assign(coordinates=[[(-71.0, 42.0), (-71.5, 42.5)]]),
            ],
            ignore_index=True,
        )

        row = dwells.aggregate_dwells(traversals, result_rows).iloc[0]

        self.assertEqual(row.coordinates, (-71.0, 42.0))
        self.assertEqual(row.stop_id, "s1")
        self.assertEqual(row.stop_name, "Stop s1")

    def test_a_stop_with_no_segment_on_the_map_is_dropped(self):
        traversals = pd.DataFrame([_traversal("s1", 8, 20.0), _traversal("elsewhere", 8, 20.0)])

        result = dwells.aggregate_dwells(traversals, _result())

        self.assertEqual(set(result.stop_id), {"s1"})

    def test_splits_by_extra_group_columns(self):
        traversals = pd.DataFrame(
            [
                _traversal("s1", 8, 20.0, day_type="business_day"),
                _traversal("s1", 8, 40.0, day_type="weekend_or_holiday"),
            ]
        )

        result = dwells.aggregate_dwells(traversals, _result(), extra_group_columns=("day_type",))
        all_day = result[result.time_band == ALL_DAY_BAND].set_index("day_type")

        self.assertEqual(all_day.loc["business_day"].median_dwell_seconds, 20.0)
        self.assertEqual(all_day.loc["weekend_or_holiday"].median_dwell_seconds, 40.0)
        self.assertNotIn("service_date", result.columns)
