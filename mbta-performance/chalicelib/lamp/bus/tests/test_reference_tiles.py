import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from .. import pmtiles, reference_tiles


def _gtfs() -> dict[str, pd.DataFrame]:
    """A tiny feed: Park Street (Red + Green-B + Green-C platforms), one bus stop on two
    routes, one bus stop only a shuttle serves, and one bus stop nothing serves."""
    stops = pd.DataFrame(
        [
            # stop_id, stop_name, lat, lon, location_type, vehicle_type, parent_station
            ("place-pktrm", "Park Street", 42.356, -71.062, 1, None, None),
            ("70075", "Park Street - Red", 42.356, -71.062, 0, 1, "place-pktrm"),
            ("70196", "Park Street - Green B", 42.356, -71.062, 0, 0, "place-pktrm"),
            ("70197", "Park Street - Green C", 42.356, -71.062, 0, 0, "place-pktrm"),
            ("place-empty", "Station Nothing Serves", 42.30, -71.10, 1, None, None),
            ("1", "Washington St @ Melnea Cass", 42.33, -71.08, 0, 3, None),
            ("2", "Shuttle-only stop", 42.31, -71.09, 0, 3, None),
            ("3", "Retired stop", 42.32, -71.07, 0, 3, None),
        ],
        columns=["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "vehicle_type", "parent_station"],
    )
    routes = pd.DataFrame(
        [
            # route_id, route_type, route_sort_order, line_id, listed_route
            ("Red", 1, 10010, "line-Red", None),
            ("Green-C", 0, 10033, "line-Green", None),
            ("Green-B", 0, 10032, "line-Green", None),
            ("47", 3, 50470, "line-47", None),
            ("1", 3, 50010, "line-1", None),
            ("Shuttle-Red", 3, 60000, "line-Shuttle", 1),
        ],
        columns=["route_id", "route_type", "route_sort_order", "line_id", "listed_route"],
    )
    trips = pd.DataFrame(
        [
            ("t-red", "Red"),
            ("t-gb", "Green-B"),
            ("t-gc", "Green-C"),
            ("t-47", "47"),
            ("t-1", "1"),
            ("t-sh", "Shuttle-Red"),
        ],
        columns=["trip_id", "route_id"],
    )
    stop_times = pd.DataFrame(
        [
            ("t-red", "70075"),
            ("t-gb", "70196"),
            ("t-gc", "70197"),
            ("t-47", "1"),
            ("t-1", "1"),
            ("t-sh", "1"),
            ("t-sh", "2"),
        ],
        columns=["trip_id", "stop_id"],
    )
    return {"stops": stops, "trips": trips, "stop_times": stop_times, "routes": routes}


def _select():
    return reference_tiles.select_reference_stops(**_gtfs())


class TestSelectReferenceStops(unittest.TestCase):
    def test_stations_roll_platform_service_up_to_the_parent(self):
        stations, _ = _select()

        self.assertEqual(stations.stop_id.tolist(), ["place-pktrm"])
        park = stations.iloc[0]
        # Green-B and Green-C collapse into one line; both orders follow route_sort_order.
        self.assertEqual(park.lines, "Red,Green")
        self.assertEqual(park.routes, "Red,Green-B,Green-C")

    def test_bus_stops_list_their_routes_in_sort_order_without_shuttles(self):
        _, bus_stops = _select()

        self.assertEqual(bus_stops.stop_id.tolist(), ["1"])
        self.assertEqual(bus_stops.iloc[0].routes, "1,47")

    def test_unserved_and_shuttle_only_stops_are_dropped(self):
        stations, bus_stops = _select()

        self.assertNotIn("place-empty", stations.stop_id.tolist())
        self.assertNotIn("2", bus_stops.stop_id.tolist())
        self.assertNotIn("3", bus_stops.stop_id.tolist())


class TestBuildReferencePmtilesBytes(unittest.TestCase):
    """Command construction and per-feature zooms, without the real tippecanoe binary."""

    def _fake_run(self, command: list[str], **_kwargs) -> subprocess.CompletedProcess:
        self.features = {}
        for index, argument in enumerate(command):
            if argument == "--named-layer":
                layer, path = command[index + 1].split(":", 1)
                with open(path) as f:
                    self.features[layer] = [json.loads(line) for line in f]
        Path(command[command.index("--output") + 1]).write_bytes(b"PMTiles\x03fake")
        return subprocess.CompletedProcess(command, 0)

    def _build(self):
        with (
            mock.patch.object(pmtiles.shutil, "which", return_value="/usr/bin/tippecanoe"),
            mock.patch.object(reference_tiles.subprocess, "run", side_effect=self._fake_run) as run,
        ):
            data = reference_tiles.build_reference_pmtiles_bytes(*_select())
        return data, run.call_args[0][0]

    def test_writes_both_layers_with_their_own_minimum_zoom(self):
        data, _ = self._build()

        self.assertEqual(data, b"PMTiles\x03fake")
        self.assertEqual(set(self.features), {reference_tiles.STATIONS_LAYER, reference_tiles.BUS_STOPS_LAYER})
        station = self.features[reference_tiles.STATIONS_LAYER][0]
        bus_stop = self.features[reference_tiles.BUS_STOPS_LAYER][0]
        self.assertEqual(station["tippecanoe"]["minzoom"], reference_tiles.STATION_MINIMUM_ZOOM)
        self.assertEqual(bus_stop["tippecanoe"]["minzoom"], reference_tiles.BUS_STOP_MINIMUM_ZOOM)
        self.assertEqual(station["geometry"], {"type": "Point", "coordinates": [-71.062, 42.356]})
        self.assertEqual(set(station["properties"]), {"stop_id", "stop_name", "lines", "routes"})
        self.assertEqual(set(bus_stop["properties"]), {"stop_id", "stop_name", "routes"})

    def test_never_drops_points(self):
        _, command = self._build()

        self.assertEqual(command[command.index("--drop-rate") + 1], "1")
        self.assertNotIn("--drop-densest-as-needed", command)


@unittest.skipUnless(shutil.which("tippecanoe"), "tippecanoe is not installed")
class TestBuildReferencePmtilesBytesWithRealTippecanoe(unittest.TestCase):
    def _decode(self, data: bytes, *tile: int) -> dict:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tileset_path = Path(tmp_dir) / "stops.pmtiles"
            tileset_path.write_bytes(data)
            decoded = subprocess.run(
                ["tippecanoe-decode", str(tileset_path), *map(str, tile)], capture_output=True, check=True, text=True
            ).stdout
        return json.loads(decoded)

    def test_bus_stops_only_appear_from_their_minimum_zoom(self):
        data = reference_tiles.build_reference_pmtiles_bytes(*_select())
        self.assertTrue(data.startswith(b"PMTiles"))

        # The z4 tile covering Boston: stations yes, bus stops not yet.
        low = self._decode(data, 4, 4, 5)
        self.assertEqual([layer["properties"]["layer"] for layer in low["features"]], [reference_tiles.STATIONS_LAYER])

        # The z11 tile covering Washington St @ Melnea Cass: bus stop now present.
        high = self._decode(data, 11, 619, 757)
        layers = {layer["properties"]["layer"]: layer["features"] for layer in high["features"]}
        self.assertEqual([f["properties"]["routes"] for f in layers[reference_tiles.BUS_STOPS_LAYER]], ["1,47"])
