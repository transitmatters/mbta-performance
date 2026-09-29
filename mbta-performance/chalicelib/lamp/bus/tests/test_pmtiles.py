import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from .. import pmtiles


def _segments(day_type: str | None = None, time_bands: tuple[str, ...] = ("am_peak",)) -> pd.DataFrame:
    return pd.concat([_segment(time_band, day_type) for time_band in time_bands], ignore_index=True)


def _segment(time_band: str, day_type: str | None) -> pd.DataFrame:
    row = {
        "route_id": "1",
        "direction_id": 0,
        "from_stop_id": "s1",
        "to_stop_id": "s2",
        "from_stop_name": "A",
        "to_stop_name": "B",
        "time_band": time_band,
        "n_traversals": 12,
        "n_interpolated": 1,
        "segment_length_m": 500.0,
        "p50_speed_mph": 11.5,
        "p90_speed_mph": 8.0,
        "moving_speed_mph": 20.0,
        "coordinates": [(-71.05, 42.36), (-71.06, 42.37)],
    }
    # day_type is only present on the weekly/monthly trend rollups (trends.py), never on a
    # daily frame -- most tests exercise the daily shape, so it's opt-in here.
    if day_type is not None:
        row["day_type"] = day_type
    return pd.DataFrame([row])


class TestSplitAllDay(unittest.TestCase):
    def test_separates_all_day_rows_from_band_rows(self):
        frame = _segments(time_bands=("am_peak", "all_day", "midday"))

        bands, all_day = pmtiles.split_all_day(frame)

        self.assertEqual(list(bands.time_band), ["am_peak", "midday"])
        self.assertEqual(list(all_day.time_band), ["all_day"])
        # Nothing else changes: the all_day archive carries the same columns as the bands'.
        self.assertEqual(list(bands.columns), list(all_day.columns))


class TestRequireTippecanoe(unittest.TestCase):
    def test_raises_a_clear_error_when_not_on_path(self):
        with mock.patch.object(pmtiles.shutil, "which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "not on PATH"):
                pmtiles.build_pmtiles_bytes(_segments())


class TestBuildPmtilesBytes(unittest.TestCase):
    """Exercises the column-filtering and command construction without needing the real
    tippecanoe binary, so this passes even where it isn't installed."""

    def _fake_run(self, command: list[str], **_kwargs) -> subprocess.CompletedProcess:
        geojson_path = Path(command[-1])
        output_path = Path(command[command.index("--output") + 1])
        with geojson_path.open() as f:
            # GeoJSONSeq (RFC 8142) prefixes each record with an ASCII record separator.
            self.written_properties = [
                json.loads(line.lstrip("\x1e"))["properties"] for line in f if line.strip("\x1e\n")
            ]
        output_path.write_bytes(b"PMTiles\x03fake")
        return subprocess.CompletedProcess(command, 0)

    def test_only_tile_properties_reach_tippecanoe(self):
        with (
            mock.patch.object(pmtiles.shutil, "which", return_value="/usr/bin/tippecanoe"),
            mock.patch.object(pmtiles.subprocess, "run", side_effect=self._fake_run),
        ):
            data = pmtiles.build_pmtiles_bytes(_segments())

        self.assertEqual(data, b"PMTiles\x03fake")
        self.assertEqual(len(self.written_properties), 1)
        # day_type isn't in a daily-shaped frame -- absent columns are dropped, not required.
        self.assertEqual(set(self.written_properties[0]), set(pmtiles.TILE_PROPERTIES) - {"day_type"})

    def test_day_type_reaches_tippecanoe_when_present(self):
        # Present on the weekly/monthly trend rollups (trends.py) -- confirms it isn't
        # silently dropped like the columns TILE_PROPERTIES deliberately excludes.
        with (
            mock.patch.object(pmtiles.shutil, "which", return_value="/usr/bin/tippecanoe"),
            mock.patch.object(pmtiles.subprocess, "run", side_effect=self._fake_run),
        ):
            pmtiles.build_pmtiles_bytes(_segments(day_type="business_day"))

        self.assertEqual(self.written_properties[0]["day_type"], "business_day")

    def test_all_day_rows_reach_tippecanoe_with_the_same_properties_as_a_band(self):
        # The map filters on an exact time_band match, so all_day must arrive as a plain
        # time_band value carrying every property a band feature has.
        with (
            mock.patch.object(pmtiles.shutil, "which", return_value="/usr/bin/tippecanoe"),
            mock.patch.object(pmtiles.subprocess, "run", side_effect=self._fake_run),
        ):
            pmtiles.build_pmtiles_bytes(_segments(time_bands=("am_peak", "all_day")))

        by_band = {properties["time_band"]: properties for properties in self.written_properties}
        self.assertEqual(set(by_band), {"am_peak", "all_day"})
        self.assertEqual(set(by_band["all_day"]), set(by_band["am_peak"]))

    def test_uses_the_segments_layer_and_configured_zoom_range(self):
        with (
            mock.patch.object(pmtiles.shutil, "which", return_value="/usr/bin/tippecanoe"),
            mock.patch.object(pmtiles.subprocess, "run", side_effect=self._fake_run) as run,
        ):
            pmtiles.build_pmtiles_bytes(_segments())

        command = run.call_args[0][0]
        self.assertIn("--layer", command)
        self.assertEqual(command[command.index("--layer") + 1], pmtiles.LAYER_NAME)
        self.assertEqual(command[command.index("--minimum-zoom") + 1], str(pmtiles.MINIMUM_ZOOM))
        self.assertEqual(command[command.index("--maximum-zoom") + 1], str(pmtiles.MAXIMUM_ZOOM))
        # Without these, a dense real day (many overlapping time-band/direction duplicates at
        # high zoom) fails outright instead of tiling -- see _run_tippecanoe's comment.
        self.assertIn("--drop-densest-as-needed", command)
        self.assertIn("--extend-zooms-if-still-dropping", command)

    def test_raises_on_a_nonzero_exit(self):
        def failing_run(command, **_kwargs):
            return subprocess.CompletedProcess(command, 1, stderr="boom")

        with (
            mock.patch.object(pmtiles.shutil, "which", return_value="/usr/bin/tippecanoe"),
            mock.patch.object(pmtiles.subprocess, "run", side_effect=failing_run),
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                pmtiles.build_pmtiles_bytes(_segments())


@unittest.skipUnless(shutil.which("tippecanoe"), "tippecanoe is not installed")
class TestBuildPmtilesBytesWithRealTippecanoe(unittest.TestCase):
    """Runs the actual binary when available -- gated so CI environments without it (or a
    developer's machine without it) still pass the rest of the suite."""

    def test_produces_a_segments_layer_with_only_tile_properties(self):
        data = pmtiles.build_pmtiles_bytes(_segments(day_type="business_day", time_bands=("am_peak", "all_day")))

        self.assertTrue(data.startswith(b"PMTiles"))

        # tippecanoe-decode needs to seek within a real .pmtiles file -- a pipe won't do.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tileset_path = Path(tmp_dir) / "segments.pmtiles"
            tileset_path.write_bytes(data)
            decoded = subprocess.run(
                ["tippecanoe-decode", str(tileset_path)], capture_output=True, check=True, text=True
            ).stdout

        tile_json = json.loads(json.loads(decoded)["properties"]["json"])
        layer = tile_json["vector_layers"][0]

        self.assertEqual(layer["id"], pmtiles.LAYER_NAME)
        self.assertEqual(set(layer["fields"]), set(pmtiles.TILE_PROPERTIES))
        self.assertEqual(self._time_bands(decoded), {"am_peak", "all_day"})

    def test_split_archives_each_hold_only_their_own_rows(self):
        decoded = []
        for rows in pmtiles.split_all_day(_segments(time_bands=("am_peak", "midday", "all_day"))):
            with tempfile.TemporaryDirectory() as tmp_dir:
                tileset_path = Path(tmp_dir) / "segments.pmtiles"
                tileset_path.write_bytes(pmtiles.build_pmtiles_bytes(rows))
                decoded.append(
                    subprocess.run(
                        ["tippecanoe-decode", str(tileset_path)], capture_output=True, check=True, text=True
                    ).stdout
                )

        self.assertEqual(self._time_bands(decoded[0]), {"am_peak", "midday"})
        self.assertEqual(self._time_bands(decoded[1]), {"all_day"})
        # Same layer name in both, so the frontend can point one layer config at either.
        for archive in decoded:
            layer = json.loads(json.loads(archive)["properties"]["json"])["vector_layers"][0]
            self.assertEqual(layer["id"], pmtiles.LAYER_NAME)

    @staticmethod
    def _time_bands(decoded: str) -> set[str]:
        return {
            feature["properties"]["time_band"]
            for tile in json.loads(decoded)["features"]
            for layer in tile["features"]
            for feature in layer["features"]
        }
