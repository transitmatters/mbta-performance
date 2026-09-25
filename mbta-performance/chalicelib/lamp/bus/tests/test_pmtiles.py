import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from .. import pmtiles


def _segments(day_type: str | None = None) -> pd.DataFrame:
    row = {
        "route_id": "1",
        "direction_id": 0,
        "from_stop_id": "s1",
        "to_stop_id": "s2",
        "from_stop_name": "A",
        "to_stop_name": "B",
        "time_band": "am_peak",
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
        data = pmtiles.build_pmtiles_bytes(_segments(day_type="business_day"))

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
