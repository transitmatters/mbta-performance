import contextlib
import io
import os
import pathlib
import tempfile
import unittest
from unittest import mock

from ..backfill import bus


class TestBackfillBus(unittest.TestCase):
    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        # backfill_bus_data reads data/input/bus/{year} relative to the working directory
        os.chdir(self._tmp.name)
        year_dir = pathlib.Path("data/input/bus/2024")
        year_dir.mkdir(parents=True)
        for month in ("01", "02", "03"):
            (year_dir / f"bus-2024-{month}.csv").touch()

    def tearDown(self):
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def test_failed_file_stops_before_upload_after_processing_the_rest(self):
        """A failed file used to be printed and skipped, and the run went on to upload as if it had
        succeeded. Now the rest still process, then it raises with every failure, so __main__
        never reaches the upload."""

        def process(csv_path, *args):
            if csv_path.endswith("02.csv"):
                raise KeyError("scheduled_headway")

        stderr = io.StringIO()
        with mock.patch.object(bus, "process_bus_events", side_effect=process) as mock_process:
            with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, r"1 bus file\(s\) failed.*bus-2024-02\.csv"):
                    bus.backfill_bus_data([2024])

        self.assertEqual(mock_process.call_count, 3)
        # The full traceback, not just the exception's message
        self.assertIn("Traceback", stderr.getvalue())
        self.assertIn("KeyError: 'scheduled_headway'", stderr.getvalue())

    def test_all_files_succeed(self):
        with mock.patch.object(bus, "process_bus_events") as mock_process:
            with contextlib.redirect_stdout(io.StringIO()):
                bus.backfill_bus_data([2024])

        self.assertEqual(mock_process.call_count, 3)
