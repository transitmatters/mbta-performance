import hashlib
import pathlib
import shutil
import tempfile
import unittest
from datetime import date
from unittest import mock

from .. import upload


def _write(root: pathlib.Path, rel: str, content: bytes) -> pathlib.Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


class TestUpload(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        _write(self.root, "Events/monthly-data/70061/Year=2026/Month=3/events.csv.gz", b"march")
        _write(self.root, "Events/monthly-data/70061/Year=2026/Month=4/events.csv.gz", b"april")
        _write(self.root, "Events/monthly-data/70036/Year=2026/Month=4/events.csv.gz", b"april-oak")
        _write(self.root, "Events/monthly-data/70036/Year=2025/Month=12/events.csv.gz", b"december")
        _write(self.root, "Events/monthly-bus-data/1-0-110/Year=2026/Month=6/events.csv.gz", b"bus")
        _write(self.root, "Events/monthly-data/70061/Year=2026/notamonth/events.csv.gz", b"ignored")

    def tearDown(self):
        shutil.rmtree(self.root)

    def _keys(self, mode, start=None, end=None):
        return sorted(key for _, key, _, _ in upload.find_local_files(str(self.root), mode, start, end))

    # --- find_local_files ---

    def test_find_local_files_maps_paths_to_s3_keys(self):
        self.assertEqual(
            self._keys("rapid"),
            [
                "Events/monthly-data/70036/Year=2025/Month=12/events.csv.gz",
                "Events/monthly-data/70036/Year=2026/Month=4/events.csv.gz",
                "Events/monthly-data/70061/Year=2026/Month=3/events.csv.gz",
                "Events/monthly-data/70061/Year=2026/Month=4/events.csv.gz",
            ],
        )
        self.assertEqual(self._keys("bus"), ["Events/monthly-bus-data/1-0-110/Year=2026/Month=6/events.csv.gz"])

    def test_find_local_files_missing_mode_dir(self):
        self.assertEqual(self._keys("ferry"), [])

    def test_find_local_files_month_range(self):
        # Only year/month matter, so a mid-month start still includes that month
        self.assertEqual(
            self._keys("rapid", start=date(2026, 4, 15), end=date(2026, 4, 1)),
            [
                "Events/monthly-data/70036/Year=2026/Month=4/events.csv.gz",
                "Events/monthly-data/70061/Year=2026/Month=4/events.csv.gz",
            ],
        )
        self.assertEqual(
            self._keys("rapid", end=date(2026, 1, 31)),
            ["Events/monthly-data/70036/Year=2025/Month=12/events.csv.gz"],
        )

    def test_find_local_files_stop_prefix(self):
        prefixes = {prefix for _, _, _, prefix in upload.find_local_files(str(self.root), "rapid")}
        self.assertEqual(prefixes, {"Events/monthly-data/70061/", "Events/monthly-data/70036/"})

    # --- upload_mode ---

    def _remote(self, bucket, prefix):
        # 70061 March already in S3 with identical content; 70061 April in S3 but different
        etags = {
            "Events/monthly-data/70061/Year=2026/Month=3/events.csv.gz": hashlib.md5(b"march").hexdigest(),
            "Events/monthly-data/70061/Year=2026/Month=4/events.csv.gz": hashlib.md5(b"stale").hexdigest(),
        }
        return {k: v for k, v in etags.items() if k.startswith(prefix)}

    def test_upload_mode_skips_unchanged_and_uploads_rest(self):
        with (
            mock.patch("chalicelib.s3.ls_etags", side_effect=self._remote),
            mock.patch("chalicelib.s3.upload_file") as mock_upload,
        ):
            summary = upload.upload_mode("rapid", str(self.root), workers=2)

        uploaded_keys = sorted(call.args[1] for call in mock_upload.call_args_list)
        self.assertEqual(
            uploaded_keys,
            [
                "Events/monthly-data/70036/Year=2025/Month=12/events.csv.gz",
                "Events/monthly-data/70036/Year=2026/Month=4/events.csv.gz",
                "Events/monthly-data/70061/Year=2026/Month=4/events.csv.gz",
            ],
        )
        self.assertEqual(mock_upload.call_args_list[0].args[0], "tm-mbta-performance")
        self.assertEqual((summary["uploaded"], summary["unchanged"], summary["failed"]), (3, 1, 0))
        self.assertEqual(dict(summary["months"]), {(2025, 12): 1, (2026, 4): 2})

    def test_upload_mode_lists_once_per_stop(self):
        with (
            mock.patch("chalicelib.s3.ls_etags", return_value={}) as mock_ls,
            mock.patch("chalicelib.s3.upload_file"),
        ):
            upload.upload_mode("rapid", str(self.root), workers=2)

        self.assertEqual(
            sorted(call.args[1] for call in mock_ls.call_args_list),
            ["Events/monthly-data/70036/", "Events/monthly-data/70061/"],
        )

    def test_upload_mode_dry_run_does_not_write(self):
        with (
            mock.patch("chalicelib.s3.ls_etags", side_effect=self._remote),
            mock.patch("chalicelib.s3.upload_file") as mock_upload,
        ):
            summary = upload.upload_mode("rapid", str(self.root), dry_run=True, workers=2)

        mock_upload.assert_not_called()
        self.assertEqual((summary["uploaded"], summary["unchanged"]), (3, 1))

    def test_upload_mode_records_failures(self):
        with (
            mock.patch("chalicelib.s3.ls_etags", return_value={}),
            mock.patch("chalicelib.s3.upload_file", side_effect=Exception("boom")),
        ):
            summary = upload.upload_mode("bus", str(self.root), workers=2)

        self.assertEqual((summary["uploaded"], summary["failed"]), (0, 1))
        self.assertIn("boom", summary["errors"][0])

    def test_upload_mode_no_files(self):
        with mock.patch("chalicelib.s3.ls_etags") as mock_ls:
            summary = upload.upload_mode("ferry", str(self.root))

        mock_ls.assert_not_called()
        self.assertEqual(summary["uploaded"], 0)

    # --- upload_monthly_outputs ---

    def test_upload_monthly_outputs_raises_on_failure(self):
        with (
            mock.patch("chalicelib.s3.ls_etags", return_value={}),
            mock.patch("chalicelib.s3.upload_file", side_effect=Exception("boom")),
        ):
            with self.assertRaises(RuntimeError):
                upload.upload_monthly_outputs(str(self.root), modes=("bus",), workers=2)

    def test_upload_monthly_outputs_all_modes(self):
        with (
            mock.patch("chalicelib.s3.ls_etags", return_value={}),
            mock.patch("chalicelib.s3.upload_file") as mock_upload,
        ):
            summaries = upload.upload_monthly_outputs(str(self.root), workers=2)

        self.assertEqual([s["mode"] for s in summaries], ["rapid", "bus", "ferry"])
        self.assertEqual(mock_upload.call_count, 5)

    # --- parse_month ---

    def test_parse_month(self):
        self.assertEqual(upload.parse_month("2026-04"), date(2026, 4, 1))

    def test_parse_month_invalid(self):
        import argparse

        with self.assertRaises(argparse.ArgumentTypeError):
            upload.parse_month("2026-4-1")


if __name__ == "__main__":
    unittest.main()
