import io
import pathlib
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

from .. import status


def _fake_prefixes(tree):
    """Build an ls_prefixes stand-in from {prefix: [child prefixes]}."""

    def ls_prefixes(bucket, prefix):
        return tree.get(prefix, [])

    return ls_prefixes


class TestStatus(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.root)

    # --- latest_s3_month ---

    def test_latest_s3_month_max_across_sentinels(self):
        tree = {
            "Events/monthly-data/70061/": [
                "Events/monthly-data/70061/Year=2025/",
                "Events/monthly-data/70061/Year=2026/",
            ],
            "Events/monthly-data/70061/Year=2026/": [
                "Events/monthly-data/70061/Year=2026/Month=9/",
                "Events/monthly-data/70061/Year=2026/Month=10/",
            ],
            "Events/monthly-data/70036/": ["Events/monthly-data/70036/Year=2026/"],
            "Events/monthly-data/70036/Year=2026/": ["Events/monthly-data/70036/Year=2026/Month=11/"],
        }
        with mock.patch("chalicelib.s3.ls_prefixes", side_effect=_fake_prefixes(tree)):
            self.assertEqual(status.latest_s3_month("rapid"), (2026, 11))

    def test_latest_s3_month_compares_months_numerically(self):
        tree = {
            "Events/monthly-bus-data/1-0-110/": ["Events/monthly-bus-data/1-0-110/Year=2026/"],
            "Events/monthly-bus-data/1-0-110/Year=2026/": [
                "Events/monthly-bus-data/1-0-110/Year=2026/Month=2/",
                "Events/monthly-bus-data/1-0-110/Year=2026/Month=12/",
            ],
        }
        with mock.patch("chalicelib.s3.ls_prefixes", side_effect=_fake_prefixes(tree)):
            self.assertEqual(status.latest_s3_month("bus"), (2026, 12))

    def test_latest_s3_month_none(self):
        with mock.patch("chalicelib.s3.ls_prefixes", return_value=[]):
            self.assertIsNone(status.latest_s3_month("ferry"))

    # --- latest_local_month ---

    def test_latest_local_month(self):
        for rel in [
            "Events/monthly-data/70061/Year=2025/Month=12",
            "Events/monthly-data/70061/Year=2026/Month=7",
            "Events/monthly-data/70036/Year=2026/Month=10",
            "Events/monthly-data/70036/Year=2026/Month=9",
        ]:
            (self.root / rel).mkdir(parents=True)
        self.assertEqual(status.latest_local_month("rapid", str(self.root)), (2026, 10))

    def test_latest_local_month_missing_dir(self):
        self.assertIsNone(status.latest_local_month("bus", str(self.root)))

    # --- helpers ---

    def test_month_end(self):
        self.assertEqual(status.month_end((2026, 2)), "2026-02-28")
        self.assertEqual(status.month_end((2024, 2)), "2024-02-29")
        self.assertEqual(status.month_end((2026, 6)), "2026-06-30")

    def test_upstream_item_ids_use_latest_year(self):
        ids = status.upstream_item_ids()
        self.assertEqual(ids["rapid"], status.ARCGIS_IDS[max(status.ARCGIS_IDS)])
        self.assertEqual(ids["bus"], status.BUS_ARCGIS_IDS[max(status.BUS_ARCGIS_IDS)])
        self.assertEqual(ids["ferry"], status.FERRY_ARCGIS_ID)

    def test_upstream_modified_handles_errors(self):
        with mock.patch("requests.get", side_effect=Exception("offline")):
            self.assertEqual(status.upstream_modified("abc"), (None, None))

    def test_upstream_modified(self):
        response = mock.Mock()
        response.json.return_value = {"title": "MBTA Rapid Transit Events 2026", "modified": 1789000000000}
        with mock.patch("requests.get", return_value=response):
            title, modified = status.upstream_modified("abc")
        self.assertEqual(title, "MBTA Rapid Transit Events 2026")
        self.assertEqual(modified.year, 2026)

    # --- report ---

    def test_report_suggests_shared_cutoff_as_min_of_rapid_and_bus(self):
        latest = {"rapid": (2026, 8), "bus": (2026, 6), "ferry": (2026, 3)}
        out = io.StringIO()
        with (
            mock.patch.object(status, "latest_s3_month", side_effect=lambda mode: latest[mode]),
            redirect_stdout(out),
        ):
            result = status.report(str(self.root), check_upstream=False)

        self.assertEqual(result, latest)
        text = out.getvalue()
        self.assertIn('MAX_MONTH_DATA_DATE = "2026-06-30"', text)
        self.assertIn("BUS_MAX_DATE = '2026-06-30'", text)
        self.assertIn("FERRY_MAX_DATE = '2026-03-31'", text)

    def test_report_flags_local_months_not_in_s3(self):
        (self.root / "Events/monthly-data/70061/Year=2026/Month=7").mkdir(parents=True)
        latest = {"rapid": (2026, 3), "bus": None, "ferry": None}
        out = io.StringIO()
        with (
            mock.patch.object(status, "latest_s3_month", side_effect=lambda mode: latest[mode]),
            redirect_stdout(out),
        ):
            status.report(str(self.root), check_upstream=False)

        self.assertIn("historic.upload --mode rapid", out.getvalue())
        self.assertNotIn("MAX_MONTH_DATA_DATE", out.getvalue())


if __name__ == "__main__":
    unittest.main()
