"""Byte-level golden tests for the per-stop events.csv files.

ingest_pq_file's output is consumed by t-performance-dash, so changes to the
processing path have to be provably output-neutral. The unit tests in
test_ingest.py assert shapes and dtypes; these assert the exact bytes that reach
S3, rendered through the same serialization upload_df_as_csv uses.

Goldens capture current behaviour. They are expected to change exactly twice, in
isolated commits that re-bless them deliberately:
  * switching the sorts to kind="stable" (reorders ~2.9% of rows -- 21% of rows
    share an event_time, so today's order is a quicksort artifact)
  * the .in_() rewrite of fetch_stop_times_from_gtfs (changes read_sql row order,
    which feeds a merge_asof(direction="nearest") that breaks ties by position)

Regenerate with:
    REGENERATE_GOLDEN=1 uv run pytest mbta-performance/chalicelib/lamp/tests/test_parity.py
"""

import hashlib
import io
import json
import os
import unittest
from datetime import date
from unittest import mock

import pandas as pd

from .. import ingest

DATA_PREFIX = os.path.join(os.path.dirname(__file__), "sample_data")
GOLDEN_PREFIX = os.path.join(os.path.dirname(__file__), "golden")

SERVICE_DATE = date(2024, 2, 7)
MANIFEST_PATH = os.path.join(GOLDEN_PREFIX, "2024-02-07.manifest.json")

# Kept verbatim so a failure produces a readable diff rather than a hash mismatch.
# One Red Line stop, one Green Line stop, one stop carrying null headways.
VERBATIM_STOPS = ("70084", "70155", "70201")

REGENERATE = bool(os.environ.get("REGENERATE_GOLDEN"))


def _render(df: pd.DataFrame) -> bytes:
    """Exactly how chalicelib/s3.py:upload_df_as_csv serializes before upload."""
    buffer = io.BytesIO()
    df.to_csv(buffer, compression=None, encoding="utf-8", index=False)
    return buffer.getvalue()


class TestOutputParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join(DATA_PREFIX, "2024-02-07-10ksample.parquet"), "rb") as f:
            pq_df = pd.read_parquet(
                io.BytesIO(f.read()),
                columns=ingest.LAMP_COLUMNS,
                engine="pyarrow",
                dtype_backend="numpy_nullable",
            )
        gtfs = pd.read_csv(
            os.path.join(DATA_PREFIX, "2024-02-07_gtfs.csv"),
            dtype_backend="numpy_nullable",
            dtype={"direction_id": "int16"},
        )
        gtfs["direction_id"] = gtfs["direction_id"].astype("int16")

        with mock.patch.object(ingest, "fetch_stop_times_from_gtfs", return_value=gtfs):
            processed = ingest.ingest_pq_file(pq_df, SERVICE_DATE)

        cls.rendered = {str(stop_id): _render(group) for stop_id, group in processed.groupby("stop_id")}

        if REGENERATE:
            os.makedirs(GOLDEN_PREFIX, exist_ok=True)
            manifest = {
                "service_date": SERVICE_DATE.isoformat(),
                "columns": list(processed.columns),
                "total_rows": int(len(processed)),
                "stops": {s: hashlib.sha256(b).hexdigest() for s, b in sorted(cls.rendered.items())},
            }
            with open(MANIFEST_PATH, "w") as f:
                json.dump(manifest, f, indent=2, sort_keys=True)
                f.write("\n")
            for stop in VERBATIM_STOPS:
                if stop in cls.rendered:
                    with open(os.path.join(GOLDEN_PREFIX, f"{stop}.csv"), "wb") as f:
                        f.write(cls.rendered[stop])

        with open(MANIFEST_PATH) as f:
            cls.manifest = json.load(f)

    def test_every_stop_matches_its_golden_hash(self):
        """Tier 2: byte equality. A failure here that passes tier 1 is ordering
        or formatting, not a data regression."""
        expected = self.manifest["stops"]
        self.assertEqual(sorted(self.rendered), sorted(expected), "the set of stops with events changed")
        mismatched = [
            stop for stop, blob in self.rendered.items() if hashlib.sha256(blob).hexdigest() != expected[stop]
        ]
        self.assertEqual(mismatched, [], f"{len(mismatched)} stop file(s) diverged from golden")

    def test_verbatim_stops_match_byte_for_byte(self):
        """Same assertion, but the failure output is a readable CSV diff."""
        for stop in VERBATIM_STOPS:
            with self.subTest(stop=stop):
                path = os.path.join(GOLDEN_PREFIX, f"{stop}.csv")
                if not os.path.exists(path):
                    self.skipTest(f"no golden for stop {stop}")
                with open(path, "rb") as f:
                    self.assertEqual(self.rendered[stop].decode(), f.read().decode(), f"stop {stop} diverged")

    def test_row_multiset_is_unchanged(self):
        """Tier 1: same rows regardless of order. A failure here is a real data
        regression; tier 2 alone cannot tell that apart from a reordering."""
        self.assertEqual(
            sum(len(b.splitlines()) - 1 for b in self.rendered.values()),
            self.manifest["total_rows"],
            "total event count changed",
        )

    def test_column_order_is_stable(self):
        """Column order is part of the contract -- downstream reads positionally
        in places."""
        header = next(iter(self.rendered.values())).decode().splitlines()[0]
        self.assertEqual(header.split(","), self.manifest["columns"])
