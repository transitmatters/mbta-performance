import unittest
from datetime import date
from unittest import mock

import pandas as pd

from .. import ingest
from ..constants import BUS_ALL_URL, BUS_RECENT_URL
from ..remote_parquet import ServiceDateNotFoundError

EVENTS = pd.DataFrame({"service_date": [date(2026, 11, 9)], "route_id": ["1"]})


class TestReadEventsForDate(unittest.TestCase):
    def test_reads_the_preferred_export_when_it_has_the_date(self):
        with (
            mock.patch.object(ingest, "source_url_for", return_value=BUS_RECENT_URL),
            mock.patch.object(ingest, "read_service_date", return_value=EVENTS) as read,
        ):
            events = ingest.read_events_for_date(date(2026, 11, 9))

        self.assertIs(events, EVENTS)
        self.assertEqual([call.args[0] for call in read.call_args_list], [BUS_RECENT_URL])

    def test_falls_back_to_the_all_time_export(self):
        with (
            mock.patch.object(ingest, "source_url_for", return_value=BUS_RECENT_URL),
            mock.patch.object(
                ingest, "read_service_date", side_effect=[ServiceDateNotFoundError("not in RECENT"), EVENTS]
            ) as read,
        ):
            events = ingest.read_events_for_date(date(2026, 11, 9))

        self.assertIs(events, EVENTS)
        self.assertEqual([call.args[0] for call in read.call_args_list], [BUS_RECENT_URL, BUS_ALL_URL])

    def test_falls_back_to_the_recent_export(self):
        with (
            mock.patch.object(ingest, "source_url_for", return_value=BUS_ALL_URL),
            mock.patch.object(
                ingest, "read_service_date", side_effect=[ServiceDateNotFoundError("not in ALL"), EVENTS]
            ) as read,
        ):
            events = ingest.read_events_for_date(date(2026, 11, 1))

        self.assertIs(events, EVENTS)
        self.assertEqual([call.args[0] for call in read.call_args_list], [BUS_ALL_URL, BUS_RECENT_URL])

    def test_raises_when_neither_export_has_the_date(self):
        with (
            mock.patch.object(ingest, "source_url_for", return_value=BUS_RECENT_URL),
            mock.patch.object(ingest, "read_service_date", side_effect=ServiceDateNotFoundError("missing")) as read,
        ):
            with self.assertRaises(ServiceDateNotFoundError):
                ingest.read_events_for_date(date(2026, 11, 12))

        self.assertEqual(read.call_count, 2)

    def test_an_empty_date_does_not_fall_back(self):
        # The export has the date and it's empty: that's an answer, not a missing date.
        with (
            mock.patch.object(ingest, "source_url_for", return_value=BUS_RECENT_URL),
            mock.patch.object(ingest, "read_service_date", return_value=pd.DataFrame()) as read,
        ):
            with self.assertRaises(ValueError):
                ingest.read_events_for_date(date(2026, 11, 9))

        read.assert_called_once()
