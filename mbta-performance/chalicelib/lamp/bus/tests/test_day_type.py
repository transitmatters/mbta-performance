import unittest
from datetime import date
from unittest import mock

import pandas as pd

from .. import day_type


class TestIsMbtaHoliday(unittest.TestCase):
    def test_true_when_the_date_has_a_holiday_name(self):
        frame = pd.DataFrame(
            [
                {"date": 20251225, "holiday_name": "Christmas Day"},
                {"date": 20251224, "holiday_name": None},
            ]
        )
        with mock.patch.object(day_type, "read_gtfs_archive_file", return_value=frame):
            self.assertTrue(day_type.is_mbta_holiday(date(2025, 12, 25)))

    def test_false_for_an_ordinary_weekday(self):
        frame = pd.DataFrame([{"date": 20260113, "holiday_name": None}])
        with mock.patch.object(day_type, "read_gtfs_archive_file", return_value=frame):
            self.assertFalse(day_type.is_mbta_holiday(date(2026, 1, 13)))

    def test_false_when_the_holiday_name_belongs_to_a_different_date(self):
        # calendar_dates.txt covers a whole feed version's worth of dates -- only an exact
        # match on this date's row counts.
        frame = pd.DataFrame([{"date": 20260119, "holiday_name": "Martin Luther King Day"}])
        with mock.patch.object(day_type, "read_gtfs_archive_file", return_value=frame):
            self.assertFalse(day_type.is_mbta_holiday(date(2026, 1, 20)))

    def test_false_when_the_archive_slice_is_empty(self):
        with mock.patch.object(day_type, "read_gtfs_archive_file", return_value=pd.DataFrame()):
            self.assertFalse(day_type.is_mbta_holiday(date(2026, 1, 13)))


class TestDayTypeFor(unittest.TestCase):
    def test_saturday_and_sunday_are_weekend_regardless_of_holiday_status(self):
        with mock.patch.object(day_type, "is_mbta_holiday", return_value=False) as holiday_check:
            self.assertEqual(day_type.day_type_for(date(2026, 1, 17)), day_type.WEEKEND_OR_HOLIDAY)  # Saturday
            self.assertEqual(day_type.day_type_for(date(2026, 1, 18)), day_type.WEEKEND_OR_HOLIDAY)  # Sunday

        # No need to even check the calendar for a weekend day.
        holiday_check.assert_not_called()

    def test_an_ordinary_weekday_is_a_business_day(self):
        with mock.patch.object(day_type, "is_mbta_holiday", return_value=False):
            self.assertEqual(day_type.day_type_for(date(2026, 1, 13)), day_type.BUSINESS_DAY)  # Tuesday

    def test_a_weekday_mbta_holiday_is_weekend_or_holiday(self):
        with mock.patch.object(day_type, "is_mbta_holiday", return_value=True):
            self.assertEqual(day_type.day_type_for(date(2026, 1, 19)), day_type.WEEKEND_OR_HOLIDAY)  # MLK Day
