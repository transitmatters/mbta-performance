import unittest
from datetime import date

from .. import periods


class TestWeekKeyFor(unittest.TestCase):
    def test_earliest_data_falls_in_iso_week_52_of_2025(self):
        # 2025-12-24 is a Wednesday. ISO week dating assigns a week to whichever year
        # contains its Thursday, and this week's Thursday (12-25) is still in 2025.
        self.assertEqual(periods.week_key_for(date(2025, 12, 24)), (2025, 52))

    def test_same_calendar_week_shares_a_key(self):
        self.assertEqual(periods.week_key_for(date(2025, 12, 22)), (2025, 52))
        self.assertEqual(periods.week_key_for(date(2025, 12, 28)), (2025, 52))

    def test_the_next_monday_rolls_into_iso_week_1_of_the_new_year(self):
        # 2025-12-29's Thursday (2026-01-01) falls in 2026, so the whole week is 2026 week 1
        # even though the week starts in December.
        self.assertEqual(periods.week_key_for(date(2025, 12, 29)), (2026, 1))
        self.assertEqual(periods.week_key_for(date(2026, 1, 1)), (2026, 1))

    def test_several_weeks_later(self):
        self.assertEqual(periods.week_key_for(date(2026, 1, 12)), (2026, 3))


class TestWeekRange(unittest.TestCase):
    def test_iso_week_52_of_2025_is_the_monday_on_or_before_the_earliest_date(self):
        self.assertEqual(periods.week_range(2025, 52), (date(2025, 12, 22), date(2025, 12, 28)))

    def test_iso_week_1_of_2026_follows_immediately(self):
        self.assertEqual(periods.week_range(2026, 1), (date(2025, 12, 29), date(2026, 1, 4)))

    def test_round_trips_with_week_key_for(self):
        for week in range(1, 10):
            start, end = periods.week_range(2026, week)
            self.assertEqual(periods.week_key_for(start), (2026, week))
            self.assertEqual(periods.week_key_for(end), (2026, week))


class TestMonthKeyFor(unittest.TestCase):
    def test_plain_calendar_year_and_month(self):
        self.assertEqual(periods.month_key_for(date(2025, 12, 24)), (2025, 12))
        self.assertEqual(periods.month_key_for(date(2026, 1, 15)), (2026, 1))
        self.assertEqual(periods.month_key_for(date(2026, 12, 1)), (2026, 12))


class TestMonthRange(unittest.TestCase):
    def test_all_of_december(self):
        self.assertEqual(periods.month_range(2025, 12), (date(2025, 12, 1), date(2025, 12, 31)))

    def test_all_of_january(self):
        self.assertEqual(periods.month_range(2026, 1), (date(2026, 1, 1), date(2026, 1, 31)))

    def test_a_december_range_ends_correctly(self):
        self.assertEqual(periods.month_range(2026, 12), (date(2026, 12, 1), date(2026, 12, 31)))

    def test_round_trips_with_month_key_for(self):
        for month in range(1, 13):
            start, end = periods.month_range(2026, month)
            self.assertEqual(periods.month_key_for(start), (2026, month))
            self.assertEqual(periods.month_key_for(end), (2026, month))


class TestDatesInRange(unittest.TestCase):
    def test_lists_every_date_inclusive(self):
        result = periods.dates_in_range(date(2026, 1, 1), date(2026, 1, 3))

        self.assertEqual(result, [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)])

    def test_clips_below_to_the_earliest_lamp_bus_data(self):
        # ISO week 52 of 2025 starts on the Monday, but LAMP data only begins on the Wednesday.
        start, end = periods.week_range(2025, 52)

        result = periods.dates_in_range(start, end)

        self.assertEqual(result[0], date(2025, 12, 24))
        self.assertEqual(result[-1], date(2025, 12, 28))

    def test_clips_above_to_not_after(self):
        result = periods.dates_in_range(date(2026, 1, 1), date(2026, 1, 10), not_after=date(2026, 1, 3))

        self.assertEqual(result, [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)])

    def test_returns_empty_when_the_range_is_entirely_in_the_future(self):
        result = periods.dates_in_range(date(2026, 1, 5), date(2026, 1, 10), not_after=date(2026, 1, 1))

        self.assertEqual(result, [])
