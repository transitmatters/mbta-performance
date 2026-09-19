import unittest
from datetime import date

from .. import periods


class TestWeekNumberFor(unittest.TestCase):
    def test_earliest_data_falls_in_week_1(self):
        # 2025-12-24 is a Wednesday; week 1 is the Mon-Sun week containing it.
        self.assertEqual(periods.week_number_for(date(2025, 12, 24)), 1)

    def test_same_calendar_week_shares_a_number(self):
        self.assertEqual(periods.week_number_for(date(2025, 12, 22)), 1)
        self.assertEqual(periods.week_number_for(date(2025, 12, 28)), 1)

    def test_the_next_monday_starts_week_2(self):
        self.assertEqual(periods.week_number_for(date(2025, 12, 29)), 2)

    def test_several_weeks_later(self):
        # Mondays: week 1 = 12/22, week 2 = 12/29, week 3 = 1/5, week 4 = 1/12.
        self.assertEqual(periods.week_number_for(date(2026, 1, 12)), 4)


class TestWeekRange(unittest.TestCase):
    def test_week_1_is_the_monday_on_or_before_the_earliest_date(self):
        self.assertEqual(periods.week_range(1), (date(2025, 12, 22), date(2025, 12, 28)))

    def test_week_2_follows_immediately(self):
        self.assertEqual(periods.week_range(2), (date(2025, 12, 29), date(2026, 1, 4)))

    def test_round_trips_with_week_number_for(self):
        for week in range(1, 10):
            start, end = periods.week_range(week)
            self.assertEqual(periods.week_number_for(start), week)
            self.assertEqual(periods.week_number_for(end), week)

    def test_rejects_a_non_positive_week(self):
        with self.assertRaises(ValueError):
            periods.week_range(0)


class TestMonthNumberFor(unittest.TestCase):
    def test_earliest_data_falls_in_month_1(self):
        self.assertEqual(periods.month_number_for(date(2025, 12, 24)), 1)

    def test_january_is_month_2(self):
        self.assertEqual(periods.month_number_for(date(2026, 1, 15)), 2)

    def test_a_year_later_advances_by_twelve(self):
        self.assertEqual(periods.month_number_for(date(2026, 12, 1)), 13)


class TestMonthRange(unittest.TestCase):
    def test_month_1_is_all_of_december(self):
        self.assertEqual(periods.month_range(1), (date(2025, 12, 1), date(2025, 12, 31)))

    def test_month_2_is_all_of_january(self):
        self.assertEqual(periods.month_range(2), (date(2026, 1, 1), date(2026, 1, 31)))

    def test_a_december_range_ends_correctly(self):
        self.assertEqual(periods.month_range(13), (date(2026, 12, 1), date(2026, 12, 31)))

    def test_round_trips_with_month_number_for(self):
        for month in range(1, 15):
            start, end = periods.month_range(month)
            self.assertEqual(periods.month_number_for(start), month)
            self.assertEqual(periods.month_number_for(end), month)

    def test_rejects_a_non_positive_month(self):
        with self.assertRaises(ValueError):
            periods.month_range(0)


class TestDatesInRange(unittest.TestCase):
    def test_lists_every_date_inclusive(self):
        result = periods.dates_in_range(date(2026, 1, 1), date(2026, 1, 3))

        self.assertEqual(result, [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)])

    def test_clips_below_to_the_earliest_lamp_bus_data(self):
        # Week 1 starts on the Monday, but LAMP data only begins on the Wednesday.
        start, end = periods.week_range(1)

        result = periods.dates_in_range(start, end)

        self.assertEqual(result[0], date(2025, 12, 24))
        self.assertEqual(result[-1], date(2025, 12, 28))

    def test_clips_above_to_not_after(self):
        result = periods.dates_in_range(date(2026, 1, 1), date(2026, 1, 10), not_after=date(2026, 1, 3))

        self.assertEqual(result, [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)])

    def test_returns_empty_when_the_range_is_entirely_in_the_future(self):
        result = periods.dates_in_range(date(2026, 1, 5), date(2026, 1, 10), not_after=date(2026, 1, 1))

        self.assertEqual(result, [])
