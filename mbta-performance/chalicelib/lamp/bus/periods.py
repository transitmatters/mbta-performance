"""Calendar week/month keys for bus speed trend rollups.

Weeks are keyed by ISO 8601 (year, week) -- `date.isocalendar()`, the standard for
year-boundary weeks, so a week belongs to whichever year contains its Thursday. This means
the very first week of LAMP bus data (Monday 2025-12-22 through Sunday 2025-12-28) is
ISO year 2025, week 52, not "2026 week 1" -- the following week is 2026 week 1. Months are
keyed by plain calendar (year, month), which has no such edge case.

Both differ from the daily pipeline's Year=/Month=/Day= keys in constants.py only in that a
day-of-month isn't part of the key: a period spans many days, so it doesn't have one.
"""

from datetime import date, timedelta

from .constants import EARLIEST_LAMP_BUS_DATA

EARLIEST_DATE = date.fromisoformat(EARLIEST_LAMP_BUS_DATA)


def week_key_for(service_date: date) -> tuple[int, int]:
    """(ISO year, ISO week) for a service date -- see `date.isocalendar()`."""
    iso = service_date.isocalendar()
    return iso.year, iso.week


def week_range(year: int, week: int) -> tuple[date, date]:
    """Inclusive (Monday, Sunday) range for an ISO (year, week), as produced by `week_key_for`."""
    monday = date.fromisocalendar(year, week, 1)
    return monday, monday + timedelta(days=6)


def month_key_for(service_date: date) -> tuple[int, int]:
    """(year, month) for a service date."""
    return service_date.year, service_date.month


def month_range(year: int, month: int) -> tuple[date, date]:
    """Inclusive (first day, last day) range for a calendar (year, month)."""
    start = date(year, month, 1)
    end = date(year, 12, 31) if month == 12 else date(year, month + 1, 1) - timedelta(days=1)
    return start, end


def dates_in_range(start: date, end: date, not_after: date | None = None) -> list[date]:
    """Every service date in [start, end] that LAMP bus data can actually exist for.

    Clips below to `EARLIEST_LAMP_BUS_DATA` and above to `not_after` (when given), since a
    week's or month's calendar boundaries routinely extend past either -- the first ISO week
    of data starts two days before LAMP's data actually begins, and the current week/month is
    only partially elapsed.
    """
    clipped_start = max(start, EARLIEST_DATE)
    clipped_end = min(end, not_after) if not_after is not None else end
    if clipped_end < clipped_start:
        return []
    return [clipped_start + timedelta(days=offset) for offset in range((clipped_end - clipped_start).days + 1)]
