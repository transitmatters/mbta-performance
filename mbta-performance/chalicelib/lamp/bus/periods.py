"""Calendar week/month numbering for bus speed trend rollups.

Weeks and months are numbered sequentially from 1, starting with the calendar week/month
that contains `EARLIEST_LAMP_BUS_DATA` -- so "week 1" is the calendar week (Monday-Sunday)
of 2025-12-24, "week 2" is the following Monday-Sunday week, and so on; "month 1" is
December 2025, "month 2" is January 2026, etc. Numbering this way, rather than ISO year-week
or a Year=/Month= key, keeps the S3 layout and the API simple while bus history is still
only a few months long -- see the daily pipeline's Year=/Month=/Day= keys in constants.py
for the alternative this deliberately avoids.
"""

from datetime import date, timedelta

from .constants import EARLIEST_LAMP_BUS_DATA

_EARLIEST = date.fromisoformat(EARLIEST_LAMP_BUS_DATA)
_EARLIEST_WEEK_START = _EARLIEST - timedelta(days=_EARLIEST.weekday())
_EARLIEST_MONTH_START = _EARLIEST.replace(day=1)


def week_number_for(service_date: date) -> int:
    """1-indexed calendar week (Mon-Sun) since the week containing EARLIEST_LAMP_BUS_DATA."""
    week_start = service_date - timedelta(days=service_date.weekday())
    return (week_start - _EARLIEST_WEEK_START).days // 7 + 1


def week_range(week: int) -> tuple[date, date]:
    """Inclusive (Monday, Sunday) range for a week number, as produced by `week_number_for`."""
    if week < 1:
        raise ValueError(f"week must be >= 1, got {week}")
    start = _EARLIEST_WEEK_START + timedelta(weeks=week - 1)
    return start, start + timedelta(days=6)


def month_number_for(service_date: date) -> int:
    """1-indexed calendar month since the month containing EARLIEST_LAMP_BUS_DATA."""
    return (
        (service_date.year - _EARLIEST_MONTH_START.year) * 12
        + (service_date.month - _EARLIEST_MONTH_START.month)
        + 1
    )


def month_range(month: int) -> tuple[date, date]:
    """Inclusive (first day, last day) range for a month number, as produced by `month_number_for`."""
    if month < 1:
        raise ValueError(f"month must be >= 1, got {month}")
    total_months = (_EARLIEST_MONTH_START.month - 1) + (month - 1)
    year = _EARLIEST_MONTH_START.year + total_months // 12
    start = date(year, total_months % 12 + 1, 1)
    end = date(year, 12, 31) if start.month == 12 else date(year, start.month + 1, 1) - timedelta(days=1)
    return start, end


def dates_in_range(start: date, end: date, not_after: date | None = None) -> list[date]:
    """Every service date in [start, end] that LAMP bus data can actually exist for.

    Clips below to `EARLIEST_LAMP_BUS_DATA` and above to `not_after` (when given), since a
    week's or month's calendar boundaries routinely extend past either -- week 1 starts on
    the Monday two days before LAMP's data actually begins, and the current week/month is
    only partially elapsed.
    """
    clipped_start = max(start, _EARLIEST)
    clipped_end = min(end, not_after) if not_after is not None else end
    if clipped_end < clipped_start:
        return []
    return [clipped_start + timedelta(days=offset) for offset in range((clipped_end - clipped_start).days + 1)]
