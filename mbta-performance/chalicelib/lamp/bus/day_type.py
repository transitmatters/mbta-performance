"""Classify a service date as a business day or a weekend/holiday.

Used by trends.py to split weekly/monthly rollups by day type -- a week or month always
blends weekday and weekend service, and averaging them together would blur exactly the
pattern a rider cares about (e.g. Saturday service running slower or faster than a weekday's).

A weekday is only reclassified as `WEEKEND_OR_HOLIDAY` when MBTA's own GTFS calendar says so.
`calendar_dates.txt`'s `holiday_name` column (present in the LAMP GTFS archive's
`calendar_dates` parquet) is MBTA's own curated list of dates it runs a modified schedule for
-- there is no hand-maintained US-holiday list here to fall out of date or disagree with what
actually ran that day. A Saturday/Sunday that happens to also carry a holiday_name (July 4th
falling on a Saturday, say) is already `WEEKEND_OR_HOLIDAY` by day of week, so nothing turns
on it either way.
"""

from datetime import date

from .constants import BUSINESS_DAY, WEEKEND_OR_HOLIDAY
from .gtfs_geo import read_gtfs_archive_file


def is_mbta_holiday(service_date: date) -> bool:
    """True if MBTA's GTFS calendar_dates.txt flags this date with a holiday_name."""
    frame = read_gtfs_archive_file("calendar_dates", service_date, ["date", "holiday_name"])
    if frame.empty:
        return False
    dateint = int(service_date.strftime("%Y%m%d"))
    return bool(((frame.date == dateint) & frame.holiday_name.notna()).any())


def day_type_for(service_date: date) -> str:
    """BUSINESS_DAY (Mon-Fri, not an MBTA holiday) or WEEKEND_OR_HOLIDAY (Sat/Sun, or a
    weekday MBTA runs a holiday schedule for)."""
    if service_date.weekday() >= 5:
        return WEEKEND_OR_HOLIDAY
    return WEEKEND_OR_HOLIDAY if is_mbta_holiday(service_date) else BUSINESS_DAY
