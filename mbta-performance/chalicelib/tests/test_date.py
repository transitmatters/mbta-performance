from datetime import date, datetime, timedelta
from ..date import EASTERN_TIME, format_dateint, get_current_service_date, service_date, service_day_start, to_dateint


def test_service_date():
    assert service_date(datetime(2023, 12, 15, 3, 0, 0)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 15, 5, 45, 0)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 15, 7, 15, 0)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 15, 23, 59, 59)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 16, 0, 0, 0)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 16, 2, 59, 59)) == date(2023, 12, 15)


def test_localized_datetime():
    assert service_date(datetime(2023, 12, 15, 3, 0, 0, tzinfo=EASTERN_TIME)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 15, 5, 45, 0, tzinfo=EASTERN_TIME)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 15, 7, 15, 0, tzinfo=EASTERN_TIME)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 15, 23, 59, 59, tzinfo=EASTERN_TIME)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 16, 0, 0, 0, tzinfo=EASTERN_TIME)) == date(2023, 12, 15)
    assert service_date(datetime(2023, 12, 16, 2, 59, 59, tzinfo=EASTERN_TIME)) == date(2023, 12, 15)


def test_edt_vs_est_datetimes():
    assert service_date(datetime(2023, 11, 5, 23, 59, 59, tzinfo=EASTERN_TIME)) == date(2023, 11, 5)
    assert service_date(datetime(2023, 11, 6, 0, 0, 0, tzinfo=EASTERN_TIME)) == date(2023, 11, 5)
    assert service_date(datetime(2023, 11, 6, 1, 0, 0, tzinfo=EASTERN_TIME)) == date(2023, 11, 5)
    assert service_date(datetime(2023, 11, 6, 2, 0, 0, tzinfo=EASTERN_TIME)) == date(2023, 11, 5)
    # 3am EST is 4am EDT
    assert service_date(datetime(2023, 11, 6, 3, 0, 0, tzinfo=EASTERN_TIME)) == date(2023, 11, 6)


def test_to_dateint():
    assert to_dateint(date(2024, 2, 7)) == 20240207
    assert to_dateint(date(2000, 1, 1)) == 20000101
    assert to_dateint(date(1999, 12, 31)) == 19991231


def test_format_dateint():
    assert format_dateint(20240207) == "2024-02-07"
    assert format_dateint(20000101) == "2000-01-01"
    assert format_dateint(19991231) == "1999-12-31"


def test_get_current_service_date_returns_date():
    result = get_current_service_date()
    assert isinstance(result, date)


def test_service_day_start_is_the_inverse_of_service_date():
    day = date(2023, 12, 15)
    start = service_day_start(day)
    assert start == datetime(2023, 12, 15, 3, 0, 0, tzinfo=EASTERN_TIME)
    assert service_date(start) == day
    # one second before the boundary belongs to the prior service day
    assert service_date(start - timedelta(seconds=1)) == date(2023, 12, 14)


def test_service_day_start_across_dst():
    # 2023-11-06 3am EST is the instant after the DST fallback on 11/5-11/6.
    start = service_day_start(date(2023, 11, 6))
    assert start.utcoffset().total_seconds() / 3600 == -5
    assert service_date(start) == date(2023, 11, 6)
