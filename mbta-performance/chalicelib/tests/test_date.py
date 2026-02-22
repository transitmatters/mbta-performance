from datetime import date, datetime
from ..date import EASTERN_TIME, format_dateint, get_current_service_date, service_date, to_dateint


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
