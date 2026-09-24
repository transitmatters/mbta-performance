from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

EASTERN_TIME = ZoneInfo("US/Eastern")

# Service days run from this hour, Eastern time, to the same hour the next day.
SERVICE_DAY_START_HOUR = 3


def to_dateint(date: date):
    """turn date into 20220615 e.g."""
    return int(str(date).replace("-", ""))


def service_date(ts: datetime) -> date:
    """
    Return the service date for a given timestamp in Eastern time.
    """
    # In practice a None TZ is UTC, but we want to be explicit
    # In many places we have an implied eastern
    ts = ts.replace(tzinfo=EASTERN_TIME)

    if ts.hour >= SERVICE_DAY_START_HOUR and ts.hour <= 23:
        return date(ts.year, ts.month, ts.day)

    prior = ts - timedelta(days=1)
    return date(prior.year, prior.month, prior.day)


def service_day_start(day: date) -> datetime:
    """
    Return the Eastern-time instant a given service date begins -- the complement
    of service_date(): every timestamp t for which service_date(t) == day satisfies
    service_day_start(day) <= t < service_day_start(day + 1 day).
    """
    return datetime(day.year, day.month, day.day, SERVICE_DAY_START_HOUR, tzinfo=EASTERN_TIME)


def get_current_service_date() -> date:
    """
    Returns the current service date in Eastern time.
    """
    return service_date(datetime.now(EASTERN_TIME))


def format_dateint(dtint: int) -> str:
    """Safely takes a dateint of YYYYMMDD to YYYY-MM-DD."""
    return datetime.strptime(str(dtint), "%Y%m%d").strftime("%Y-%m-%d")
