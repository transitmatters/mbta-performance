import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from chalice import Chalice, ConvertToMiddleware, Cron
from chalicelib import (
    benchmarks,
    lamp,
)
from datadog_lambda.wrapper import datadog_lambda_wrapper

# Configure logging level from environment variable (default: INFO)
# Set LOG_LEVEL=DEBUG in environment to enable debug logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))

app = Chalice(app_name="mbta-performance")

app.register_middleware(ConvertToMiddleware(datadog_lambda_wrapper))


def _in_dst_dead_zone(now_boston: datetime) -> bool:
    """True between 3-6 AM Boston time, when the 30-minute schedule shouldn't run yet.

    The schedule itself spans 0-7 and 10-23 UTC to cover both DST offsets (5 AM/6 AM start
    depending on time of year); this narrows that down to "6 AM or later Boston time" so we
    don't run before service starts on whichever side of DST we're currently on.
    """
    return 3 <= now_boston.hour < 6


# Runs every 30 minutes from either 5 AM -> 2:30AM or 6 AM -> 3:30 AM depending on DST
@app.schedule(Cron("*/30", "0-7,10-23", "*", "*", "?", "*"))
def process_daily_lamp(event):
    """Ensure execution only happens at 6 AM or later in Boston time."""
    now_boston = datetime.now(ZoneInfo("US/Eastern"))

    # If it's before 6 AM Boston time, exit early to avoid errors
    if _in_dst_dead_zone(now_boston):
        return

    lamp.ingest_today_lamp_data()


# Runs once the next day at 11am or 12pm depending on DST
@app.schedule(Cron("0", "15", "*", "*", "?", "*"))
def process_yesterday_lamp(event):
    """Process yesterday's LAMP data, to ensure we have everything we need."""
    lamp.ingest_yesterday_lamp_data()


# Runs on the 1st of every month at 10:00 UTC (5-6 AM Boston depending on DST).
# Historical p50 barely moves week-to-week so monthly is plenty.
@app.schedule(Cron("0", "10", "1", "*", "?", "*"))
def regenerate_tm_benchmarks(event):
    """Regenerate TransitMatters travel-time benchmarks for rapid transit."""
    benchmarks.generate_travel_time_benchmarks()


# Bus LAMP data processing.
#
# Paused 2026-09-23: every run timed out at 60s. Processing takes ~21s, but the upload is
# 10,520 route-direction-stop CSVs (rail uploads ~473), and at a 30-minute cadence finishing
# them would be ~13.9M S3 PUTs/month (~$69). Left deployed but unscheduled until the cadence
# and object layout are revisited; to resume, swap the decorator back to
# @app.schedule(Cron("*/30", "0-7,10-23", "*", "*", "?", "*")).
@app.lambda_function()
def process_daily_bus_lamp(event, context=None):
    """Ingest today's bus LAMP data."""
    now_boston = datetime.now(ZoneInfo("US/Eastern"))

    if _in_dst_dead_zone(now_boston):
        return

    lamp.ingest_today_bus_data()


# Runs once the next day at 11am or 12pm depending on DST. The 300s timeout is a guess sized
# for one full day of uploads, not a measurement -- right-size it from real runs, as #96 did.
@app.schedule(Cron("0", "15", "*", "*", "?", "*"))
def process_yesterday_bus_lamp(event):
    """Process yesterday's bus LAMP data, to ensure we have everything we need."""
    lamp.ingest_yesterday_bus_data()


# Runs daily at 11:00 UTC (6-7 AM Boston depending on DST), after the LAMP alerts
# parquet has settled for the prior service day.
@app.schedule(Cron("0", "11", "*", "*", "?", "*"))
def process_lamp_alerts(event):
    """Rebuild the rolling window of Alerts/lamp/{date}.json.gz day files from the LAMP alerts archive."""
    lamp.ingest_lamp_alerts()
