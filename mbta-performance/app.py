import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from chalice import Chalice, ConvertToMiddleware, Cron
from chalicelib import (
    lamp,
)
from datadog_lambda.wrapper import datadog_lambda_wrapper

# Configure logging level from environment variable (default: INFO)
# Set LOG_LEVEL=DEBUG in environment to enable debug logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))

app = Chalice(app_name="mbta-performance")

app.register_middleware(ConvertToMiddleware(datadog_lambda_wrapper))


# Runs every 30 minutes from either 5 AM -> 2:30AM or 6 AM -> 3:30 AM depending on DST
@app.schedule(Cron("*/30", "0-7,10-23", "*", "*", "?", "*"))
def process_daily_lamp(event):
    """Ensure execution only happens at 6 AM or later in Boston time."""
    now_boston = datetime.now(ZoneInfo("US/Eastern"))

    # If it's before 6 AM Boston time, exit early to avoid errors
    if now_boston.hour >= 3 and now_boston.hour < 6:
        return

    lamp.ingest_today_lamp_data()


# Runs once the next day at 11am or 12pm depending on DST
@app.schedule(Cron("0", "15", "*", "*", "?", "*"))
def process_yesterday_lamp(event):
    """Process yesterday's LAMP data, to ensure we have everything we need."""
    lamp.ingest_yesterday_lamp_data()
