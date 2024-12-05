from chalice import Chalice, Cron, ConvertToMiddleware
from datadog_lambda.wrapper import datadog_lambda_wrapper
from chalicelib import (
    lamp,
)

app = Chalice(app_name="mbta-performance")

app.register_middleware(ConvertToMiddleware(datadog_lambda_wrapper))


# Runs every 30 minutes from either 5 AM -> 2:30AM or 6 AM -> 3:30 AM depending on DST
@app.schedule(Cron("*/30", "0-7,10-23", "*", "*", "?", "*"))
def process_daily_lamp(event):
    lamp.ingest_today_lamp_data()
