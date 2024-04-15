from chalice import Chalice, Cron, ConvertToMiddleware
from datadog_lambda.wrapper import datadog_lambda_wrapper
from chalicelib import (
    lamp,
)

app = Chalice(app_name="mbta-performance")

app.register_middleware(ConvertToMiddleware(datadog_lambda_wrapper))


# Runs every 60 minutes from either 4 AM -> 1:55AM or 5 AM -> 2:55 AM depending on DST
@app.schedule(Cron("0", "0-6,9-23", "*", "*", "?", "*"))
def process_daily_lamp(event):
    lamp.ingest_today_lamp_data()
