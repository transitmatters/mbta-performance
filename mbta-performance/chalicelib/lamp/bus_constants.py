# Remote URL for fetching bus LAMP data (daily parquet files)
BUS_DAILY_URL_TEMPLATE = "https://performancedata.mbta.com/lamp/bus_vehicle_events/{YYYYMMDD}.parquet"

# Columns to read from the source parquet files
BUS_LAMP_COLUMNS = [
    "service_date",
    "route_id",
    "trip_id",
    "stop_id",
    "direction_id",
    "stop_sequence",
    "vehicle_label",
    "previous_stop_id",
    # Actual timestamps (UTC-aware)
    "stop_arrival_dt",
    "stop_departure_dt",
    # Actual metrics
    "travel_time_seconds",
    "stopped_duration_seconds",
    "route_direction_headway_seconds",
    # Scheduled metrics
    "plan_travel_time_seconds",
    "plan_route_direction_headway_seconds",
]

# Columns output to S3 events.csv
BUS_S3_COLUMNS = [
    "service_date",
    "route_id",
    "trip_id",
    "direction_id",
    "stop_id",
    "stop_sequence",
    "vehicle_label",
    "event_type",
    "event_time",
    "travel_time_seconds",
    "dwell_time_seconds",
    "headway_seconds",
    "scheduled_tt",
    "scheduled_headway",
]

BUS_COLUMN_RENAME_MAP = {
    "stopped_duration_seconds": "dwell_time_seconds",
    "route_direction_headway_seconds": "headway_seconds",
    "plan_travel_time_seconds": "scheduled_tt",
    "plan_route_direction_headway_seconds": "scheduled_headway",
}

# Output S3 bucket and key template
BUS_S3_BUCKET = "tm-mbta-performance"
# month and day are not zero-padded
BUS_S3_KEY_TEMPLATE = (
    "Events-lamp/bus-daily-data/{route_id}-{direction_id}-{stop_id}/Year={YYYY}/Month={_M}/Day={_D}/events.csv"
)
