# LAMP columns to fetch from parquet files
LAMP_COLUMNS = [
    "service_date",
    "route_id",
    "trip_id",
    "stop_id",
    "direction_id",
    "stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "move_timestamp",  # departure time from the previous station
    "stop_timestamp",  # arrival time at the current station
    # BENCHMARKING COLUMNS
    "travel_time_seconds",
    "dwell_time_seconds",
    "headway_trunk_seconds",
    "headway_branch_seconds",
    "scheduled_travel_time",
    "scheduled_headway_trunk",
    "scheduled_headway_branch",
]

# columns that should be output to s3 events.csv
S3_COLUMNS = [
    "service_date",
    "route_id",
    "trip_id",
    "direction_id",
    "stop_id",
    "stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "event_type",
    "event_time",
    "travel_time_seconds",
    "dwell_time_seconds",
    "headway_seconds",
    "headway_branch_seconds",
    "scheduled_travel_time",
    "scheduled_headway",
    "scheduled_headway_branch",
]
