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
    "scheduled_tt",
    "scheduled_headway",
    "scheduled_headway_branch",
]


# Live data will sometimes report an aliased version of stop_id different
# from that which GTFS reports in its schedule. These are the known id's.
STOP_ID_NUMERIC_MAP = {
    "Forest Hills-01": "70001",
    "Forest Hills-02": "70001",
    "Braintree-01": "70105",
    "Braintree-02": "70105",
    "Oak Grove-01": "70036",
    "Oak Grove-02": "70036",
    "Union Square-01": "70504",
    "Union Square-02": "70504",
}
