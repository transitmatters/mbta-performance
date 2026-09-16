"""Constants for LAMP bus speed-segment processing."""

# LAMP publishes bus events as two Tableau/OPMI exports. Both share a schema and are
# partitioned with exactly one parquet row group per service_date, so a single day can
# be pulled with HTTP range reads instead of downloading the whole file.
BUS_RECENT_URL = "https://performancedata.mbta.com/lamp/tableau/bus/LAMP_RECENT_Bus_Events.parquet"
BUS_ALL_URL = "https://performancedata.mbta.com/lamp/tableau/bus/LAMP_ALL_Bus_Events.parquet"

# LAMP_ALL_Bus_Events currently begins here. Earlier bus data has to come from the
# ArcGIS monthly archive handled in chalicelib/historic.
EARLIEST_LAMP_BUS_DATA = "2025-12-24"

# GTFS archive published by LAMP, one parquet per year per GTFS file. Each row carries
# gtfs_active_date/gtfs_end_date so a single service date can be sliced out.
GTFS_ARCHIVE_URL_TEMPLATE = "https://performancedata.mbta.com/lamp/gtfs_archive/{year}/{filename}.parquet"

# Columns read from the LAMP bus export. Deliberately narrow: the full schema is 61 columns.
BUS_COLUMNS = [
    "service_date",
    "route_id",
    "route_pattern_id",
    "route_pattern_typicality",
    "trip_id",
    "tm_pullout_id",
    "direction_id",
    "stop_id",
    "stop_sequence",
    "gtfs_stop_sequence",
    "vehicle_label",
    # Seconds after midnight of the service date. These agree with the *_dt columns and
    # exceed 86400 for after-midnight trips. Prefer them over plan_*_dt -- see below.
    "stop_arrival_seconds",
    "stop_departure_seconds",
    "plan_stop_departure_sam",
    "point_type",
]

# NOTE: the plan_*_dt columns in the LAMP bus export are offset from the rest of the feed
# by one UTC offset (verified: plan_stop_departure_sam=16620 vs plan_stop_departure_dt=00:37
# on 2026-09-03, a constant 14400s difference). The seconds-after-midnight columns are
# self-consistent with the actuals, so this module uses only those.

# Time-of-day bands, as (name, start, end) in seconds after midnight of the service date.
# The final band runs past 86400 to catch after-midnight trips still on the same service date.
TIME_BANDS = (
    ("early_am", 0, 7 * 3600),
    ("am_peak", 7 * 3600, 9 * 3600),
    ("midday", 9 * 3600, 16 * 3600),
    ("pm_peak", 16 * 3600, 18 * 3600 + 1800),
    ("evening", 18 * 3600 + 1800, 22 * 3600),
    ("late_night", 22 * 3600, 32 * 3600),
)

# A segment traversal is discarded outright if it implies a speed outside these bounds --
# these come from GPS noise, layovers counted as travel, and mis-snapped stops.
MIN_PLAUSIBLE_SPEED_MPH = 0.5
MAX_PLAUSIBLE_SPEED_MPH = 65.0

# Percentiles reported per (segment, date, band).
PERCENTILES = (50, 90)

# Aggregation buckets with fewer than this many traversals are still emitted, but a
# consumer will usually want to filter on n_traversals for map rendering.
MIN_TRAVERSALS_HINT = 3

METERS_PER_SECOND_TO_MPH = 2.2369362920544
METERS_PER_MILE = 1609.344


# Output lives in the same bucket as the rest of the performance archive. Month and day are
# not zero-padded, matching the existing Events-lamp/ and Events/ key layouts.
S3_BUCKET = "tm-mbta-performance"
S3_KEY_TEMPLATE = "BusSpeedSegments/daily/Year={YYYY}/Month={_M}/Day={_D}/segments.parquet"

# Same key, but the PMTiles vector tileset the live map reads instead of the GeoParquet
# above -- see pmtiles.py.
PMTILES_KEY_TEMPLATE = "BusSpeedSegments/daily/Year={YYYY}/Month={_M}/Day={_D}/segments.pmtiles"

# Daily per-route speed rollup, a coarser companion to the per-segment GeoParquet above --
# one row per (route, service_date) rather than per segment, for the same kind of "how fast
# is this route" line chart the dashboard already draws for rail from the DeliveredTripMetrics
# family of tables. Named separately from that family: bus has ~150 independent routes rather
# than a handful of lines with fixed branches and a nominal track length.
DYNAMO_TABLE_NAME = "DeliveredTripMetricsBus"
