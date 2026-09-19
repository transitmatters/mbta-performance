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

# Two-way split of service dates for the weekly/monthly trend rollups in trends.py --
# business_day is Monday-Friday minus MBTA-observed holidays, weekend_or_holiday is
# Saturday, Sunday, and any weekday MBTA runs a holiday schedule for. See day_type.py.
BUSINESS_DAY = "business_day"
WEEKEND_OR_HOLIDAY = "weekend_or_holiday"

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

# Weekly/monthly trend rollups (trends.py), keyed by (year, period number) rather than a
# single running integer -- see periods.py for how week (ISO 8601) and month (calendar)
# numbers are assigned. Year/week/month are not zero-padded, matching the daily keys above.
WEEKLY_S3_KEY_TEMPLATE = "BusSpeedSegments/weekly/Year={year}/Week={week}/segments.parquet"
WEEKLY_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/weekly/Year={year}/Week={week}/segments.pmtiles"
MONTHLY_S3_KEY_TEMPLATE = "BusSpeedSegments/monthly/Year={year}/Month={month}/segments.parquet"
MONTHLY_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/monthly/Year={year}/Month={month}/segments.pmtiles"

# Same keys again, but the "slowest segments" leaderboard as plain JSON -- see leaderboard.py.
# Fetched directly by the frontend (same CloudFront-backed path as the GeoParquet/PMTiles
# siblings above), unlike the route leaderboard's Dynamo-scan-and-cache-to-S3 approach: the
# aggregated table is already in hand at generation time, so there's nothing to scan later.
LEADERBOARD_KEY_TEMPLATE = "BusSpeedSegments/daily/Year={YYYY}/Month={_M}/Day={_D}/leaderboard.json"
WEEKLY_LEADERBOARD_KEY_TEMPLATE = "BusSpeedSegments/weekly/Year={year}/Week={week}/leaderboard.json"
MONTHLY_LEADERBOARD_KEY_TEMPLATE = "BusSpeedSegments/monthly/Year={year}/Month={month}/leaderboard.json"

# Entries kept per (day_type, time_band) slice of the leaderboard. This is a display list, not
# an analytical export -- the full ranked table already exists as GeoParquet/PMTiles -- so it's
# capped well below the ~10.8k distinct segments/day rather than dumping the whole table.
LEADERBOARD_SIZE = 100

# Higher than MIN_TRAVERSALS_HINT, and deliberately a separate constant: verified against real
# data (2026-09-17/18), a segment's *slowest* end is dominated by 3-6-traversal noise (a single
# bus stuck at a light) up through that threshold -- on the daily file, 90-100% of the top 10
# slowest entries per band had fewer than 10 traversals. MIN_TRAVERSALS_HINT is fine for the
# map, where a thin segment is one faint line among thousands; a leaderboard entry is a much
# louder claim ("the #1 slowest segment"), so it needs a real sample behind it. A single day
# only sees a handful of trips through most segments in a given time band, so this thins the
# daily leaderboard considerably (some bands may have few or no qualifying entries) -- the
# weekly/monthly rollups pool traversals across many days and comfortably clear this bar.
LEADERBOARD_MIN_TRAVERSALS = 20

# Daily per-route speed rollup, a coarser companion to the per-segment GeoParquet above --
# one row per (route, service_date) rather than per segment, for the same kind of "how fast
# is this route" line chart the dashboard already draws for rail from the DeliveredTripMetrics
# family of tables. Named separately from that family: bus has ~150 independent routes rather
# than a handful of lines with fixed branches and a nominal track length.
DYNAMO_TABLE_NAME = "DeliveredTripMetricsBus"
