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
    # Only for the per-route daily metrics in daily_metrics.py, read from the raw events.
    "route_direction_headway_seconds",
    "plan_route_direction_headway_seconds",
    "waiver_remark",
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

# The whole-day band segments.aggregate_segments emits alongside TIME_BANDS, for the map's
# "All day" filter. Deliberately not in TIME_BANDS: that tuple is a set of non-overlapping
# departure windows for assign_time_band, and every traversal already falls in one of them.
# The all_day rows are aggregated from the same traversals directly -- a median of the band
# medians is not the day's median, so it can't be derived from the band rows afterwards.
# GeoParquet and the leaderboard carry these rows alongside the bands; PMTiles puts them in a
# separate archive (ALL_DAY_PMTILES_KEY_TEMPLATE below).
ALL_DAY_BAND = "all_day"

# A segment traversal is discarded outright if its arrival-to-arrival speed is above this, from
# GPS noise or mis-snapped stops. A running speed above it nulls only the running time. See
# segments.build_traversals.
MAX_PLAUSIBLE_SPEED_MPH = 65.0

# Two-way split of service dates for the weekly/monthly trend rollups in trends.py --
# business_day is Monday-Friday minus MBTA-observed holidays, weekend_or_holiday is
# Saturday, Sunday, and any weekday MBTA runs a holiday schedule for. See day_type.py.
BUSINESS_DAY = "business_day"
WEEKEND_OR_HOLIDAY = "weekend_or_holiday"

# Percentiles reported per (segment, date, band), for both the running and the total clock.
PERCENTILES = (10, 50, 90)

# LAMP's bus departure and arrival times aren't taken at the stop itself, so departure from one
# stop to arrival at the next -- the running time -- covers only part of the segment. On
# 2026-11-04 (322k traversals) running time fit 0.168 s/m x (length - 91m): it hits zero on a
# 91m segment. Over the full length, that read a median 28 mph on 200-250m segments and 40 mph
# on 100-150m ones. Over the length less this zone, those come to 16.7 and 7-12 mph, and every
# length band from 125m up falls between 12 and 17 mph. Separate fits put the zone at 76m
# mid-trip, 70m on a trip's first segment and 41m on its last, so this is an average, not a
# measured fact. See "Running speed" in the README.
STOP_ZONE_M = 90

# Below this, a segment's running length is too little of it to give a speed: on 2026-11-04,
# 100-125m segments still read a median 7 mph after the correction, and under 100m it goes
# negative. 3.6% of that day's traversals. Their segments carry no running speed.
MIN_RUNNING_SPEED_SEGMENT_M = 125

# A segment is a terminal section if at least this share of its trips start or end on it. See
# segments._with_terminal_sections.
TERMINAL_SECTION_SHARE = 0.5

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

# The ALL_DAY_BAND rows get a PMTiles archive of their own beside each segments.pmtiles above,
# rather than sharing it with the time bands: an all_day feature has exactly the geometry of
# a band feature, so in one archive tippecanoe's --drop-densest-as-needed always dropped it
# first -- verified on real data, no all_day feature survived at zooms 4-11 -- see pmtiles.py.
ALL_DAY_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/daily/Year={YYYY}/Month={_M}/Day={_D}/segments_all_day.pmtiles"
WEEKLY_ALL_DAY_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/weekly/Year={year}/Week={week}/segments_all_day.pmtiles"
MONTHLY_ALL_DAY_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/monthly/Year={year}/Month={month}/segments_all_day.pmtiles"

# Dwell at each stop, as points for the map, beside each segments.pmtiles above -- see dwells.py.
# Split by band and all_day the same way, and for the same reason: an all_day point sits exactly
# on a band point.
DWELL_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/daily/Year={YYYY}/Month={_M}/Day={_D}/dwells.pmtiles"
DWELL_ALL_DAY_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/daily/Year={YYYY}/Month={_M}/Day={_D}/dwells_all_day.pmtiles"
WEEKLY_DWELL_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/weekly/Year={year}/Week={week}/dwells.pmtiles"
WEEKLY_DWELL_ALL_DAY_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/weekly/Year={year}/Week={week}/dwells_all_day.pmtiles"
MONTHLY_DWELL_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/monthly/Year={year}/Month={month}/dwells.pmtiles"
MONTHLY_DWELL_ALL_DAY_PMTILES_KEY_TEMPLATE = "BusSpeedSegments/monthly/Year={year}/Month={month}/dwells_all_day.pmtiles"

# Bus stops and rapid transit stations as a context layer under the speed map -- see
# reference_tiles.py. One fixed key rather than one per date: stops only move when the GTFS
# feed does, and the frontend wants a single URL it never has to compute.
REFERENCE_PMTILES_KEY = "BusSpeedSegments/reference/stops.pmtiles"

# Same keys again, but the "slowest segments" leaderboard as plain JSON -- see leaderboard.py.
# Fetched directly by the frontend (same CloudFront-backed path as the GeoParquet/PMTiles
# siblings above), unlike the route leaderboard's Dynamo-scan-and-cache-to-S3 approach: the
# aggregated table is already in hand at generation time, so there's nothing to scan later.
LEADERBOARD_KEY_TEMPLATE = "BusSpeedSegments/daily/Year={YYYY}/Month={_M}/Day={_D}/leaderboard.json"
WEEKLY_LEADERBOARD_KEY_TEMPLATE = "BusSpeedSegments/weekly/Year={year}/Week={week}/leaderboard.json"
MONTHLY_LEADERBOARD_KEY_TEMPLATE = "BusSpeedSegments/monthly/Year={year}/Month={month}/leaderboard.json"

# Entries kept per (day_type, time_band) slice of the leaderboard. This is a display list, not
# an analytical export -- the full ranked table already exists as GeoParquet/PMTiles -- so it's
# capped well below the ~10.8k distinct segments/day rather than dumping the whole table. It's
# still deep, because the dashboard's route and Frequent Bus filters narrow this slice
# client-side: at 100, a filtered view came out with only a handful of rows. 500 puts a monthly
# file at ~1.7MB of JSON, ~180KB once upload_json gzips it.
LEADERBOARD_SIZE = 500

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

# LAMP marks a trip's timepoints with point_type "start", "mid" or "end". Headway reliability
# counts only the "mid" ones, leaving out terminals as transitmatters/data-ingestion#232 does
# for rail: origin departures are held by dispatchers (73% within 0.75-1.25x of schedule on
# 2026-11-04, against 58% mid-route), and terminal arrivals are past where anyone boards.
HEADWAY_POINT_TYPES = ("mid",)

# Daily counts of headways by actual / scheduled headway, as (field name, lower, upper,
# includes lower, includes upper). Matches the dashboard: exactly 0.5 is bunched (`<= 0.5`) and
# on time is 0.75-1.25 exclusive. Exactly 1.5 goes to the top bucket, mirroring 0.5. Named by
# ratio rather than "bunched"/"on_time" so the dashboard can pick 75-125% or 50-150% later.
# Bucket names and edges are shared with transitmatters/data-ingestion#232; keep them in step.
HEADWAY_RATIO_BUCKETS = (
    ("headways_under_50", float("-inf"), 0.5, True, True),
    ("headways_50_75", 0.5, 0.75, False, True),
    ("headways_75_125", 0.75, 1.25, False, False),
    ("headways_125_150", 1.25, 1.5, True, False),
    ("headways_over_150", 1.5, float("inf"), True, True),
)
