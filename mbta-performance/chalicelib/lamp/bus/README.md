# Bus Speed Segments

Generates per-segment bus speeds — how long a bus took to get from one stop to the next —
as **GeoParquet**, so the result drops straight into QGIS, DuckDB spatial, GeoPandas, Felt
or deck.gl without any conversion step.

```shell
# Local file only -- never touches S3 or DynamoDB.
uv run python -m mbta-performance.chalicelib.lamp.bus.ingest --date 2026-09-03 --out segments.parquet

# Also publish to the performance bucket.
uv run python -m mbta-performance.chalicelib.lamp.bus.ingest --date 2026-09-03 --upload

# Also write daily per-route metrics to DynamoDB (see "Daily route metrics" below).
uv run python -m mbta-performance.chalicelib.lamp.bus.ingest --date 2026-09-03 --write-to-dynamo

# Also build and publish PMTiles for the live map (see "PMTiles" below; requires tippecanoe).
uv run python -m mbta-performance.chalicelib.lamp.bus.ingest --date 2026-09-03 --write-pmtiles

# Also build and publish the slowest-segments leaderboard (see "Slowest-segments leaderboard" below).
uv run python -m mbta-performance.chalicelib.lamp.bus.ingest --date 2026-09-03 --write-leaderboard
```

One row per `(route, direction, from_stop, to_stop, service_date, time_band)`, where
`time_band` is one of six departure windows or `all_day` (see "Time bands" below). A typical
weekday produces ~64k rows -- ~53k band rows plus one `all_day` row for each of the ~10.8k
distinct segments -- across 152 routes, and takes about 40 seconds end to end (2026-09-17:
38s, of which ~15s is reading LAMP and building traversals and ~23s is aggregation).

## Scheduling

This pipeline isn't in `app.py`. It runs as k8s CronJobs from
[basement-dweller](https://github.com/transitmatters/basement-dweller)
(`charts/bus-speed-segments`), from the image the repo-root `Containerfile` builds, which
bundles `tippecanoe` and the `geo` group. The `container` workflow pushes it to
`ghcr.io/transitmatters/mbta-performance:main` on every merge, and the jobs pull it at the start
of each run, so merging here is all it takes to ship a change. Every morning, Boston time:

| Job | Command |
| --- | --- |
| daily | `ingest --upload --write-to-dynamo --write-pmtiles --write-leaderboard` (yesterday) |
| weekly | `trends --latest-week --upload --write-pmtiles --write-leaderboard` |
| monthly | `trends --latest-month --upload --write-pmtiles --write-leaderboard` |
| reference (Mondays) | `reference_tiles --upload` |

`--latest-week` and `--latest-month` build every period holding one of the last three service
dates, newest first. That keeps the dashboard's in-progress week and month current, and it
builds a period that just ended on the morning after its last day and on the two mornings after
that.

A late LAMP morning is why it keeps going back. These runs won't publish a period without its
newest date: a date LAMP hasn't published yet counts as missing, the same thing that fails the
daily job. They read that date first, so a missing one fails in seconds. They still try every
other period, then exit non-zero, so all three jobs fail together and a rerun of each fixes it.
Without a rerun, one of the next two mornings fills in a finished week's or month's last day
by itself. An older date with no data is still skipped and logged, since that's a gap in
LAMP's export rather than lateness. A file that can't be parsed (`ArrowInvalid`, which MBTA's
rewrites can briefly cause) fails the period instead of being skipped as a date without data.
If LAMP is out for longer than that window, rebuild the periods by hand with `--year` and
`--week` or `--month` once it has caught up.

Each extra build runs on basement-dweller and costs 6 PUTs with the dwell layer: about 70 more
PUTs a month, a fraction of a cent. Measured without tiles on 2026-11-11, one week took ~1.5 minutes (1.7GB
peak with the current week built first in the same process). A whole month took ~5 minutes,
with a 5.5GB peak after building the new month's first two days, against the ~8GB a full month
peaks at in production with tiles. Both fit the jobs' memory limits.

## Pipeline

1. **Read one service date** from the LAMP bus export. Both exports are partitioned one
   parquet row group per `service_date`, so `remote_parquet.py` reads the footer over HTTP
   and pulls just the row groups covering that date (one, in practice) — ~35MB, rather than
   downloading the 6.8GB all-time file. The last seven days come from the small rolling export
   and older dates from the all-time one; if the chosen export doesn't have the date,
   `ingest.read_events_for_date` tries the other.
2. **Locate every stop along its route pattern's shape** (`gtfs_geo.py`, `patterns.py`).
3. **Interpolate stop times LAMP did not observe** (`segments.py`).
4. **Build per-traversal times, then aggregate** to percentiles per time band, plus a
   whole-day `all_day` band. Segments are timed on the street only (see "Running speed"), and
   the time at each stop is aggregated separately as dwell (`dwells.py`).
5. **Write GeoParquet** with road-following `LineString` geometry (`geoparquet.py`).
6. **Optionally build PMTiles** from the same result, for the live map (`pmtiles.py`),
   plus a pooled row for each stop pair several routes share (`shared_segments.py`), and
   dwell points (`dwells.py`).

## Time bands

Every traversal is labelled by the time it *departed* its first stop, in seconds after
midnight of the service date (`constants.TIME_BANDS`, `segments.assign_time_band`):

| `time_band` | Departs |
| --- | --- |
| `early_am` | before 7:00 |
| `am_peak` | 7:00-9:00 |
| `midday` | 9:00-16:00 |
| `pm_peak` | 16:00-18:30 |
| `evening` | 18:30-22:00 |
| `late_night` | 22:00 until 8:00 the next morning (after-midnight trips stay on their service date) |
| `all_day` | any of the above |

`all_day` (`constants.ALL_DAY_BAND`) is not a seventh window and is deliberately not in
`TIME_BANDS`. `aggregate_segments` builds it from the same traversals the six bands use --
one extra aggregation with `time_band` dropped from the group key -- and concatenates those
rows onto the band rows. It is never derived from the band rows: `p50_speed_mph` is a
median, and a median of band medians is not the day's median. A traversal that departed
outside every band (before 0:00 or after 32:00) is dropped before both aggregations, so
`all_day` covers exactly the traversals the bands do. In practice there are none: across 38
real days (10.8M traversals) every departure fell between 3:00 and 27:30.

Because `aggregate_segments` is shared, `all_day` reaches every output the bands do -- daily
(`ingest.py`, one row per segment) and weekly/monthly (`trends.py`, one row per segment per
`day_type`) -- with the same columns as any band row. GeoParquet and the leaderboard hold it
alongside the bands; PMTiles puts it in an archive of its own (see "PMTiles" below).

## Output location

```
s3://tm-mbta-performance/BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments.parquet
```

One self-contained GeoParquet per service date, ~5.7MB, holding the whole network. Month
and day are **not** zero-padded, matching the existing `Events-lamp/` and `Events/` keys.

This deliberately breaks from the per-`(stop, day)` CSV layout the rest of the bucket uses.
A map needs every segment at once, so fetching 10.8k objects to draw a single day would be
far slower and more expensive than one GET. Re-running a date overwrites its object in
place, so backfills and corrected re-runs are safe to repeat.

Uploading is opt-in (`--upload`, or `upload=True`), so a local run never writes to the
bucket. `generate_yesterday_speed_segments()` is the production entry point and publishes
by default.

## PMTiles

GeoParquet is for analysts -- QGIS, DuckDB spatial, GeoPandas, Felt. The live dashboard map
reads a **PMTiles** vector tileset instead, fetched byte-range by byte-range straight from
the browser via MapLibre's `pmtiles://` protocol, at the same key with a `.pmtiles`
extension:

```
s3://tm-mbta-performance/BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments.pmtiles
s3://tm-mbta-performance/BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments_all_day.pmtiles
```

`pmtiles.py` builds it from the same `result` frame the GeoParquet above is written from --
one `tippecanoe` invocation, writing every segment into a single vector layer named
`segments` (matching `modules/busspeedmap/constants.ts`'s `PMTILES_SOURCE_LAYER` in the
dashboard repo). Only the columns the map actually reads (`TILE_PROPERTIES`: route, direction,
stop names, time band, `p50_running_speed_mph`, traversal counts, `segment_length_m`,
`running_length_m`, `p50_running_time_seconds`, `p10_running_speed_mph`,
`p90_running_speed_mph`, `terminal_section` and `shared_routes`) are handed to tippecanoe, so
the arrival-to-arrival and dwell columns can never end up on a segment by accident -- see
`modules/busspeedmap/types.ts`'s `BusSpeedSegmentProperties` for the matching frontend type.
They're rounded to what the dashboard displays first (speed to 0.1 mph, length and time to whole
metres and seconds, the speed range to two significant figures), which keeps the archive smaller
than full-precision floats would.

**The map's speeds are running speeds** (see "Running speed" below), named `running_` rather than
reusing `p50_speed_mph`: tiles built before 2026-11 carry that name for arrival to arrival, so
the map can't mix the two clocks across old and new archives without knowing. A segment under
`MIN_RUNNING_SPEED_SEGMENT_M` has no running speed at all, which tippecanoe leaves off the
feature; the map should draw it as no data.

**`all_day` rows are a separate archive, `segments_all_day.pmtiles`;** `segments.pmtiles`
holds only the six time bands. Both use the same `segments` layer and the same properties
(an all-day feature has `time_band = "all_day"`), so the map can point one layer config at
either URL. They're split because an all_day feature has exactly the geometry of a band
feature, and `--drop-densest-as-needed` drops exact duplicates first. Measured with both in
one archive (2026-09-17): no all_day feature survived at zooms 4-11 -- the dashboard opens
at zoom 11 -- and zoom 11 went over the 500KB tile limit and lost 86% of its band features.
Split (`pmtiles.split_all_day`), the band archive is exactly what it was before all_day
existed, and the all-day archive keeps essentially every segment from zoom 8 up (~60% at
zoom 4) and every route at `MINIMUM_ZOOM`, checked on a real day, week (2026-W38) and month
(2026-08). It costs 6.5MB a day (9.2MB a week, 9.6MB a month) and 2-3s of tippecanoe.

The band archive thins out at low zoom on its own, which predates all_day: at zooms 4-10
only 8-14% of band features survive on a daily file, and 5-7% through zoom 11 on a
weekly/monthly one, because every segment's geometry is repeated once per band and the
duplicates are the "densest" features. Every route is still present at `MINIMUM_ZOOM`.

**Stop pairs several routes share get one more, pooled row** (`shared_segments.py`), in
the tiles only. Where routes run between the same two stops -- SL4 and SL5 on Washington St,
SL1-SL3 through the Silver Line tunnel -- each route's own line is drawn exactly over the
others, and the colour you'd see is whichever is on top. The pooled row is the median of
every route's traversals together, and the map draws it in their place whenever all of them
are selected (including when no route is). Its `route_id` is the routes' ids sorted and
comma-joined (`"749,751"`), and every row on the pair, pooled or not, carries that string as
`shared_routes`; it's null, and so left off the feature, everywhere else. Only routes the map
draws (`MIN_TRAVERSALS_HINT` or more traversals) pool, and only when they agree on
`direction_id` and their lengths are within 25m.

Pooled rows go first in the input, so where tippecanoe thins out the low zooms and keeps
one of a run of identical lines, it keeps them. They also make the densest tile at a zoom up
to 25% bigger, since shared corridors cluster downtown, and `--drop-densest-as-needed` thins
a whole zoom when one tile overflows. At tippecanoe's default 500KB, that tipped the lowest
complete zoom into thinning (2026-W44's zoom 10 went from every segment to 15% of them), so
`MAXIMUM_TILE_BYTES` raises the limit by those 25%, to 625KB. Checked against the same data
without pooled rows at 500KB, on 2026-10-28, 2026-10-29, 2026-W44 and September and October
2026: no zoom has fewer segments, and on a day zoom 9 now holds every one (5,417 stop pairs in
2026-10-29's am_peak, against 751).

Measured on 2026-10-29, the band and all_day archives grow 17% and 7% for the day (most of
that is zoom 9 filling in), 10% and 8% for the week, and 10% and 10% for the month: about 1GB
more S3 a year, and about 10% more tile transfer for whatever part of the map is viewed.
Pooling takes 2s for a day, 4s for a week and 12s for a month. The GeoParquet and the
leaderboard stay one row per route -- `"749,751"` isn't a route.

**Dwell is a layer of points of its own** (`dwells.py`), since the segments leave it out:

```
s3://tm-mbta-performance/BusSpeedSegments/daily/Year=2026/Month=9/Day=3/dwells.pmtiles
s3://tm-mbta-performance/BusSpeedSegments/daily/Year=2026/Month=9/Day=3/dwells_all_day.pmtiles
```

and the same beside each weekly and monthly `segments.pmtiles`, split by band and `all_day` the
same way. One point per (route, direction, stop, time band), and `day_type` on a week or month,
in a layer named `dwells` (`DWELL_TILE_PROPERTIES`: route, direction, `stop_id`, `stop_name`, time
band, `n_dwells`, `median_dwell_seconds` and `p90_dwell_seconds`). Each sits where the stop falls
on the route's shape -- the start of the segment leaving it -- so it lands on the route's line, not
across the street at the pole. `stop_id` matches the reference layer's bus stops.

Only dwells LAMP observed count (`segments.has_observed_dwell`): a trip's first stop is layover,
an interpolated stop has none, and a trip's last stop has no departure. They're the same dwells
`total_dwell` sums in "Daily route metrics", and the ones the GeoParquet's per-segment
`median_dwell_seconds` is over, which is null on a segment with none, as one every trip starts
on. On 2026-11-10, counting those zeros pulled down 24% of segment rows' medians, by 2.5s at the
median. A median above a few minutes looks like a bus holding mid-trip, like the midday trips
that wait 20-odd minutes on Ave Louis Pasteur. Points run from zoom 11, where the reference
layer's bus stops start, to 14, and every one is kept at every zoom (`--drop-rate 1`):
tippecanoe's default point thinning left 2026-11-04's zoom 11 with 3.5k of its 54k band points.

Dwell is rounded to 5s because stop times don't look any finer: medians pile up around 5.5s
and 11s, which looks like the spacing of the vehicle positions behind them.

On 2026-11-04 that's 50k band points and 10k all-day ones, 1.9MB and 1.0MB. Nothing else in
the archives changed size much: taking dwell and `from_stop_id` off the segments made the band
and all-day archives 1% and 9% smaller (and the band archive now keeps 57k features at zooms
6-8, against 7k), while the GeoParquet grew 12% with the running columns. About 2.9MB a day more
in all: roughly 1GB more S3 a year, $0.02/month after a year, and 6 more PUTs a day across the
daily, weekly and monthly jobs.

**`p10_running_speed_mph` and `p90_running_speed_mph` are the range around the median in the
segment popup**, as "8 in 10 trips ran 2.6-7.5 mph". A section that's always slow and one a few trips hold up can
share a median; and where even the fastest trips are slow, it's the street, not the traffic.
Both are running speeds, like `p50_running_speed_mph`. They're only written for rows with 10 or
more trips, since "in 10" means nothing with fewer: that leaves them off 76% of a day's band
rows, and took its densest zoom 9 tile from 861KB to 771KB when they were added. Most sections are consistently
slow rather than held up: across segments under 5 mph on 2026-10-28, p90 time was 1.5 times p50
at the median, about the same as for every other segment.

**`terminal_section` marks the first and last segments of most trips**: `"first"` where most
trips over a segment start on it, `"last"` where most end on it, and null, so left off the
feature, everywhere else. The map dashes them from zoom 13, and the segment popup says which it is. Only
the very end of a trip reads differently, by median arrival-to-arrival speed per traversal (on
running speed, on 2026-11-04, first segments ran 11.1 mph and last ones 10.0, against 15.3 for
the rest):

| | Last segment | The one before | Second segment | First segment |
| --- | --- | --- | --- | --- |
| 2026-10-28 (Wed) | 9.0 mph | 11.9 mph | 11.9 mph | 15.1 mph |
| 2026-10-20 (Tue) | 8.9 mph | 11.8 mph | 11.9 mph | 14.9 mph |
| 2026-10-25 (Sun) | 10.0 mph | 13.4 mph | 13.2 mph | 15.7 mph |

The first segment is quicker because its layover isn't dwell (see below). The last ones are 3% of
segments but 35-50% of those under 3 mph, so they're much of what looks oddly slow on the map.
The flag costs 0.3% of an archive. The leaderboard leaves terminal sections off (see
"Slowest-segments leaderboard").

These columns grow the densest tile at a zoom 13-14% for dwell and `from_stop_id`, then 11% on a
day and 22% on a week for the speed range, so `MAXIMUM_TILE_BYTES` rises to 825KB; at 750KB, a
day's zoom 9 falls to 15% of its segments. Measured on 2026-10-29 against the archives before any
of them, the band and all_day archives grow 21% and 24% for the day, 30% and 24% for the week,
and 29% and 25% for the month, with no zoom losing segments. The GeoParquet gains
`p10_total_time_seconds`, `p10_speed_mph` and `terminal_section`, growing 13-14%. Together that's
about 2.3GB more S3 a year, or $0.05/month more after a year, and 21-30% more tile transfer for
whatever part of the map is viewed. p10 adds about 4s to aggregating a month.

**Requires `tippecanoe` on `PATH`.** It's a system binary, not a Python dependency -- there's
nothing to add to `pyproject.toml` for it. Install it via `apt install tippecanoe` (the
`Containerfile` does the same); `build_pmtiles_bytes` raises a
clear error if it's missing rather than a bare `FileNotFoundError`.

Writing is opt-in (`--write-pmtiles`, or `write_pmtiles=True`) and independent of `--upload`
and `--write-to-dynamo`. `generate_yesterday_speed_segments()` writes all three by default.

## Reference layer: stops and stations

A separate, rarely-changing PMTiles file of bus stops and rapid transit stations, drawn under
the speed segments so a viewer can tell where in the system they're looking. It is built
independently of the segment tiles because stops only move when the GTFS feed does --
rebuild it when a new feed goes live, not with every daily ingest.

```shell
# Uses the GTFS feed active on --date (default yesterday).
uv run python -m mbta-performance.chalicelib.lamp.bus.reference_tiles --date 2026-09-20 --upload
```

```
s3://tm-mbta-performance/BusSpeedSegments/reference/stops.pmtiles
```

One file, two layers (`reference_tiles.py`):

- `stations` -- rapid transit parent stations (subway, light rail, Mattapan) with `lines`
  (`"Red,Green"`; Green-B..E collapse to `Green`) and `routes` (`"Red,Green-B,Green-C"`).
  MBTA parent stations have no `vehicle_type` of their own, so which lines serve a station
  is rolled up from its platforms' trips. Present from zoom 4, the segment tiles' minimum.
- `bus_stops` -- bus stop poles with `routes` (`"1,47"`). Present from zoom 11 only, via
  tippecanoe's per-feature `minzoom`; ~7k points would be noise any further out.

Both lists are comma-joined in GTFS `route_sort_order`. Routes with MBTA's `listed_route = 1`
(the ~200 Rail Replacement Bus shuttles, which are `route_type` 3 and would otherwise look like
bus routes) are left out, and a bus stop that only such routes serve -- or nothing does -- is
dropped. Every point is kept at every zoom it's visible at (`--drop-rate 1`); the whole file is
~100KB.

## Weekly and monthly trend tiles

The daily PMTiles above are for "how fast was the network on this specific day." `trends.py`
builds the same kind of tileset rolled up across a calendar week or month instead, for
slower-moving patterns -- a detour that lasts a season, a route that's consistently faster
off-peak -- that would otherwise mean flipping through individual days.

```shell
# Weeks are ISO 8601 (year, week); months are plain calendar (year, month) -- see below.
uv run python -m mbta-performance.chalicelib.lamp.bus.trends --year 2026 --week 6 --upload --write-pmtiles
uv run python -m mbta-performance.chalicelib.lamp.bus.trends --year 2026 --month 2 --upload --write-pmtiles

# The week or month containing yesterday, as the scheduled jobs run it.
uv run python -m mbta-performance.chalicelib.lamp.bus.trends --latest-week --upload --write-pmtiles

# Backfill every week/month LAMP has data for (default range: EARLIEST_LAMP_BUS_DATA..yesterday).
uv run python -m mbta-performance.chalicelib.lamp.bus.trends_backfill --weeks --months --upload --write-pmtiles
```

```
s3://tm-mbta-performance/BusSpeedSegments/weekly/Year=2026/Week=6/segments.pmtiles
s3://tm-mbta-performance/BusSpeedSegments/weekly/Year=2026/Week=6/segments_all_day.pmtiles
s3://tm-mbta-performance/BusSpeedSegments/monthly/Year=2026/Month=2/segments.pmtiles
s3://tm-mbta-performance/BusSpeedSegments/monthly/Year=2026/Month=2/segments_all_day.pmtiles
```

**Weeks use ISO 8601 (year, week); months use plain calendar (year, month).** `periods.py`
computes both from `date.isocalendar()` and `date.year`/`date.month` respectively, rather
than a single running integer -- a Year=/Week=/Month= key that means what it looks like
(week 6 of 2026, not "the 37th week since some fixed start date") is worth the small extra
key complexity once trend tiles span more than a year. The one surprise: ISO week dating
assigns a week to whichever year contains its Thursday, so the very first week of LAMP bus
data (Monday 2025-12-22 through Sunday 2025-12-28) is `Year=2025/Week=52`, not "2026 week 1"
-- the following week is `Year=2026/Week=1`. `week_range(year, week)` / `month_range(year,
month)` convert a key back to its calendar boundaries; `dates_in_range` then clips that range
to whatever LAMP data actually exists (before `EARLIEST_LAMP_BUS_DATA`, or later than
yesterday). `trends_backfill.py` walks calendar dates (one Monday at a time for weeks, one
first-of-month at a time for months) rather than iterating an integer range, mirroring
`backfill.py`'s `--start-date`/`--end-date` interface.

**Percentiles are recomputed from every underlying traversal, not averaged from the daily
p50/p90 already published by `ingest.py`.** A percentile of percentiles is a different (and
wrong) number from the true percentile across the period, so `trends.py` re-runs
`build_traversals_for_date` (the same per-day pipeline `ingest.py` uses, shared via
`ingest.build_traversals_for_date`) for every date in the week or month, concatenates the
traversals, and aggregates once across all of them -- `aggregate_segments`'s
`extra_group_columns` parameter, `("day_type",)` instead of the default `("service_date",)`,
is what drops the per-date split while keeping the business-day/weekend split below. This
means a weekly/monthly run costs one full day's ingest per day in the period (~25s/day): a
few minutes for a week, up to half an hour for a month. There is no dependency on `ingest.py`
having already run for those dates, and nothing here touches `DeliveredTripMetricsBus` --
that daily per-route rollup is unaffected.

**Every row is also split by `day_type`: `business_day` or `weekend_or_holiday`.** A week
always blends weekday and weekend service, and a month blends both several times over, so
without this split a Tuesday's rush-hour speed and a Saturday afternoon's would average into
one meaningless number. `day_type.py` classifies each date -- Saturday and Sunday are always
`weekend_or_holiday`; a weekday is too, if MBTA's own GTFS calendar says so.

That check comes from the LAMP GTFS archive's `calendar_dates` parquet, which carries a
`holiday_name` column MBTA curates itself (Christmas, MLK Day, Juneteenth, and so on) --
`is_mbta_holiday` looks up whether the service date has a row there. This is deliberately not
a hand-maintained US-holiday list: it can't drift out of date, it only flags a date MBTA
itself schedules differently for, and it costs one more small per-date fetch alongside the
trips/stop_times/stops/shapes reads `build_pattern_geometry` already does.

`day_type` reaches the map the same way `time_band` does -- as a plain tile property
(`pmtiles.TILE_PROPERTIES`) a consumer filters on client-side, not a second file or S3 key.
It's absent from the daily segment outputs entirely: a single date is already wholly one
type or the other, so there's nothing to split there. (The daily `DeliveredTripMetricsBus`
rows do carry it, from the same `day_type_for` -- see "Daily route metrics".)

Geometry is chosen the same way as the daily pipeline (`select_segment_geometry`): whichever
pattern actually ran each segment most often across the *whole* period, not per day.

A week or month that is only partially elapsed still builds -- from whatever dates in it have
already happened -- so a currently-running week/month gets a trend tile from its
dates-so-far rather than requiring a wait until it ends. Only a period with *no* available
dates at all (entirely in the future, or entirely before `EARLIEST_LAMP_BUS_DATA`) raises. A
single unusable date inside an otherwise-good range (a gap in LAMP's export) is logged and
skipped instead, matching `backfill.py`.

## Slowest-segments leaderboard

The route-level "how fast is this route" leaderboard on the dashboard (`bus_speed_leaderboard`
in t-performance-dash) ranks ~150-200 routes by summing `miles_covered`/`total_time` out of a
DynamoDB scan over the requested date range -- cheap because the table is small enough that a
full scan is fine. Segments don't fit that shape: there are ~10.8k of them a day, and the
numbers already published for each one (`p50_speed_mph`) are percentiles, not summable
totals, so there's no cheap way to combine several days' rows after the fact the way
`miles_covered`/`total_time` can be summed for routes.

Rather than build a new segment-grain DynamoDB table and a route-leaderboard-style scan
against it, `leaderboard.py` ranks directly from the same aggregated table `ingest.py` and
`trends.py` already hold in memory when they build the GeoParquet/PMTiles -- there's nothing
left to compute at request time, so the ranked list is just published as JSON alongside those
siblings and fetched directly by the frontend, the same way PMTiles are:

```
s3://tm-mbta-performance/BusSpeedSegments/daily/Year=2026/Month=9/Day=3/leaderboard.json
s3://tm-mbta-performance/BusSpeedSegments/weekly/Year=2026/Week=6/leaderboard.json
s3://tm-mbta-performance/BusSpeedSegments/monthly/Year=2026/Month=2/leaderboard.json
```

Segments are ranked slowest-first (by `p50_running_speed_mph`) **within each time band**, not blended
across them -- a segment that's slow at 7am rush and one that's slow at midnight aren't
comparable, the same reasoning behind splitting by time band and `day_type` everywhere else
in this pipeline. The result is a dict keyed by `time_band`, including `all_day`, e.g.
`{"all_day": [...], "am_peak": [...], ...}`; on the weekly/monthly files, which also carry
`day_type`, it's nested one level deeper: `{"business_day": {"all_day": [...], "am_peak":
[...]}, "weekend_or_holiday": {...}}`. `all_day` is ranked like any other band, from its own
rows, and needs no code of its own here. A daily file has no `day_type` key at all, matching
how it's absent from the PMTiles properties for the same reason. A band with no segment
above `LEADERBOARD_MIN_TRAVERSALS` has no key.

Each entry carries only what a leaderboard row needs to display -- `route_id`, `direction_id`,
`from_stop_name`, `to_stop_name`, `p50_running_speed_mph`, `n_traversals`, `n_interpolated`,
`segment_length_m`, `running_length_m`, `p50_running_time_seconds`
(`leaderboard.LEADERBOARD_COLUMNS`) -- the same kind of explicit allowlist as
`pmtiles.TILE_PROPERTIES`, so geometry and the p90, arrival-to-arrival and dwell columns never
leak into a public file by accident. Segments are ranked by running speed, the same speed the
map shows; one too short to have a running speed, or with no usable running time, isn't
ranked. Each slice is capped at `LEADERBOARD_SIZE` (500) entries: this is a display list, not
an analytical export, so it's kept well below the ~10.8k distinct segments/day rather than
dumping the whole aggregated table. It's still deep because the dashboard's route and Frequent
Bus filters narrow a slice client-side, and at 100 a filtered view came out with only a handful
of rows.

**Segments below `LEADERBOARD_MIN_TRAVERSALS` (20) traversals in a slice are dropped before
ranking** -- higher than the map's `MIN_TRAVERSALS_HINT` (3), and deliberately a separate
constant. Verified against real data (2026-09-17/18): on the daily file, 90-100% of the top 10
slowest entries per time band had fewer than 10 traversals -- a single bus stuck at a red
light, not a systematically slow segment. `MIN_TRAVERSALS_HINT` is fine for the map, where a
thin segment is one faint line among thousands; a leaderboard entry is a much louder claim
("the #1 slowest segment"), so it needs a real sample behind it. A single day often can't
clear this bar for every band -- some may come back sparse or empty, since a route may only
run a handful of trips through a given segment in a 2-hour window -- but the weekly/monthly
rollups pool traversals across many days and comfortably clear it (checked against the
in-progress 2026-W38 rollup: business-day AM peak alone had 24-42 segments still qualifying
at thresholds of 20-30).

**Terminal sections are left off** -- the first and last segments of most trips (see
`terminal_section` under "PMTiles"). Their running speed is the least sure of any: the stop
zone fits 70m on a trip's first segment and 41m on its last, against the 90m `STOP_ZONE_M`
takes off every segment (see "Running speed"), so they read slow. At 90m against 41m, a last
segment reads a median 12% slower, and a tenth of them 28% or more. On 2026-11-10's all_day
rows, terminal sections were 6.6% of the rows that would otherwise be ranked but 23 of the
slowest 100 (15 last, 8 first). Taking 41m off last segments alone would have left 10 of them;
the arrival-to-arrival ranking this replaced had 11 last and none first. The map keeps them,
dashed.

Writing is opt-in (`--write-leaderboard`, or `write_leaderboard=True`) and independent of
`--upload`/`--write-pmtiles`/`--write-to-dynamo`. `generate_yesterday_speed_segments()` writes
it by default; the weekly/monthly generators and `trends_backfill.py` take the same flag but
default it to off, matching `--write-pmtiles`'s existing opt-in convention there.

## Daily route metrics

The GeoParquet above is per-segment, for the map. `daily_metrics.py` additionally rolls the
same traversals up to one row per `(route, service_date)` -- both directions combined -- and
upserts them into a `DeliveredTripMetricsBus` DynamoDB table (`route` partition key,
`date` sort key):

| Field | Meaning |
| --- | --- |
| `count` | Distinct trips that contributed a segment that day. |
| `n_traversals` | Raw segment traversals, always ≥ `count`. |
| `n_interpolated` | Of those, how many crossed a stop LAMP didn't observe directly. |
| `miles_covered` | Sum of actual segment distance traveled that day. |
| `total_time` | Sum of actual segment time (dwell-inclusive), in seconds. |
| `total_dwell` | Sum of dwell at the stops where it was observed, in seconds. |
| `n_dwell_stops` | The stops `total_dwell` covers: every traversal except a trip's first (its wait is layover, zeroed) and ones starting at an interpolated stop (no recorded dwell). Average dwell is `total_dwell / n_dwell_stops`, about 30s on 2026-11-04. |
| `median_speed_mph` / `mean_speed_mph` | Across all segment traversals for the route that day. |
| `day_type` | `business_day` or `weekend_or_holiday`, from `day_type.day_type_for` -- the same MBTA GTFS holiday calendar the weekly/monthly map tiles split on. |
| `time_bands` | Map keyed by time band name (the six in "Time bands" above, not `all_day`), each holding `count`, `n_traversals`, `n_interpolated`, `miles_covered`, `total_time`, `total_dwell`, `n_dwell_stops` for traversals that departed in that band. A band with no traversals that day is absent. |
| `scheduled_trips` | Every trip in the LAMP events, run or not. LAMP has a row for every scheduled stop of every scheduled trip, and matched GTFS exactly over six days. A trip interlined across routes (~18 a day) counts on each. |
| `unobserved_trips` | Trips with no observed arrival or departure at any stop. Nearly all carry the placeholder `vehicle_label` `____`, but so do some partly observed trips, so the label isn't the test. |
| `unobserved_waived_trips` | Of those, trips with a `waiver_remark` on any row. |
| `headways_under_50` ... `headways_over_150` | Headways at mid-route timepoints (`point_type` `mid`; terminals left out), counted by actual / scheduled route-direction headway: `≤0.5`, `(0.5, 0.75]`, `(0.75, 1.25)`, `[1.25, 1.5)`, `≥1.5`. The edges reproduce the dashboard's bunched (`≤ 0.5`) and on-time (`0.75–1.25` exclusive) rules, and are shared with transitmatters/data-ingestion#232. Rows with no positive scheduled headway are skipped. |

The trip and headway fields come from the LAMP events as read, before they become
traversals, since a trip that never ran has no traversals. So a route whose trips were all
unobserved (usually a shuttle) still gets a row, with these fields and no speed fields. All
of them are whole counts that sum across days.

The top-level fields are the all-day figures. `time_bands` carries only fields that can be
summed across days, so a weekly or monthly view can add up daily rows per band the same way
it already does for the top level; per-band medians and means are left out because they
can't be combined. A band's `count` is the distinct trips with at least one traversal
departing in it, so a trip that crosses a band boundary counts in both bands and the band
counts sum to more than `count` (10-15% more on real days). The other four fields do add up
to the top level: every traversal lands in exactly one band, and across 38 real days none
fell outside all of them. The only difference is rounding -- each band is rounded the same
way as the top level (3dp miles, 1dp seconds), so the band sums can drift from the top level
by a few thousandths of a mile or a tenth of a second.

`time_bands` is written as one whole map on every run, not per nested path, so a band that
disappears on a re-run doesn't linger from the previous write. Adding it takes an item from
~235 bytes to ~740 (max 793 on 2026-09-17), still under the 1KB that one write unit covers.
The dwell, trip and headway fields take it to roughly 1KB (a rough count put 2026-11-04 at
1,040 median, 1,079 max), so some writes now take two units. At ~153 rows a day that's still
well under a cent a month.

This is a table for the same kind of "how fast is this route" line chart the dashboard
already draws for rail from `DeliveredTripMetrics`, kept separate from that table because bus
has no fixed round-trip track length or line/branch structure to key on. Where rail
multiplies a nominal round-trip length by an observed trip count, `miles_covered` and
`total_time` here are summed directly from what LAMP actually recorded, so they're already
directly comparable via `miles_covered / (total_time / 3600)` without a `speed.py`-style
API round trip.

The same rows also carry bus fleet stats (battery-electric share, propulsion mix, average
bus age), written by data-ingestion's fleet job (transitmatters/data-ingestion#203) under the
same GTFS route id keys, plus a system-wide row under `route = "all"`. Both jobs write with
UpdateItem and SET only their own fields, so they can run in either order without wiping
each other. Fleet history starts 2023-12-14, before LAMP bus data, so rows before
2025-12-24 carry fleet fields only.

Writing is opt-in (`--write-to-dynamo`, or `write_to_dynamo=True`) and independent of
`--upload`: turning one on doesn't turn on the other. It adds one small `calendar_dates`
fetch for `day_type`, only when set. `generate_yesterday_speed_segments()`
writes both by default. The scheduled daily job writes it (see "Scheduling" above), with
credentials scoped to this table and `BusSpeedSegments/`, not the Lambdas' policies.

Geometry is repeated on every row -- six time bands plus `all_day`, a 5.9x duplication.
Splitting it into a static sidecar would cut the 2026-09-17 file from 5.67MB to 3.82MB and
cost 0.95MB once. That is not worth doing for storage -- a year is 2.1GB vs 1.4GB, about
$0.05/month vs $0.03/month -- but it would let a browser cache the geometry once instead of
refetching it per day. Left combined for now, on the grounds that one file per day is the
simpler thing to consume.

## Things that will bite you

**`plan_*_dt` columns are shifted by one UTC offset.** On 2026-09-03,
`plan_stop_departure_sam` = 16620 (04:37) while `plan_stop_departure_dt` = `00:37`, a
constant 14400s apart. The `*_seconds` / `*_sam` columns are seconds after midnight of the
service date, are mutually consistent, and correctly exceed 86400 for after-midnight trips.
This module uses only those. Comparing the `_dt` pairs directly invents a 4-hour delay on
every row.

**`shape_dist_traveled` is null throughout the MBTA feed** — in both `stop_times` and
`shapes`. Distance along a shape has to be computed from the geometry, which is why
`gtfs_geo.py` carries its own projection code.

**Stops must be matched to shapes monotonically.** Loop and out-and-back routes (216, 220,
112, 119, 120) run past the same stop twice, so nearest-point matching puts stops on the
wrong pass. Matching greedily forward is worse: one bad match drags every later stop with
it and produces 4km "segments". `project_stops_onto_shape` enumerates each candidate pass
and runs a DP for the cheapest strictly-increasing assignment, trading each stop's distance
from the shape against how far the implied spacing strays from the straight-line distance
between stops. Across 564 patterns this holds the worst stop-to-shape offset to 56m
(median 11m) and leaves 2 non-advancing segments, which are dropped.

**`latitude`/`longitude` in the LAMP bus feed is the vehicle position, not the stop** —
median 112m from the GTFS stop, p99 1.5km. Geometry comes from GTFS, never from these.

**LAMP never omits a scheduled stop.** Of 12,851 trips on a sample weekday, none had fewer
event rows than GTFS scheduled stops. The gap is ~7% of rows carrying a *null* timestamp,
72% of which are bracketed by real observations. There are no phantom stops to infer.

## Running speed

Two clocks are emitted, and the map and leaderboard use the first:

| Column | Meaning |
| --- | --- |
| `p50_running_speed_mph` | Departure from A to arrival at B (`p50_running_time_seconds`, LAMP's own `travel_time_seconds` exactly), over `running_length_m`: the segment less `STOP_ZONE_M`. The bus on the street, with no time at either stop. **What the map shows.** |
| `p50_speed_mph` | Arrival to arrival (`p50_total_time_seconds`), so it includes the dwell at A, over the whole segment. What a rider on board experiences. In the GeoParquet, and behind the route-level `DeliveredTripMetricsBus` speeds. |

The time at the stop goes on the dwell layer instead (see "PMTiles").

**Why the length less `STOP_ZONE_M` (90m).** LAMP's bus arrival and departure times aren't taken
at the stop, so the running clock only covers part of each segment. On 2026-11-04 (322k
traversals) running time fit 0.168 s/m x (length - 91m): it hits zero on a 91m segment. Over the
full length, that read impossibly fast on short segments:

| Segment length | Share of traversals | Running, full length | Running, length - 90m | Arrival to arrival |
| --- | --- | --- | --- | --- |
| up to 100m | 1% | 34 mph | negative | 10.3 mph |
| 100-150m | 8% | 40 mph | 7.2 mph to 125m, 12.4 mph above | 10.1 mph |
| 200-250m | 19% | 28 mph | 16.7 mph | 11.2 mph |
| 300-400m | 20% | 22 mph | 16.4 mph | 11.6 mph |
| 500-750m | 6% | 15 mph | 13.1 mph | 11.5 mph |

Medians, per traversal. 90m is an average, not a measured fact: fits per kind of segment put it at
76m mid-trip, 70m on a trip's first segment and 41m on its last, and we haven't confirmed how LAMP
stamps the times. So first and last segments read slow, and the leaderboard leaves them off
(see "Slowest-segments leaderboard"). Dwell is the other side of the same zone, so it's best read
as time around the stop -- slowing, standing and pulling out -- not time with the doors open.

Segments under `MIN_RUNNING_SPEED_SEGMENT_M` (125m) get no running speed: 100-125m still read a
median 7 mph after the correction, and under 100m it goes negative. On 2026-11-04 that's 3.6% of
traversals and 6.5% of all-day rows. Everything else about them -- counts, arrival-to-arrival
times, dwell at their first stop -- is still published.

**A traversal is kept or dropped on arrival to arrival**, the one clock that spans the whole
segment. It's dropped if that time is zero or less, or works out faster than
`MAX_PLAUSIBLE_SPEED_MPH` (65 mph): 1.4% of traversals on 2026-11-10. Judged on the running
clock over the full length, as it was until #130, 22-32% of traversals under 200m were
dropped, mostly the fastest, and about a quarter of those segments read more than 1 mph slow.
There's no lower bound: the slowest traversals are buses holding mid-trip, for up to an hour,
often on segments where most trips do (route 109 from stop 7417 runs at a median 0.64 mph),
and a floor hid the slowest of them.

Where the running clock reads zero or less, or faster than 65 mph over `running_length_m`, the
traversal is kept with a null running time. On a segment too short to have a running length,
only zero or less counts. The running percentiles come from the rest, so from fewer traversals
than `n_traversals`, and are null on a row where none are left. Judged over the full segment
on 2026-11-10, that nulled 10% of traversals and left 1.5% of rows, nearly all with 1-3
traversals, with no running time; over the running length it's fewer.

Dwell at a trip's *first* stop is excluded: the bus sits at the terminal awaiting its
scheduled departure, often 10+ minutes. This matches how `chalicelib.benchmarks` treats the
dwell at an origin stop for rail.

Sanity check on 2026-11-04, all-day rows weighted by traversals: 15.8 mph running against 13.3
mph arrival to arrival; median row 16.2 mph, 10th and 90th percentiles 7.9 and 25.0 mph.
Measured before #130's change to how traversals are kept or dropped, which nudges both up
slightly: arrival to arrival on 2026-09-03 went from 13.90 to 14.26 mph overall.

## Interpolation

A stop with no observed time that is bracketed by observed stops gets a pass time
interpolated **by distance along the shape**: the bus is assumed to hold constant speed
across the gap. Because the bus was never recorded stopping there, it is given no dwell.
Leading and trailing gaps cannot be bracketed and are left null. An interpolated stop's
dwell isn't on the dwell layer. Interpolated values are included in the percentiles; `n_interpolated` per row lets a consumer filter them out.

## Coverage

`LAMP_ALL_Bus_Events` starts **2025-12-24**. For earlier bus data use the ArcGIS monthly
archive in `chalicelib/historic` — note it writes a different S3 layout
(`monthly-bus-data/{route}-{direction}-{stop}/...`), so spanning both eras means
reconciling the two.

`source_url_for` picks the small rolling 7-day export when the date is recent enough and
the all-time export otherwise.

## Dependencies

Uses `shapely`, `geopandas` and `pyproj` from the `geo` dependency group (`uv sync --group
geo`) rather than the project's base dependencies -- this pipeline runs as a k8s cron job
(see "Scheduling" above), not through the Lambda deployment that `uv export --no-dev` packages, so there's no bundle
size to protect. Geometry is projected into Massachusetts State Plane (EPSG:26986, metres)
with `pyproj`, cut with `shapely.ops.substring`, and reprojected back to WGS84 lon/lat for
storage; GeoParquet is written by `geopandas.GeoDataFrame.to_parquet`. An earlier version
hand-rolled all of this (a local equirectangular approximation and WKB written by hand) to
avoid the dependency while still deploying through Lambda -- that version was validated
against GeoPandas at a median 0.21m (0.085%) discrepancy in `segment_length_m`, which is why
EPSG:26986 was kept as the reference CRS here.
