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
```

One row per `(route, direction, from_stop, to_stop, service_date, time_band)`. A typical
weekday produces ~53k rows covering ~10.8k distinct segments across 151 routes, and takes
about 25 seconds end to end.

## Pipeline

1. **Read one service date** from the LAMP bus export. Both exports are partitioned one
   parquet row group per `service_date`, so `remote_parquet.py` reads the footer over HTTP
   and pulls just that row group — ~35MB, rather than downloading the 6.8GB all-time file.
2. **Locate every stop along its route pattern's shape** (`gtfs_geo.py`, `patterns.py`).
3. **Interpolate stop times LAMP did not observe** (`segments.py`).
4. **Build per-traversal times, then aggregate** to percentiles per time band.
5. **Write GeoParquet** with road-following `LineString` geometry (`geoparquet.py`).
6. **Optionally build PMTiles** from the same result, for the live map (`pmtiles.py`).

## Output location

```
s3://tm-mbta-performance/BusSpeedSegments/daily/Year=2026/Month=9/Day=3/segments.parquet
```

One self-contained GeoParquet per service date, ~4.1MB, holding the whole network. Month
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
```

`pmtiles.py` builds it from the same `result` frame the GeoParquet above is written from --
one `tippecanoe` invocation, writing every segment into a single vector layer named
`segments` (matching `modules/busspeedmap/constants.ts`'s `PMTILES_SOURCE_LAYER` in the
dashboard repo). Only the columns the map actually reads (`TILE_PROPERTIES`: route, direction,
stop names, time band, `p50_speed_mph`, traversal counts) are handed to tippecanoe, so a
column like `p90_speed_mph` or `moving_speed_mph` can never end up on the map by accident --
see `modules/busspeedmap/types.ts`'s `BusSpeedSegmentProperties` for the matching frontend
type.

**Requires `tippecanoe` on `PATH`.** It's a system binary, not a Python dependency -- there's
nothing to add to `pyproject.toml` for it. Install it via `apt install tippecanoe` (or the
equivalent for the container image this pipeline runs in); `build_pmtiles_bytes` raises a
clear error if it's missing rather than a bare `FileNotFoundError`.

Writing is opt-in (`--write-pmtiles`, or `write_pmtiles=True`) and independent of `--upload`
and `--write-to-dynamo`. `generate_yesterday_speed_segments()` writes all three by default.

## Weekly and monthly trend tiles

The daily PMTiles above are for "how fast was the network on this specific day." `trends.py`
builds the same kind of tileset rolled up across a calendar week or month instead, for
slower-moving patterns -- a detour that lasts a season, a route that's consistently faster
off-peak -- that would otherwise mean flipping through individual days.

```shell
# Week/month numbers are sequential from 1, not a calendar date -- see below.
uv run python -m mbta-performance.chalicelib.lamp.bus.trends --week 6 --upload --write-pmtiles
uv run python -m mbta-performance.chalicelib.lamp.bus.trends --month 2 --upload --write-pmtiles
```

```
s3://tm-mbta-performance/BusSpeedSegments/weekly/Week=6/segments.pmtiles
s3://tm-mbta-performance/BusSpeedSegments/monthly/Month=2/segments.pmtiles
```

**Weeks and months are keyed by a sequential integer, not a calendar date.** `periods.py`
numbers them from 1, starting at the calendar week (Monday-Sunday) and month containing
`EARLIEST_LAMP_BUS_DATA` -- week 1 is 2025-12-22..2025-12-28, month 1 is December 2025, week
2/month 2 follow immediately after, and so on. A Year=/Week= or Year=/Month= key like the
daily pipeline's would work just as well, but bus history is only a few months long, so a
plain integer is simpler to key and fetch by. `week_range` / `month_range` convert a number
back to its calendar boundaries; `dates_in_range` then clips that range to whatever LAMP data
actually exists (before `EARLIEST_LAMP_BUS_DATA`, or later than yesterday).

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
It's absent from the daily pipeline's output entirely: a single date is already wholly one
type or the other, so there's nothing to split there.

Geometry is chosen the same way as the daily pipeline (`select_segment_geometry`): whichever
pattern actually ran each segment most often across the *whole* period, not per day.

A week or month that is only partially elapsed still builds -- from whatever dates in it have
already happened -- so a currently-running week/month gets a trend tile from its
dates-so-far rather than requiring a wait until it ends. Only a period with *no* available
dates at all (entirely in the future, or entirely before `EARLIEST_LAMP_BUS_DATA`) raises. A
single unusable date inside an otherwise-good range (a gap in LAMP's export) is logged and
skipped instead, matching `backfill.py`.

## Daily route metrics

The GeoParquet above is per-segment, for the map. `daily_metrics.py` additionally rolls the
same traversals up to one row per `(route, service_date)` -- both directions combined -- and
batch-writes them to a `DeliveredTripMetricsBus` DynamoDB table (`route` partition key,
`date` sort key):

| Field | Meaning |
| --- | --- |
| `count` | Distinct trips that contributed a segment that day. |
| `n_traversals` | Raw segment traversals, always ≥ `count`. |
| `n_interpolated` | Of those, how many crossed a stop LAMP didn't observe directly. |
| `miles_covered` | Sum of actual segment distance traveled that day. |
| `total_time` | Sum of actual segment time (dwell-inclusive), in seconds. |
| `median_speed_mph` / `mean_speed_mph` | Across all segment traversals for the route that day. |

This is a table for the same kind of "how fast is this route" line chart the dashboard
already draws for rail from `DeliveredTripMetrics`, kept separate from that table because bus
has no fixed round-trip track length or line/branch structure to key on. Where rail
multiplies a nominal round-trip length by an observed trip count, `miles_covered` and
`total_time` here are summed directly from what LAMP actually recorded, so they're already
directly comparable via `miles_covered / (total_time / 3600)` without a `speed.py`-style
API round trip.

Writing is opt-in (`--write-to-dynamo`, or `write_to_dynamo=True`) and independent of
`--upload`: turning one on doesn't turn on the other. `generate_yesterday_speed_segments()`
writes both by default. Note this table and the write path are not yet wired into `app.py`'s
scheduled Lambdas or granted DynamoDB permissions in `policy-lamp-ingest.json` -- that's a
follow-up, same as the map segments' S3 upload isn't scheduled yet either.

Geometry is repeated across the six time bands, a 4.9x duplication. Splitting it into a
static sidecar would cut daily files from 4.08MB to 2.90MB and cost 1.07MB once. That is
not worth doing for storage -- a year is 1.5GB vs 1.0GB, about $0.035/month either way --
but it would let a browser cache the geometry once instead of refetching it per day. Left
combined for now, on the grounds that one file per day is the simpler thing to consume.

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

## The two speeds

Dwell time on MBTA buses is about as large as running time (both median ~21s), so these
differ by more than a factor of two. Both are emitted:

| Column | Meaning |
| --- | --- |
| `p50_moving_speed_mph` | Departure from A to arrival at B. Running speed in traffic. Reproduces LAMP's own `travel_time_seconds` exactly. |
| `p50_speed_mph` | Arrival to arrival, so it includes dwell at A. What a rider on board experiences. **This is the headline number.** |

Dwell at a trip's *first* stop is excluded: the bus sits at the terminal awaiting its
scheduled departure, often 10+ minutes, and charging that to the first segment made
terminal segments read as 0.5mph. This matches how `chalicelib.benchmarks` treats the dwell
at an origin stop for rail.

Sanity check on 2026-09-03, traversal-weighted p50: 12.35mph at pm peak rising to 16.97mph
late night, 13.90mph overall — consistent with a system whose buses average ~13mph.

## Interpolation

A stop with no observed time that is bracketed by observed stops gets a pass time
interpolated **by distance along the shape**: the bus is assumed to hold constant speed
across the gap. Because the bus was never recorded stopping there, it is given no dwell.
Leading and trailing gaps cannot be bracketed and are left null. Interpolated values are
included in the percentiles; `n_interpolated` per row lets a consumer filter them out.

## Coverage

`LAMP_ALL_Bus_Events` starts **2025-12-24**. For earlier bus data use the ArcGIS monthly
archive in `chalicelib/historic` — note it writes a different S3 layout
(`monthly-bus-data/{route}-{direction}-{stop}/...`), so spanning both eras means
reconciling the two.

`source_url_for` picks the small rolling 7-day export when the date is recent enough and
the all-time export otherwise.

## Dependencies

Uses `shapely`, `geopandas` and `pyproj` from the `geo` dependency group (`uv sync --group
geo`) rather than the project's base dependencies -- this pipeline runs as a k8s cron job,
not through the Lambda deployment that `uv export --no-dev` packages, so there's no bundle
size to protect. Geometry is projected into Massachusetts State Plane (EPSG:26986, metres)
with `pyproj`, cut with `shapely.ops.substring`, and reprojected back to WGS84 lon/lat for
storage; GeoParquet is written by `geopandas.GeoDataFrame.to_parquet`. An earlier version
hand-rolled all of this (a local equirectangular approximation and WKB written by hand) to
avoid the dependency while still deploying through Lambda -- that version was validated
against GeoPandas at a median 0.21m (0.085%) discrepancy in `segment_length_m`, which is why
EPSG:26986 was kept as the reference CRS here.
