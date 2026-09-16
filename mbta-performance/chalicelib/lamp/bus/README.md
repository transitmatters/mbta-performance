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

Deliberately none beyond what the project already has. Geometry is projected with a local
equirectangular approximation and WKB is written by hand, keeping shapely/geopandas/pyproj
out of the Lambda bundle. Validated against GeoPandas: measuring the emitted geometry in
Massachusetts State Plane (EPSG:26986) reproduces the reported `segment_length_m` to a
median of 0.21m (0.085%).
