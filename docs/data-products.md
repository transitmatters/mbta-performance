# Data Products

All outputs are written to the `tm-mbta-performance` S3 bucket as CSV files partitioned by stop and date. Month and day values in key paths are **not** zero-padded.

---

## LAMP Daily Events

Produced by the daily LAMP ingest pipeline (`chalicelib.lamp.ingest`).

**S3 key pattern:**
```
Events-lamp/daily-data/{stop_id}/Year={YYYY}/Month={M}/Day={D}/events.csv
```

**Columns:**

| Column | Description |
|---|---|
| `service_date` | Operating day (`YYYY-MM-DD`) |
| `route_id` | GTFS route identifier |
| `trip_id` | GTFS trip identifier |
| `direction_id` | 0 = outbound, 1 = inbound |
| `stop_id` | GTFS stop identifier |
| `stop_sequence` | Sequence number within the trip |
| `vehicle_id` | Vehicle identifier |
| `vehicle_label` | Human-readable vehicle number |
| `event_type` | `ARR` (arrival) or `DEP` (departure) |
| `event_time` | Timestamp of the event (Eastern time) |
| `travel_time_seconds` | Observed travel time from previous station |
| `dwell_time_seconds` | Time spent at the station |
| `headway_seconds` | Observed trunk headway |
| `headway_branch_seconds` | Observed branch headway |
| `scheduled_tt` | GTFS-scheduled travel time (seconds from trip start) |
| `scheduled_headway` | 30-minute bucket average of scheduled trunk headway |
| `scheduled_headway_branch` | GTFS-scheduled branch headway |
| `vehicle_consist` | Consist / car set identifier |

!!! note "Departure stop assignment"
    Raw LAMP data records departures at the *next* stop. During ingest, departure events are re-assigned to the stop the vehicle *left from*, so that each stop has a matching ARR + DEP pair.

!!! note "Scheduled headway smoothing"
    `scheduled_headway` is averaged across all trips serving a route/direction/stop within each 30-minute window, then rounded to the nearest 10 seconds.

---

## Historic Rapid Transit Events

Produced by the historic backfill pipeline (`chalicelib.historic.backfill.main`).

**S3 key pattern:**
```
Events/monthly-data/{stop_id}/Year={YYYY}/Month={M}/events.csv.gz
```

Data is grouped by stop and calendar month and written as gzip-compressed CSV with a deterministic `mtime=0` header (so re-generated files are bitwise-identical and won't trigger unnecessary S3 uploads).

**Columns:** `service_date`, `route_id`, `trip_id`, `direction_id`, `stop_id`, `stop_sequence`, `vehicle_id`, `vehicle_label`, `event_type`, `event_time`, `scheduled_headway`, `scheduled_tt`

---

## Historic Bus Events

Produced by the bus backfill pipeline (`chalicelib.historic.backfill.bus`).

**S3 key pattern:**
```
Events/monthly-bus-data/{route_id}-{direction_id}-{stop_id}/Year={YYYY}/Month={M}/events.csv.gz
```

Bus data only records events at timepoints (start, mid, and end points), not every stop. `event_type` is derived from the point type:

| Point type | Events generated |
|---|---|
| `Startpoint` | `DEP` |
| `Midpoint` | `ARR`, `DEP` |
| `Endpoint` | `ARR` |

**Columns:** `service_date`, `route_id`, `trip_id`, `direction_id`, `stop_id`, `stop_sequence`, `vehicle_id`, `vehicle_label`, `event_type`, `event_time`, `scheduled_headway`, `scheduled_tt`

---

## Historic Ferry Events

Produced by the ferry backfill pipeline (`chalicelib.historic.backfill.ferry`).

**S3 key pattern:**
```
Events/monthly-ferry-data/{route_id}|{direction_id}|{stop_id}/Year={YYYY}/Month={M}/events.csv.gz
```

Note the `|` delimiter in the directory name (vs `-` for bus), reflecting that ferry route IDs contain hyphens (e.g. `Boat-F1`).

**Columns:** `service_date`, `route_id`, `trip_id`, `direction_id`, `stop_id`, `stop_sequence`, `vehicle_id`, `vehicle_label`, `event_type`, `event_time`, `vehicle_consist`
