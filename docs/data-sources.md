# Data Sources

LAMPLighter pulls from four upstream sources.

---

## LAMP

**Lightweight Application for Measuring Performance** — the MBTA's system for consuming real-time network data, computing performance metrics, and making that data publicly available. Source code: [mbta/lamp](https://github.com/mbta/lamp).

| | |
|---|---|
| **Coverage** | 2019-09-15 → present |
| **Modes** | Subway (Red, Orange, Blue, Green, Silver) |
| **Format** | Parquet (one file per service date) |

### Endpoints

| Resource | URL |
|---|---|
| Index CSV | `https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/index.csv` |
| Daily parquet | `https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/{YYYY-MM-DD}-subway-on-time-performance-v1.parquet` |

### Columns ingested

| Column | Description |
|---|---|
| `service_date` | Operating day (Eastern time) |
| `route_id` | GTFS route identifier |
| `trip_id` | GTFS trip identifier |
| `stop_id` | GTFS stop identifier |
| `direction_id` | 0 = outbound, 1 = inbound |
| `stop_sequence` | Sequence number within the trip |
| `vehicle_id` | Vehicle identifier |
| `vehicle_label` | Human-readable vehicle number |
| `move_timestamp` | Unix epoch seconds — departure from previous station |
| `stop_timestamp` | Unix epoch seconds — arrival at this station |
| `travel_time_seconds` | Observed travel time |
| `dwell_time_seconds` | Time spent at the station |
| `headway_trunk_seconds` | Observed headway on the trunk |
| `headway_branch_seconds` | Observed headway on the branch |
| `scheduled_travel_time` | GTFS-scheduled travel time |
| `scheduled_headway_trunk` | GTFS-scheduled trunk headway |
| `scheduled_headway_branch` | GTFS-scheduled branch headway |
| `vehicle_consist` | Consist / car set identifier |

---

## Historic Rapid Transit

Pre-LAMP performance data published annually to the [MBTA ArcGIS Hub](https://mbta-massdot.opendata.arcgis.com/).

| | |
|---|---|
| **Coverage** | 2016 – 2025 |
| **Modes** | Subway (Red, Orange, Blue, Green) |
| **Format** | ZIP of CSVs (one per year) |

!!! note
    2024 and later files use the LAMP column format (`sync_stop_sequence` instead of `stop_sequence`). The pipeline automatically selects the correct column set based on year.

### Available datasets

| Year | ArcGIS item ID |
|---|---|
| 2016 | `3e892be850fe4cc4a15d6450de4bd318` |
| 2017 | `cde60045db904ad299922f4f8759dcad` |
| 2018 | `25c3086e9826407e9f59dd9844f6c975` |
| 2019 | `11bbb87f8fb245c2b87ed3c8a099b95f` |
| 2020 | `cb4cf52bafb1402b9b978a424ed4dd78` |
| 2021 | `611b8c77f30245a0af0c62e2859e8b49` |
| 2022 | `99094a0c59e443cdbdaefa071c6df609` |
| 2023 | `9a7f5634db72459ab731b6a9b274a1d4` |
| 2024 | `0711756aa5e1400891e79b984a94b495` |
| 2025 | `e2344a2297004b36b82f57772926ed1a` |

---

## Historic Bus

Bus arrival/departure data published annually to the MBTA ArcGIS Hub.

| | |
|---|---|
| **Coverage** | 2020 – 2025 (2018 & 2019 no longer available) |
| **Modes** | Bus |
| **Format** | ZIP of CSVs (quarterly through 2020, monthly from 2021+) |

### Available datasets

| Year | ArcGIS item ID |
|---|---|
| 2020 | `4c1293151c6c4a069d49e6b85ee68ea4` |
| 2021 | `2d415555f63b431597721151a7e07a3e` |
| 2022 | `ef464a75666349f481353f16514c06d0` |
| 2023 | `b7b36fdb7b3a4728af2fccc78c2ca5b7` |
| 2024 | `96c77138c3144906bce93d0257531b6a` |
| 2025 | `924df13d845f4907bb6a6c3ed380d57a` |

!!! note
    Starting June 2024, MBTA changed the time format in bus data from Eastern Time to UTC. The pipeline detects this automatically based on the service date and converts accordingly.

---

## Historic Ferry

Ferry ridership data published to the MBTA ArcGIS Hub as a single cumulative CSV (updated over time rather than in annual snapshots).

| | |
|---|---|
| **Modes** | Ferry (Boat-F1, Boat-F4, Boat-EastBoston, Boat-Lynn, and others) |
| **Format** | Single CSV via ArcGIS Hub download API |

Because the dataset is large, ArcGIS does not automatically update its download cache. The pipeline explicitly triggers a cache refresh before downloading.

---

## GTFS

General Transit Feed Specification schedules are used to enrich LAMP data with:

- **Scheduled travel times** — matched per trip and stop
- **Scheduled headways** — smoothed into 30-minute buckets
- **Trip branch assignment** — for Red Line Ashmont/Braintree disambiguation

The MBTA publishes GTFS to communicate planned system service. The LAMP team maintains a compressed archive of all MBTA GTFS schedules issued since 2009, available in two formats.

### Parquet archive

One parquet file per GTFS file/field definition per year. Each file includes two integer columns added by LAMP:

| Column | Description |
|---|---|
| `gtfs_active_date` | First date (as `YYYYMMDD` integer) the feed was active |
| `gtfs_end_date` | Last date (as `YYYYMMDD` integer) the feed was active |

Filter with `gtfs_active_date <= YYYYMMDD AND gtfs_end_date >= YYYYMMDD` to get records applicable to a single service date.

**URL pattern:** `https://performancedata.mbta.com/lamp/gtfs_archive/{YYYY}/{file}.parquet`

Example — stop times applicable on 2022-12-25:
```
https://performancedata.mbta.com/lamp/gtfs_archive/2022/stop_times.parquet
```
filtered with `gtfs_active_date <= 20221225 AND gtfs_end_date >= 20221225`.

### SQLite archive

A gzipped SQLite database mirroring the parquet files, with the same `(gtfs_active_date, gtfs_end_date)` filtering columns on every table.

**URL pattern:** `https://performancedata.mbta.com/lamp/gtfs_archive/{YYYY}/GTFS_ARCHIVE.db.gz`

!!! note
    Not all GTFS files are available for every year. For example, `timeframes.txt` was introduced in 2023, so `timeframes.parquet` does not exist for earlier years.

    GTFS records are considered applicable starting the day *after* the publish date in `feed_info.feed_version`. The most recently published schedule is treated as active for one year past its publish date, so future service dates can be queried.

### How this project uses GTFS

Feeds are fetched via the [`mbta-gtfs-sqlite`](https://github.com/transitmatters/mbta-gtfs-sqlite) library, which downloads from the `tm-gtfs` S3 bucket. If a feed for the requested service date does not exist in S3 yet, it is built locally from the LAMP archive and uploaded for future reuse.
