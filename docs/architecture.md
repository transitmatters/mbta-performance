# Architecture

LAMPLighter is an AWS Chalice serverless application. It has two modes of operation: an **automated daily pipeline** that runs on a schedule via Lambda, and a set of **manual backfill scripts** for loading historical data.

---

## Data flow

```
LAMP (performancedata.mbta.com)
  │
  ▼
fetch_pq_file_from_remote()      ← daily parquet file
  │
  ▼
ingest_pq_file()                 ← process arrivals/departures, enrich with GTFS
  │                                  │
  │                           tm-gtfs (S3)
  │                           GTFS feed cache (build locally if missing)
  ▼
upload_to_s3() × N stops         ← parallel upload
  │
  ▼
tm-mbta-performance (S3)
Events-lamp/daily-data/{stop_id}/Year=.../Month=.../Day=.../events.csv
```

---

## Lambda functions

Both functions are deployed via AWS CloudFormation (stack: `mbta-performance`) using Chalice. Each has:

- **Memory:** 2048 MB
- **Ephemeral storage:** 2048 MB
- **Timeout:** 250 seconds
- **Monitoring:** Datadog APM + profiling via `datadog_lambda_wrapper`

### `process_daily_lamp`

Processes **today's** LAMP data, re-running throughout the day as new data arrives.

| | |
|---|---|
| **Schedule** | Every 30 minutes (`*/30 0-7,10-23 * * ? *`) |
| **Effective window** | 6:00 AM – 2:30 AM Eastern (exits early before 6 AM to avoid incomplete data) |
| **Entry point** | `app.process_daily_lamp` → `lamp.ingest_today_lamp_data()` |

### `process_yesterday_lamp`

Processes **yesterday's** LAMP data once in the morning, to capture any data that was cleaned or published after midnight.

| | |
|---|---|
| **Schedule** | Daily at 15:00 UTC (`0 15 * * ? *`) — 11:00 AM ET (EDT) or 10:00 AM ET (EST) |
| **Entry point** | `app.process_yesterday_lamp` → `lamp.ingest_yesterday_lamp_data()` |

---

## Manual backfill scripts

These are run locally (not deployed to Lambda) to load historical data into S3. Each script prompts for confirmation before making large numbers of S3 writes.

| Script | Coverage | Source |
|---|---|---|
| `chalicelib/lamp/backfill/main.py` | 2019-09-15 → yesterday | LAMP parquet files |
| `chalicelib/historic/backfill/main.py` | 2016 – 2025 | ArcGIS rapid transit ZIPs |
| `chalicelib/historic/backfill/bus.py` | 2020 – 2025 | ArcGIS bus ZIPs |
| `chalicelib/historic/backfill/ferry.py` | All available | ArcGIS ferry CSV |

See [Home](index.md) for the commands to run each script.

---

## AWS resources

| Resource | Name | Purpose |
|---|---|---|
| CloudFormation stack | `mbta-performance` | Owns all Lambda resources |
| S3 bucket | `tm-mbta-performance` | Output: processed event CSVs |
| S3 bucket | `tm-gtfs` | GTFS feed cache (SQLite builds) |
| S3 bucket | `mbta-performance-lambda-deployments` | Chalice deployment artifacts |
| CloudWatch Logs | (auto-created) | Lambda execution logs |
| Datadog | `mbta-performance` service | APM, profiling, metrics |

### IAM permissions (per Lambda)

| Action | Resource |
|---|---|
| `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` | All CloudWatch Logs |
| `s3:GetObject`, `s3:PutObject` | `tm-mbta-performance/Events-lamp/daily-data/*` |
| `s3:*` | `tm-gtfs`, `tm-gtfs/*` |
