# Monthly Data Processing

The MBTA publishes monthly batches of performance data for rapid transit, bus, and ferry on the [MassDOT/MBTA ArcGIS Hub](https://mbta-massdot.opendata.arcgis.com/). These are more complete than the daily LAMP feed. The dashboard uses them for every date up to a cutoff, and daily LAMP data after it.

| Mode          | Source                                                      | Output (`data/output/` locally, `s3://tm-mbta-performance/` remotely)                |
| ------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Rapid transit | one ArcGIS item per year (`ARCGIS_IDS` in `constants.py`)     | `Events/monthly-data/{stop_id}/Year={y}/Month={m}/events.csv.gz`                     |
| Bus           | one ArcGIS item per year (`BUS_ARCGIS_IDS`)                 | `Events/monthly-bus-data/{route}-{direction}-{stop_id}/Year={y}/Month={m}/events.csv.gz` |
| Ferry         | a single ArcGIS item covering all years (`FERRY_ARCGIS_ID`) | `Events/monthly-ferry-data/{route}\|{direction}\|{stop_id}/Year={y}/Month={m}/events.csv.gz` |

The MBTA usually posts rapid transit and bus data a month or two behind, and ferry roughly every six months. None of it arrives on a fixed schedule, so check before each update.

All commands run from the repo root, need AWS credentials for the S3 steps (see the root README), and write under `./data/`.

## Monthly update runbook

### 1. See what's needed

```sh
uv run python -m mbta-performance.chalicelib.historic.status
```

For each mode, this prints the latest month in S3, the latest month processed locally, and when the MBTA last updated the source. If the upstream date is newer than your last run, there is probably a new month to process.

```
mode   latest in S3  latest local  upstream source (last modified)
rapid  2026-03       2026-07       MBTA Rapid Transit Events 2026 (2026-09-11)
       -> local has months not in S3; run: python -m mbta-performance.chalicelib.historic.upload --mode rapid
bus    2026-06       2026-06       MBTA Bus Arrival Departure Times 2026 (2026-07-07)
ferry  2025-10       none          MBTA Ferry Daily Ridership by Trip, Route, and Stop (2026-04-21)
```

### 2. Download and process

Each command re-downloads that year's source zip, so you always get the latest version of the file.

```sh
# Rapid transit, current year (~270 MB download)
uv run python -m mbta-performance.chalicelib.historic.backfill.main --year 2026

# Bus, current year (~180 MB download, ~1.5 GB unzipped; ~2 min per month to process)
uv run python -m mbta-performance.chalicelib.historic.backfill.bus --year 2026

# Ferry: the whole history (2018+) is one file, so ALWAYS pass --start-date
# (the first month after "latest in S3" from step 1)
uv run python -m mbta-performance.chalicelib.historic.backfill.ferry --start-date 2025-11-01
```

Things to expect:

- GTFS archives for scheduled headways are downloaded into `data/gtfs_archives/` and cached there. There is one archive per feed version, at roughly 100 MB unzipped each. If you process the whole ferry history without `--start-date`, it downloads hundreds of them (50+ GB) and takes hours.
- A year of processed rapid transit and bus data is several GB. `data/` is gitignored.
- `backfill.main` also accepts `--start-date/--end-date` to process only part of a year.
- Every backfill command accepts `--upload` to go straight to step 3. For a normal update it's safer to process first, then dry-run the upload.

### 3. Upload to S3

```sh
# See what would change. Nothing is written.
uv run python -m mbta-performance.chalicelib.historic.upload --mode rapid --dry-run

# Upload
uv run python -m mbta-performance.chalicelib.historic.upload --mode rapid
```

Options: `--mode` is repeatable and defaults to all modes. `--start YYYY-MM` / `--end YYYY-MM` limit the months.

The upload works like a sync. Processing is deterministic (gzip `mtime=0`), so any file whose MD5 matches the existing S3 object's ETag is skipped, and only new or changed months get written. The dry run lists how many files would change in each month. **If a month you didn't expect to change shows up, stop and diff a file before uploading.** A change usually means the processing code behaves differently, or the MBTA changed the source.

### 4. Verify

- Re-run `historic.status`. The "latest in S3" column should now show the new month.
- Re-run the upload with `--dry-run`. It should report `would upload: 0`.
- Spot-check a file:
  ```sh
  aws s3 ls s3://tm-mbta-performance/Events/monthly-data/70061/Year=2026/
  aws s3 cp s3://tm-mbta-performance/Events/monthly-data/70061/Year=2026/Month=8/events.csv.gz - | gunzip | head
  ```
  Check that `event_time` values fall within normal service hours (roughly 5am–1am). A block of events at 1–4am means a timezone problem (see below).
- After the dashboard follow-up (step 5) is deployed, open a new month on the dashboard. For example, look at Red Line travel times from Alewife.

### 5. Follow-ups in t-performance-dash

The dashboard only reads monthly files up to hard-coded cutoffs, so new data isn't visible until you bump them. `historic.status` prints the exact values to use.

- `server/chalicelib/date_utils.py`: `MAX_MONTH_DATA_DATE`. This is the last day of the latest month present for **both** rapid transit and bus, because one cutoff is shared by both modes. Dates after it are read from daily LAMP data instead.
- `common/constants/dates.ts`: `BUS_MAX_DATE` and `FERRY_MAX_DATE`, the last day of each mode's latest month.

Example PR: [t-performance-dash#1104](https://github.com/transitmatters/t-performance-dash/pull/1104). Deploy the dashboard after merging.

## When a new year starts

The MBTA publishes each calendar year as a new ArcGIS item, usually once January's data is ready (February or March).

1. Find the new items on the hub by searching for "MBTA Rapid Transit Events YYYY" and "MBTA Bus Arrival Departure Times YYYY". The item ID is the 32-character hex string in the item URL.
2. Add them to `ARCGIS_IDS` and `BUS_ARCGIS_IDS` in `constants.py`. Example: [#84](https://github.com/transitmatters/mbta-performance/pull/84).
3. Run the new year once and check the output before uploading. Things that have changed between years before:
   - Column names. Rapid transit switched to LAMP-style columns (`sync_stop_sequence`) in 2024. See `HISTORIC_COLUMNS_LAMP`.
   - Bus zip layout and file names. See `process_bus_file_names` in `download.py`.
   - Timezones (see below).
4. Re-run the **previous** year one last time. The MBTA often re-publishes it with December filled in weeks later (the 2025 files were updated 2026-02-10). The upload sync skips the months that didn't change.
5. Update the tests that reference specific years, if needed. Run `uv run pytest mbta-performance`.

## When a route or station changes

Processing in this repo doesn't depend on routes. Every stop and route in the source files is processed, so a route change usually needs no code change here. The follow-ups are in the dashboard and in a few mappings:

- **Bus routes (new, renamed, or rerouted):**
  - In `t-performance-dash/server/bus`, add the route to the lists in `gen_manifests.sh` and `check_latest_manifests.sh`.
  - Regenerate the route manifests in `common/constants/bus_constants/*.json` and compare them against the old ones.
  - Update `common/constants/stations.ts`, `common/types/lines.ts`, and `common/constants/baselines.ts` as needed. Examples: [t-performance-dash#988](https://github.com/transitmatters/t-performance-dash/pull/988), [#1074](https://github.com/transitmatters/t-performance-dash/pull/1074).
  - When a route's stops change, new `{route}-{direction}-{stop}` folders appear. The old ones stay in S3, and that's expected. Don't delete them, because older dates still use them.
- **Rapid transit stations (new, or changed stop IDs):** update `common/constants/stations.ts` in t-performance-dash. Stops are processed by `stop_id`, so new IDs show up automatically in `monthly-data/`.
- **Ferry:** the source uses free-text terminal names and unofficial route labels. A new terminal or route needs an entry in `station_mapping` or `unofficial_ferry_labels_map` in `constants.py`. Otherwise its events are written under the raw name.

## Bus timezones

The bus source files always carry a `Z` suffix, but that suffix hasn't always meant UTC:

| Period                  | Actual timezone                  |
| ----------------------- | -------------------------------- |
| up to May 2024          | Eastern (mislabelled `Z`)        |
| June 2024 – January 2026 | UTC                              |
| February 2026 onward    | Eastern again (mislabelled `Z`)  |

`load_bus_data` works out which case applies from each file's data: service is quietest around 2–4am local. It falls back to the table above only for very small inputs. If the MBTA switches again, detection should handle it. Still, check the spot-check in step 4 after each update.

## Full rebuilds

These reprocess every year and produce a **lot** of data (the full bus history is tens of GB). Only do this after changing the processing code, or to repopulate an empty bucket.

```sh
uv run python -m mbta-performance.chalicelib.historic.backfill.main   # all rapid transit years
uv run python -m mbta-performance.chalicelib.historic.backfill.bus    # all bus years
```

Then upload with `historic.upload`. Only files that actually changed get written.
