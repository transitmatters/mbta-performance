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

Each command re-downloads that year's source zip and replaces the files extracted last time, so you always get the latest version of the file.

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

- Ferry first checks that the ArcGIS Hub's copy of the CSV is newer than the MBTA's last change to the data. If it isn't, the command has the Hub regenerate it and waits, for up to 30 minutes. The CSV is saved to `data/input/ferry.csv`.
- GTFS archives for scheduled headways are downloaded into `data/gtfs_archives/` and cached there. There is one archive per feed version, at roughly 100 MB unzipped each. If you process the whole ferry history without `--start-date`, it downloads hundreds of them (50+ GB) and takes hours.
- A day that GTFS can't cover (no archive for the date, or no trips matching the events) is still written, just without scheduled headways or travel times, and the day is printed. If a bus file fails to process, the other files still run, then the command lists the failures and stops before `--upload`.
- A year of processed rapid transit and bus data is several GB. `data/` is gitignored.
- `backfill.main` also accepts `--start-date/--end-date` to process only some months of a year.
- `--start-date/--end-date` always cover whole months. A mid-month date is widened to the 1st or to the month's last day, because each output file holds a whole month, and uploading it replaces that month in S3.
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
  Check that `event_time` values fall within normal service hours (roughly 5am–1am). A block of events at 1–4am means a timezone problem (see below). This catches a shift of several hours, but not one of a single hour.
- For bus, compare one day against gobble's live recordings. This is the check that catches a 1-hour timezone error, like #121 (see [Bus timezones](#bus-timezones)). Pick a weekday under daylight time if the new month has one, since that's when the readings can be an hour apart:
  ```sh
  aws s3 cp s3://tm-mbta-performance/Events/monthly-bus-data/1-1-67/Year=2026/Month=6/events.csv.gz - | gunzip | grep '^2026-06-10' | grep ',DEP,' | cut -d, -f10 | sort | head
  aws s3 cp s3://tm-mbta-performance/Events-live/daily-bus-data/1-1-67/Year=2026/Month=6/Day=10/events.csv.gz - | gunzip | grep ',DEP,' | cut -d, -f10 | head
  ```
  Gobble's times carry a UTC offset and the monthly file's don't, but both are Eastern, so most departures should match to within a minute. Gobble misses the odd trip. If the monthly times are consistently an hour off, fix the timezone handling and reprocess the month before the step 5 follow-up.
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
   - Column names. Rapid transit switched to LAMP-style columns (`sync_stop_sequence`) in 2024. See `HISTORIC_COLUMNS_LAMP`. Of the bus files, only Oct–Dec 2020 uses PascalCase headers (`ServiceDate`, `Stop`, ...); `load_bus_data` maps them.
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

The bus source files always carry a `Z` suffix, but it never means UTC. As currently published:

| Period                | What the times are                                                             |
| --------------------- | ------------------------------------------------------------------------------ |
| up to May 2024        | Eastern wall-clock time                                                        |
| June – December 2024  | Eastern + 5 hours, all year: the same as UTC in winter, an hour past it under DST |
| 2025 onward           | Eastern wall-clock time again                                                  |

The shifted months look like UTC, but under daylight time reading them as UTC puts every bus an hour late. Taking 5 hours off matches gobble's recordings to within a minute (route 1, stop 67, on days in June, August, October and November 2024). This table used to list 2025 and January 2026 as UTC too; as the MBTA republished them in October 2026, they're Eastern.

`load_bus_data` works out which case applies from each file's data: service is quietest around 2–4am local, which lands at 7–9 when shifted. It falls back to the table above only for very small inputs. Detection only tells these two cases apart, Eastern and Eastern + 5 hours, so it handles the MBTA switching between them again but not a new format. True UTC under daylight time, for example, is Eastern + 4 hours: its lull lands at 6–8, so it would count as shifted and come out an hour early. A 1-hour error still looks like normal service hours, so the service-hours spot check in step 4 can't catch it. After each bus update, run the gobble comparison in step 4 as well.

## Full rebuilds

These reprocess every year and produce a **lot** of data (the full bus history is tens of GB). Only do this after changing the processing code, or to repopulate an empty bucket.

```sh
uv run python -m mbta-performance.chalicelib.historic.backfill.main   # all rapid transit years
uv run python -m mbta-performance.chalicelib.historic.backfill.bus    # all bus years
```

Then upload with `historic.upload`. Only files that actually changed get written.
