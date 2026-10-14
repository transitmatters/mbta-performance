# 🏮 MBTA Performance Processing (LAMPLighter)

Scripts for processing MBTA performance data both from LAMP and from monthly historical files

## Setup

### Requirements

- Python 3.12 (with [mise](https://mise.jdx.dev): `mise trust && mise install`, pinned in `.python-version`)
- [uv](https://docs.astral.sh/uv/) - Fast Python package manager
  - Install: `curl -LsSf https://astral.sh/uv/install.sh | sh`
  - Verify: `uv --version`

### Installation

1. Install dependencies:

   ```shell
   uv sync --group dev
   ```

2. Set up pre-commit hooks:

   ```shell
   uv run pre-commit install
   ```

3. Configure AWS credentials (required for running locally):
   - Add `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your shell environment, OR
   - Configure with awscli: `aws configure`

## Development

### Testing

Run tests with pytest:

```shell
uv run pytest mbta-performance
```

### Linting & Formatting

Check code style with Ruff:

```shell
uv run ruff check mbta-performance
uv run ruff format mbta-performance
```

## Run Locally

If you want more verbose logging and debug info, export `DEBUG` as log level. However this will heavily pollute logs with s3transfer and botocore logs.

```shell
export LOG_LEVEL="DEBUG"
```

GTFS feeds are cached locally (`data/gtfs_archives/` for historic, `./feeds` for LAMP backfills). To share one cache with other TransitMatters repos, point `GTFS_DATA_DIR` at a common directory; archives go in `$GTFS_DATA_DIR/gtfs_archives` and sqlite feeds in `$GTFS_DATA_DIR/sqlite_feeds`.

```shell
export GTFS_DATA_DIR="$HOME/transitmatters-gtfs"
```

### Run today's LAMP ingest

```shell
uv run python -m mbta-performance.chalicelib.lamp.ingest
```

### Backfill LAMP

```shell
uv run python -m mbta-performance.chalicelib.lamp.backfill.main
```

### Monthly (historic) data

Monthly updates for rapid transit, bus, and ferry use their own runbook, covering processing, S3 upload, verification, and the dashboard follow-ups. See [`mbta-performance/chalicelib/historic/README.md`](mbta-performance/chalicelib/historic/README.md).

```shell
uv run python -m mbta-performance.chalicelib.historic.status
```
