# Monthly Data Processing

MBTA uploads monthly data files periodically. These monthly batches take the place of performance data when available (This may change with LAMP).

## Backfill Ferry Data

MBTA updates the Ferry data ~every 6 months or so.

```sh
uv run python -m mbta-performance.chalicelib.historic.backfill.ferry
```

## Backfill all years

This should only be done if we change the processing code or need to repopulate an empty bucket

```sh
uv run python -m mbta-performance.chalicelib.historic.backfill.main
```

## Backfill Bus Data

This will produce a **huge** amount of data, run with caution. Watch your disk space.

```sh
uv run python -m mbta-performance.chalicelib.historic.backfill.bus
```

### Upload to S3

```sh
aws s3 cp --recursive data/output/Events/ s3://tm-mbta-performance/Events/
```
