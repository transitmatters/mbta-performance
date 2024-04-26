# Monthly Data Processing

MBTA uploads monthly data files periodically. These monthly batches take the place of performance data when available (This may change with LAMP).

## Backfill all years

This should only be done if we change the processing code or need to repopulate an empty bucket

```sh
poetry run python -m mbta-performance.chalicelib.historic.backfill.main
```

### Upload to S3

```sh
aws s3 cp --recursive data/output/Events/ s3://tm-mbta-performance/Events/
```
