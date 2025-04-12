# 🏮 MBTA Performance Processing (LAMPLighter)

Scripts for processing MBTA performance data both from LAMP and from monthly historical files

## Requirements to develop locally

- Python 3.12 with recent poetry (1.7.0 or later)
  - Verify with `python --version && poetry --version`
  - `poetry self update` to update poetry

## Instructions to run locally

1. Add your AWS credentials (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) to your shell environment, OR add them to a .boto config file with awscli command `aws configure`.

## Testing

1. From the `mbta-performance` directory, set up the poetry with `poetry shell; poetry install`
2. From the `mbta-performance` directory, run `poetry run pytest`

## Run Locally

### Run today's LAMP ingest

```shell
poetry run python -m mbta-performance.chalicelib.lamp.ingest
```

### Backfill LAMP

```shell
poetry run python -m mbta-performance.chalicelib.lamp.backfill.main
```

### Backfill Historic

```shell
poetry run python -m mbta-performance.chalicelib.historic.backfill.main
```
