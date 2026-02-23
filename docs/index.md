# LAMPLighter – MBTA Performance Processing

Scripts for processing MBTA performance data both from LAMP and from monthly historical files.

## Setup

### Requirements

- Python 3.12
- [uv](https://docs.astral.sh/uv/) – Fast Python package manager
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

```shell
uv run pytest mbta-performance
```

### Linting & Formatting

```shell
uv run ruff check mbta-performance
uv run ruff format mbta-performance
```

### Docs

Serve locally:

```shell
uv run mkdocs serve
```

Build static site:

```shell
uv run mkdocs build
```

## Run Locally

Export `DEBUG` for verbose logging (note: this will include noisy s3transfer/botocore output):

```shell
export LOG_LEVEL="DEBUG"
```

### Run today's LAMP ingest

```shell
uv run python -m mbta-performance.chalicelib.lamp.ingest
```

### Backfill LAMP

```shell
uv run python -m mbta-performance.chalicelib.lamp.backfill.main
```

### Backfill Historic

```shell
uv run python -m mbta-performance.chalicelib.historic.backfill.main
```
