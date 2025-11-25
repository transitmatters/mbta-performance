# 🏮 MBTA Performance Processing (LAMPLighter)

Scripts for processing MBTA performance data both from LAMP and from monthly historical files

## Setup

### Requirements

- Python 3.12
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
