# Image for the bus speed segment jobs in mbta-performance/chalicelib/lamp/bus, which run as
# k8s CronJobs from transitmatters/basement-dweller instead of as Lambdas: they need tippecanoe
# and the geo dependency group, and the monthly rollup peaks at ~8GB. The Chalice app doesn't
# use this image.
FROM python:3.14-slim-trixie

LABEL org.opencontainers.image.source=https://github.com/transitmatters/mbta-performance

# tippecanoe builds the map's PMTiles; tile-join ships in the same package.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tippecanoe \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.10.1 /uv /usr/local/bin/uv

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

# Dependencies before code, so a code-only change reuses this layer.
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --group geo --no-cache

COPY mbta-performance/chalicelib mbta-performance/chalicelib

RUN useradd --uid 1000 --create-home lamplighter
USER 1000

# The CronJobs set their own command; see charts/bus-speed-segments in basement-dweller.
CMD ["python", "-m", "mbta-performance.chalicelib.lamp.bus.ingest", "--help"]
