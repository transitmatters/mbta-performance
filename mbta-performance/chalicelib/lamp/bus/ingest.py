"""Generate bus speed segments for a service date.

Pipeline:
  LAMP bus events (one parquet row group per service date)
    -> located on their GTFS route pattern's shape
    -> missing stop times interpolated by distance
    -> per-traversal segment times
    -> aggregated per (route, direction, stop pair, date, time band)
    -> GeoParquet with road-following LineString geometry (for analysts)
    -> PMTiles built from the same result, opt-in (for the live map)
"""

import logging
from datetime import date, timedelta

import pandas as pd

from ...date import get_current_service_date
from .constants import (
    BUS_ALL_URL,
    BUS_COLUMNS,
    BUS_RECENT_URL,
    EARLIEST_LAMP_BUS_DATA,
    S3_BUCKET,
    S3_KEY_TEMPLATE,
)
from .daily_metrics import write_daily_route_metrics
from .geoparquet import write_geoparquet
from .patterns import build_pattern_geometry
from .remote_parquet import read_service_date
from .s3_writer import upload_pmtiles, upload_speed_segments
from .segments import (
    aggregate_segments,
    attach_shape_distance,
    build_traversals,
    interpolate_missing_times,
    select_segment_geometry,
)

logger = logging.getLogger(__name__)

# LAMP_RECENT_Bus_Events holds a rolling seven days and is refreshed through the day;
# anything older has to come out of the (much larger) all-time export.
RECENT_WINDOW_DAYS = 7


def source_url_for(service_date: date) -> str:
    """Prefer the small rolling export when the date is recent enough to be in it."""
    if service_date >= date.today() - timedelta(days=RECENT_WINDOW_DAYS - 1):
        return BUS_RECENT_URL
    earliest = date.fromisoformat(EARLIEST_LAMP_BUS_DATA)
    if service_date < earliest:
        raise ValueError(
            f"LAMP bus data starts at {earliest}; {service_date} predates it. "
            "Use the ArcGIS monthly archive in chalicelib.historic for earlier dates."
        )
    return BUS_ALL_URL


def generate_speed_segments(
    service_date: date,
    output_path: str | None = None,
    upload: bool = False,
    write_to_dynamo: bool = False,
    write_pmtiles: bool = False,
) -> pd.DataFrame:
    """Build the aggregated speed-segment table for one service date.

    Writes a local GeoParquet when `output_path` is given, and publishes to S3 when
    `upload` is set. Uploading is opt-in so a local run never touches the bucket.

    `write_to_dynamo` additionally rolls the same traversals up to one row per (route,
    service_date) -- miles covered, total time, trip count -- and batch-writes them to the
    DeliveredTripMetricsBus table, for the same kind of daily speed chart the dashboard
    already draws for rail. Also opt-in, and independent of `upload`: this is a per-route
    daily summary alongside the per-segment map data, not a replacement for it.

    `write_pmtiles` additionally builds a PMTiles tileset from the same result and publishes
    it alongside the GeoParquet, under the same S3 prefix -- this is what the live map
    actually reads. Also opt-in and independent of `upload`, and requires tippecanoe on
    PATH (see pmtiles.py).
    """
    logger.info(f"Generating bus speed segments for {service_date}")

    events = read_service_date(source_url_for(service_date), service_date, BUS_COLUMNS)
    if events.empty:
        raise ValueError(f"No LAMP bus events for {service_date}")

    geometry = build_pattern_geometry(service_date, route_pattern_ids=set(events.route_pattern_id.dropna()))
    if geometry.segments.empty:
        raise ValueError(f"No route pattern geometry could be built for {service_date}")

    events = attach_shape_distance(events, geometry.stop_positions)
    events = interpolate_missing_times(events)

    traversals = build_traversals(events, geometry.segments)
    if traversals.empty:
        raise ValueError(f"No usable segment traversals for {service_date}")

    if write_to_dynamo:
        write_daily_route_metrics(traversals)

    aggregated = aggregate_segments(traversals)
    chosen_geometry = select_segment_geometry(traversals, geometry.segments)

    result = aggregated.merge(
        chosen_geometry, on=["route_id", "direction_id", "from_stop_id", "to_stop_id"], how="inner"
    )
    logger.info(f"{len(result)} segment-band rows carry geometry (from {len(aggregated)} aggregated rows)")

    if output_path:
        write_geoparquet(result, output_path)
    if upload:
        upload_speed_segments(result, service_date)
    if write_pmtiles:
        upload_pmtiles(result, service_date)
    return result


def generate_yesterday_speed_segments(
    output_path: str | None = None,
    upload: bool = True,
    write_to_dynamo: bool = True,
    write_pmtiles: bool = True,
) -> pd.DataFrame:
    """Yesterday's service date is the first one LAMP has finished writing.

    This is the production entry point, so it publishes, writes daily metrics, and builds
    PMTiles by default.
    """
    return generate_speed_segments(
        get_current_service_date() - timedelta(days=1),
        output_path,
        upload=upload,
        write_to_dynamo=write_to_dynamo,
        write_pmtiles=write_pmtiles,
    )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Generate LAMP bus speed segments as GeoParquet.")
    parser.add_argument("--date", type=date.fromisoformat, help="Service date (YYYY-MM-DD), default yesterday")
    parser.add_argument("--out", default="bus_speed_segments.parquet", help="Output GeoParquet path")
    parser.add_argument("--upload", action="store_true", help=f"Also publish to s3://{S3_BUCKET}/{S3_KEY_TEMPLATE}")
    parser.add_argument(
        "--write-to-dynamo", action="store_true", help="Also write daily per-route metrics to DeliveredTripMetricsBus"
    )
    parser.add_argument(
        "--write-pmtiles", action="store_true", help="Also build and publish a PMTiles tileset (requires tippecanoe)"
    )
    arguments = parser.parse_args()

    target = arguments.date or (get_current_service_date() - timedelta(days=1))
    generate_speed_segments(
        target,
        arguments.out,
        upload=arguments.upload,
        write_to_dynamo=arguments.write_to_dynamo,
        write_pmtiles=arguments.write_pmtiles,
    )
