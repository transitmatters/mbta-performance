"""Read a single service date out of a remote parquet file using HTTP range requests.

The LAMP bus exports are large (LAMP_ALL_Bus_Events is ~6.8GB) but are partitioned with
one row group per service_date. Reading the footer and then a single row group pulls
roughly 35MB for a day, rather than the whole file.
"""

import io
import logging
import time
from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import requests

logger = logging.getLogger(__name__)

HEAD_TIMEOUT_SECONDS = 30
RANGE_TIMEOUT_SECONDS = 120

# LAMP_RECENT_Bus_Events.parquet is rewritten by MBTA on a rolling basis (it's the live
# seven-day export, unlike the static all-time archive). A read that spans the moment it's
# replaced can pair a footer parsed from the old file with byte ranges served from the new
# one -- surfacing as a truncated HTTP read (requests.exceptions.ChunkedEncodingError) or a
# corrupt parquet footer (pyarrow raises a plain OSError, e.g. "Couldn't deserialize
# thrift"). Both are IOError/OSError under the hood. Retrying re-opens the file and re-reads
# the footer from scratch, which self-heals once the rewrite finishes -- observed in
# practice needing at most one retry once the file has settled.
MAX_READ_ATTEMPTS = 4
READ_RETRY_DELAY_SECONDS = 5


class HttpRangeFile(io.RawIOBase):
    """A seekable read-only file over HTTP, backed by Range requests.

    pyarrow only needs seek/read/tell to parse a parquet footer and fetch selected
    column chunks, so this is enough to treat a remote parquet as random access.
    """

    def __init__(self, url: str, session: requests.Session | None = None):
        self.url = url
        self._session = session or requests.Session()
        self._pos = 0
        response = self._session.head(url, timeout=HEAD_TIMEOUT_SECONDS)
        response.raise_for_status()
        if "content-length" not in response.headers:
            raise ValueError(f"{url} did not report a content-length; cannot range-read it")
        self.size = int(response.headers["content-length"])

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self.size + offset
        else:
            raise ValueError(f"invalid whence {whence}")
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = self.size - self._pos
        if size == 0 or self._pos >= self.size:
            return b""
        last = min(self._pos + size, self.size) - 1
        response = self._session.get(
            self.url, headers={"Range": f"bytes={self._pos}-{last}"}, timeout=RANGE_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        self._pos += len(response.content)
        return response.content


def _row_group_for_date(parquet_file: pq.ParquetFile, column: str, service_date: date) -> int | None:
    """Find the row group whose statistics cover only `service_date`."""
    metadata = parquet_file.metadata
    column_index = parquet_file.schema_arrow.get_field_index(column)
    if column_index < 0:
        raise ValueError(f"{column} not present in remote parquet schema")

    for group in range(metadata.num_row_groups):
        statistics = metadata.row_group(group).column(column_index).statistics
        if statistics is None:
            continue
        if statistics.min <= service_date <= statistics.max:
            return group
    return None


def read_service_date(url: str, service_date: date, columns: list[str]) -> pd.DataFrame:
    """Read one service date out of a remote, date-partitioned parquet file.

    Retried a few times on a network or parquet-parsing error -- see MAX_READ_ATTEMPTS above
    for why a live, actively-rewritten export needs this and a static one is just being
    handled defensively.
    """
    for attempt in range(1, MAX_READ_ATTEMPTS + 1):
        try:
            return _read_service_date_once(url, service_date, columns)
        except OSError as error:
            if attempt == MAX_READ_ATTEMPTS:
                raise
            logger.warning(
                f"Attempt {attempt}/{MAX_READ_ATTEMPTS} to read {service_date} from {url} failed "
                f"({error!r}); retrying in {READ_RETRY_DELAY_SECONDS}s"
            )
            time.sleep(READ_RETRY_DELAY_SECONDS)
    raise AssertionError("unreachable")  # loop always returns or raises


def _read_service_date_once(url: str, service_date: date, columns: list[str]) -> pd.DataFrame:
    logger.info(f"Opening remote parquet {url}")
    parquet_file = pq.ParquetFile(HttpRangeFile(url))

    group = _row_group_for_date(parquet_file, "service_date", service_date)
    if group is None:
        raise ValueError(f"{url} has no row group covering service date {service_date}")

    logger.info(f"Reading row group {group} for {service_date} ({len(columns)} columns)")
    table = parquet_file.read_row_group(group, columns=columns)
    frame = table.to_pandas()

    # A row group's statistics are a range; when a file is not perfectly one-day-per-group
    # this keeps us honest.
    frame = frame[frame["service_date"] == service_date]
    logger.info(f"Read {len(frame)} bus event rows for {service_date}")
    return frame
