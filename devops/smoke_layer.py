"""Smoke-test a shrunk Lambda layer inside the Lambda Python runtime image.

`shrink` in helpers.sh deletes parts of pyarrow (and other packages) that we believe are never
loaded. This imports everything the Lambda handlers need and exercises the pyarrow paths we use,
so an over-eager trim fails CI instead of failing in production. Run via `smoke_layer` in helpers.sh.
"""

import io
from zoneinfo import ZoneInfo

import boto3
import datadog_lambda.wrapper  # noqa: F401
import mbta_gtfs_sqlite  # noqa: F401
import numpy  # noqa: F401
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset  # noqa: F401
import pyarrow.fs  # noqa: F401
import pyarrow.parquet as pq
import sqlalchemy  # noqa: F401

# Parquet round trip through pandas, the way the LAMP ingest reads files
df = pd.DataFrame({"id": ["a", "b", "c"], "ts": [1, 2, 3]})
buffer = io.BytesIO()
df.to_parquet(buffer)
buffer.seek(0)
read = pd.read_parquet(buffer, engine="pyarrow", dtype_backend="numpy_nullable")
assert len(read) == 3, read

# ParquetFile + Table.filter with an expression, the Acero path used by lamp/alerts.py
buffer.seek(0)
table = pq.ParquetFile(buffer).read()
filtered = table.filter(pc.field("ts") >= pa.scalar(2, pa.int64()))
assert filtered.num_rows == 2, filtered

# Clients for the services we call; shrink deletes every other botocore service model
boto3.client("s3", region_name="us-east-1")
boto3.client("cloudfront", region_name="us-east-1")
boto3.resource("s3", region_name="us-east-1")
boto3.resource("sqs", region_name="us-east-1")

# Timezones resolve from the layer's tzdata (shrink drops most zones but keeps US/*)
ZoneInfo("US/Eastern")

print(f"Layer smoke test passed: pandas {pd.__version__}, pyarrow {pa.__version__}")
