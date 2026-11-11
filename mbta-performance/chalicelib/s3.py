import boto3
import botocore.exceptions
import gzip
import hashlib
import io
import pandas as pd
import zlib
import time
from concurrent.futures import ThreadPoolExecutor

s3 = boto3.client("s3")
cloudfront = boto3.client("cloudfront")


# General downloading/uploading
def download(bucket, key, encoding="utf8", compressed=True):
    obj = s3.get_object(Bucket=bucket, Key=key)
    s3_data = obj["Body"].read()
    if not compressed:
        return s3_data.decode(encoding)
    # 32 should detect zlib vs gzip
    decompressed = zlib.decompress(s3_data, zlib.MAX_WBITS | 32).decode(encoding)
    return decompressed


# TODO: confirm if we want zlib or gzip compression
# note: alerts are zlib, but dashboard download code can handle either (in theory)
def upload(bucket, key, bytes, compress=True):
    if compress:
        bytes = zlib.compress(bytes)
    s3.put_object(Bucket=bucket, Key=key, Body=bytes)


def df_to_csv_bytes(df) -> bytes:
    """The exact bytes upload_df_as_csv sends, so a local MD5 can be compared with S3's ETag."""
    buffer = io.BytesIO()
    df.to_csv(buffer, compression=None, encoding="utf-8", index=False)
    return buffer.getvalue()


def upload_df_as_csv(bucket, key, df):
    key = str(key)

    buffer = io.BytesIO(df_to_csv_bytes(df))

    s3.upload_fileobj(buffer, bucket, Key=key, ExtraArgs={"ContentType": "text/csv"})


def upload_df_as_csv_if_changed(bucket, key, df, remote_etags=None) -> bool:
    """Upload df as a CSV unless S3 already holds the same bytes. Returns True if it uploaded.

    remote_etags says what's already in S3:
      - a {key: etag} dict listed up front with ls_etags_parallel (backfills),
      - None to HEAD this one key instead (Lambdas: a HEAD is billed like a GET, ~12x cheaper than a PUT),
      - {} to skip the check and always upload.

    The bucket uses SSE-S3, so a single-part object's ETag is the MD5 of its bytes. This uploads
    with one put_object (not upload_fileobj, which goes multipart above 8 MB) so that stays true
    for what it writes. A multipart ETag never matches, so that file is just uploaded again.
    """
    key = str(key)
    body = df_to_csv_bytes(df)
    remote_etag = head_etag(bucket, key) if remote_etags is None else remote_etags.get(key)
    if remote_etag == hashlib.md5(body).hexdigest():
        return False
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/csv")
    return True


def head_etag(bucket, key):
    """The object's ETag (quotes stripped), or None if it doesn't exist.

    Without s3:ListBucket, S3 answers a HEAD for a missing key with 403 instead of 404, so both
    count as missing. A real permissions problem still surfaces on the PUT that follows.
    """
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ETag"].strip('"')
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("403", "404", "NoSuchKey", "NotFound"):
            return None
        raise


def upload_parquet(bucket, key, data: bytes):
    """Upload already-serialised parquet bytes.

    Deliberately not zlib-wrapped the way upload() is: parquet carries its own internal
    compression, and readers (pandas, GeoPandas, DuckDB, the dashboard) expect to open the
    object directly rather than having to inflate it first.
    """
    key = str(key)
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/vnd.apache.parquet")


def upload_json(bucket, key, data: bytes):
    """Upload already-serialised JSON bytes, gzipped with `Content-Encoding: gzip`.

    Not zlib-wrapped the way upload() is: this is fetched directly by a browser's `fetch()`,
    which only inflates a body when the response's Content-Encoding says to, and S3 serves the
    ContentEncoding set here as exactly that header. The dashboard's CloudFront behavior for
    these files has compression off (it must, for the PMTiles beside them -- see
    upload_pmtiles), so this is the only compression they get. `mtime=0` keeps re-uploads of
    the same JSON byte-identical.
    """
    key = str(key)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=gzip.compress(data, mtime=0),
        ContentType="application/json",
        ContentEncoding="gzip",
    )


def upload_pmtiles(bucket, key, data: bytes):
    """Upload an already-built PMTiles tileset.

    Not zlib-wrapped, and for a stricter reason than upload_parquet: a PMTiles reader fetches
    individual tiles with HTTP Range requests directly against this object, so wrapping the
    whole file in a stream-level compression layer would make every ranged read return
    garbage instead of a valid tile.
    """
    key = str(key)
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/vnd.pmtiles")


def download_csv_as_df(bucket, key):
    key = str(key)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


def ls(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_keys = []
    for page in pages:
        keys = [x["Key"] for x in page["Contents"]]
        all_keys.extend(keys)

    return all_keys


def upload_file(bucket, key, path, content_type="text/csv"):
    """Upload a local file as-is (no re-compression) in a single PUT, so the ETag is its MD5."""
    with open(path, "rb") as f:
        s3.put_object(Bucket=bucket, Key=str(key), Body=f.read(), ContentType=content_type)


def ls_etags(bucket, prefix):
    """Return {key: etag} for every object under prefix (ETag quotes stripped). Empty dict if none."""
    paginator = s3.get_paginator("list_objects_v2")
    etags = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            etags[obj["Key"]] = obj["ETag"].strip('"')
    return etags


def ls_etags_parallel(bucket, prefix, workers=16):
    """ls_etags for everything under prefix, listing each immediate sub-prefix in parallel.

    For skipping unchanged files in a backfill, list once up front: a LIST returns up to
    1,000 keys and costs the same as a PUT, so listing per file would cost as much as the
    PUTs it saves. Objects sitting directly under prefix (not in a sub-prefix) are not listed.
    """
    etags = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for sub_etags in executor.map(lambda p: ls_etags(bucket, p), ls_prefixes(bucket, prefix)):
            etags.update(sub_etags)
    return etags


def ls_prefixes(bucket, prefix):
    """Return the immediate "subdirectory" prefixes under prefix (e.g. Year=2026/ under a stop)."""
    paginator = s3.get_paginator("list_objects_v2")
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        prefixes.extend(p["Prefix"] for p in page.get("CommonPrefixes", []))
    return prefixes


def clear_cf_cache(distribution, keys):
    cloudfront.create_invalidation(
        DistributionId=distribution,
        InvalidationBatch={
            "Paths": {"Quantity": len(keys), "Items": keys},
            "CallerReference": str(time.time()).replace(".", ""),
        },
    )
