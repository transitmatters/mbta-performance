import boto3
import io
import pandas as pd
import zlib
import time

s3 = boto3.client("s3")
cloudfront = boto3.client("cloudfront")


# General downloading/uploading
def download(bucket, key, encoding="utf8", compressed=True):
    """Download and optionally decompress an object from S3.

    Args:
        bucket: Name of the S3 bucket.
        key: S3 object key to download.
        encoding: Character encoding used to decode the bytes. Defaults to "utf8".
        compressed: If True, decompress the object using zlib/gzip before returning.
            Defaults to True.

    Returns:
        The decoded string contents of the S3 object.
    """
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
    """Upload bytes to S3, optionally compressing with zlib first.

    Args:
        bucket: Name of the S3 bucket.
        key: S3 object key to write to.
        bytes: Raw bytes to upload.
        compress: If True, compress the bytes with zlib before uploading.
            Defaults to True.
    """
    if compress:
        bytes = zlib.compress(bytes)
    s3.put_object(Bucket=bucket, Key=key, Body=bytes)


def upload_df_as_csv(bucket, key, df):
    """Upload a pandas DataFrame to S3 as an uncompressed CSV file.

    Args:
        bucket: Name of the S3 bucket.
        key: S3 object key to write to. Coerced to string.
        df: DataFrame to serialize and upload.
    """
    key = str(key)

    buffer = io.BytesIO()
    df.to_csv(buffer, compression=None, encoding="utf-8", index=False)
    buffer.seek(0)

    s3.upload_fileobj(buffer, bucket, Key=key, ExtraArgs={"ContentType": "text/csv"})


def download_csv_as_df(bucket, key):
    """Download a CSV file from S3 and return it as a pandas DataFrame.

    Args:
        bucket: Name of the S3 bucket.
        key: S3 object key to read. Coerced to string.

    Returns:
        A DataFrame containing the CSV contents.
    """
    key = str(key)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


def ls(bucket, prefix):
    """List all object keys in an S3 bucket under a given prefix.

    Handles pagination automatically so all matching keys are returned
    regardless of result set size.

    Args:
        bucket: Name of the S3 bucket.
        prefix: Key prefix to filter results.

    Returns:
        A list of S3 object key strings matching the prefix.
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_keys = []
    for page in pages:
        keys = [x["Key"] for x in page["Contents"]]
        all_keys.extend(keys)

    return all_keys


def clear_cf_cache(distribution, keys):
    """Create a CloudFront invalidation to clear cached responses for the given paths.

    Args:
        distribution: The CloudFront distribution ID to invalidate.
        keys: A list of path strings to invalidate (e.g. ["/data/file.csv"]).
    """
    cloudfront.create_invalidation(
        DistributionId=distribution,
        InvalidationBatch={
            "Paths": {"Quantity": len(keys), "Items": keys},
            "CallerReference": str(time.time()).replace(".", ""),
        },
    )
