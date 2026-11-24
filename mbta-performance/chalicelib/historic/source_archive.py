"""
Our own copy of every source file downloaded from ArcGIS, in s3://tm-mbta-performance/ArcGIS-archive/.

The MBTA has removed old years from ArcGIS before (rapid transit 2016-2019, bus 2018-2019), so each download is
archived, and a backfill falls back to the archived copy when ArcGIS no longer has the item. Only the latest copy of
each file is kept, in Glacier Instant Retrieval: ~4.4 GB is ~$0.02/month, and reading it all back once is ~$0.13.
"""

import hashlib
import os
import subprocess
from datetime import date
from zipfile import BadZipFile, ZipFile

import botocore.exceptions
from boto3.s3.transfer import TransferConfig

from .. import s3
from .constants import HISTORIC_S3_BUCKET

ARCHIVE_PREFIX = "ArcGIS-archive"
STORAGE_CLASS = "GLACIER_IR"

# boto3's and the AWS CLI's default part size, which the archive was seeded with. A multipart object's ETag depends
# on the part size, so uploads pin it to keep comparing local files with the archive cheap (a HEAD, no download).
PART_SIZE = 8 * 1024 * 1024
TRANSFER_CONFIG = TransferConfig(multipart_threshold=PART_SIZE, multipart_chunksize=PART_SIZE)


def archive_key(mode: str, year: str = None) -> str:
    """Where a source file is archived: rapid and bus have one zip per year, ferry one CSV for every year."""
    if mode == "ferry":
        return f"{ARCHIVE_PREFIX}/ferry/ferry.csv"
    return f"{ARCHIVE_PREFIX}/{mode}/{year}.zip"


class InvalidSourceFile(ValueError):
    """A downloaded file that isn't usable data: a truncated zip, an error page, or a CSV with no rows."""


def _has_rows(stream) -> bool:
    """Whether a CSV stream has a header and at least one row after it. Reads just those two lines."""
    header, first_row = stream.readline(), stream.readline()
    return bool(header.strip()) and bool(first_row.strip())


def _zip_member_has_rows(zip_file: ZipFile, path: str, name: str) -> bool:
    try:
        with zip_file.open(name) as member:
            return _has_rows(member)
    except NotImplementedError:
        # Python can't read Deflate64, which the 2020 bus files use; stream the start of the file from unzip instead
        proc = subprocess.Popen(["unzip", "-p", path, name], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            return _has_rows(proc.stdout)
        finally:
            proc.kill()
            proc.wait()


def validate_source_file(path: str):
    """
    Raise InvalidSourceFile unless path is a CSV, or a zip of CSVs, where every CSV has a header and at least one
    row. Only the first two lines of each CSV are read, so this is quick even for a year's zip.
    """
    if path.endswith(".zip"):
        try:
            with ZipFile(path) as zip_file:
                csvs = [i.filename for i in zip_file.infolist() if i.filename.lower().endswith(".csv")]
                if not csvs:
                    raise InvalidSourceFile(f"{path} has no CSV files in it")
                empty = [name for name in csvs if not _zip_member_has_rows(zip_file, path, name)]
        except BadZipFile as e:
            raise InvalidSourceFile(f"{path} isn't a valid zip file: {e}") from e
        if empty:
            raise InvalidSourceFile(f"{path} has CSV files with no rows: {empty}")
    else:
        with open(path, "rb") as f:
            if not _has_rows(f):
                raise InvalidSourceFile(f"{path} has no rows")


def s3_etag(path: str) -> str:
    """The ETag S3 gives this file when it's uploaded with TRANSFER_CONFIG."""
    digests = []
    with open(path, "rb") as f:
        while chunk := f.read(PART_SIZE):
            digests.append(hashlib.md5(chunk).digest())
    if os.path.getsize(path) < PART_SIZE:
        # Uploaded in one piece, so the ETag is the file's own MD5
        return digests[0].hex() if digests else hashlib.md5(b"").hexdigest()
    return f"{hashlib.md5(b''.join(digests)).hexdigest()}-{len(digests)}"


def archive_source_file(path: str, mode: str, year: str = None, item_id: str = None, source_date: date = None) -> bool:
    """
    Copy a downloaded source file to the archive, unless it isn't valid (see validate_source_file) or the archive
    already has the same bytes. Returns True if it uploaded. A failed upload (no credentials, say) only prints a
    warning, so processing can go on.
    """
    key = archive_key(mode, year)
    try:
        validate_source_file(path)
    except InvalidSourceFile as e:
        print(f"Warning: not archiving to s3://{HISTORIC_S3_BUCKET}/{key}: {e}")
        return False
    try:
        if s3.head_etag(HISTORIC_S3_BUCKET, key) == s3_etag(path):
            print(f"s3://{HISTORIC_S3_BUCKET}/{key} is already up to date")
            return False
        metadata = {}
        if item_id:
            metadata["arcgis-item-id"] = item_id
        if source_date:
            metadata["source-date"] = source_date.strftime("%Y-%m-%d")
        print(f"Archiving {path} to s3://{HISTORIC_S3_BUCKET}/{key}...")
        s3.s3.upload_file(
            path,
            HISTORIC_S3_BUCKET,
            key,
            ExtraArgs={"StorageClass": STORAGE_CLASS, "Metadata": metadata},
            Config=TRANSFER_CONFIG,
        )
        return True
    except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as e:
        print(f"Warning: couldn't archive {path} to s3://{HISTORIC_S3_BUCKET}/{key}: {e}")
        return False


def restore_source_file(path: str, mode: str, year: str = None) -> str:
    """
    Download the archived copy of a source file to path, unless path already has the same bytes (a Glacier IR read
    is billed per GB). Raises FileNotFoundError if there's no archived copy.
    """
    key = archive_key(mode, year)
    try:
        head = s3.s3.head_object(Bucket=HISTORIC_S3_BUCKET, Key=key)
    except botocore.exceptions.ClientError as e:
        raise FileNotFoundError(f"there's no archived copy at s3://{HISTORIC_S3_BUCKET}/{key}") from e

    source = f"s3://{HISTORIC_S3_BUCKET}/{key} (source date {head['Metadata'].get('source-date', 'unknown')})"
    if os.path.isfile(path) and s3_etag(path) == head["ETag"].strip('"'):
        print(f"Using {path}, which matches the archived {source}")
        return path

    print(f"Using the archived {source}")
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    s3.s3.download_file(HISTORIC_S3_BUCKET, key, path, Config=TRANSFER_CONFIG)
    return path
