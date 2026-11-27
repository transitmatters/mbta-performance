import hashlib
import pathlib
import tempfile
import unittest
import zipfile
from datetime import date
from unittest import mock

import botocore.exceptions

from .. import source_archive


def write_zip(path, files):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return path


def client_error(code):
    return botocore.exceptions.ClientError({"Error": {"Code": code}}, "HeadObject")


class TestArchiveKey(unittest.TestCase):
    def test_keys(self):
        self.assertEqual(source_archive.archive_key("rapid", "2016"), "ArcGIS-archive/rapid/2016.zip")
        self.assertEqual(source_archive.archive_key("bus", "2026"), "ArcGIS-archive/bus/2026.zip")
        self.assertEqual(source_archive.archive_key("ferry"), "ArcGIS-archive/ferry/ferry.csv")


class TestS3Etag(unittest.TestCase):
    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())

    def test_small_file_is_plain_md5(self):
        path = self.dir / "small.csv"
        path.write_bytes(b"abc")
        self.assertEqual(source_archive.s3_etag(str(path)), hashlib.md5(b"abc").hexdigest())

    def test_large_file_is_multipart(self):
        """Above one part, S3's ETag is the MD5 of the parts' MD5s, then the part count."""
        part_a = b"a" * source_archive.PART_SIZE
        part_b = b"b" * 10
        path = self.dir / "large.zip"
        path.write_bytes(part_a + part_b)

        expected = hashlib.md5(hashlib.md5(part_a).digest() + hashlib.md5(part_b).digest()).hexdigest()
        self.assertEqual(source_archive.s3_etag(str(path)), f"{expected}-2")


class TestValidateSourceFile(unittest.TestCase):
    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())

    def test_valid_zip(self):
        path = write_zip(self.dir / "2026.zip", {"a.csv": "service_date,stop_id\n2026-01-01,70061\n", "README.txt": ""})
        source_archive.validate_source_file(str(path))

    def test_valid_zip_with_subdirectory(self):
        files = {"MBTA Bus 2019/Q1.csv": "h\n1\n", "MBTA Bus 2019/Q2.csv": "h\n2\n"}
        source_archive.validate_source_file(str(write_zip(self.dir / "2019.zip", files)))

    def test_valid_csv(self):
        path = self.dir / "ferry.csv"
        path.write_text("service_date,route_id\n2026-01-01,F1\n")
        source_archive.validate_source_file(str(path))

    def test_not_a_zip(self):
        """An error page saved as the zip, or a truncated download."""
        path = self.dir / "2026.zip"
        path.write_bytes(b"<html>Item does not exist or is inaccessible</html>")
        with self.assertRaisesRegex(source_archive.InvalidSourceFile, "isn't a valid zip"):
            source_archive.validate_source_file(str(path))

    def test_truncated_zip(self):
        path = write_zip(self.dir / "2026.zip", {"a.csv": "h\n1\n" * 1000})
        path.write_bytes(path.read_bytes()[:-30])
        with self.assertRaises(source_archive.InvalidSourceFile):
            source_archive.validate_source_file(str(path))

    def test_zip_without_csvs(self):
        path = write_zip(self.dir / "2026.zip", {"README.txt": "nothing here"})
        with self.assertRaisesRegex(source_archive.InvalidSourceFile, "no CSV files"):
            source_archive.validate_source_file(str(path))

    def test_zip_with_header_only_csv(self):
        path = write_zip(self.dir / "2026.zip", {"good.csv": "h\n1\n", "empty.csv": "h\n"})
        with self.assertRaisesRegex(source_archive.InvalidSourceFile, "empty.csv"):
            source_archive.validate_source_file(str(path))

    def test_header_only_csv(self):
        path = self.dir / "ferry.csv"
        path.write_text("service_date,route_id\n")
        with self.assertRaisesRegex(source_archive.InvalidSourceFile, "no rows"):
            source_archive.validate_source_file(str(path))

    def test_empty_csv(self):
        path = self.dir / "ferry.csv"
        path.write_bytes(b"")
        with self.assertRaises(source_archive.InvalidSourceFile):
            source_archive.validate_source_file(str(path))

    def test_deflate64_member_is_read_with_unzip(self):
        """Python can't open Deflate64 members (the 2020 bus files), so their first lines come from unzip."""
        path = write_zip(self.dir / "2020.zip", {"2020-Q1.csv": "h\n1\n"})
        with (
            mock.patch.object(zipfile.ZipFile, "open", side_effect=NotImplementedError("Deflate64")),
            mock.patch("subprocess.Popen") as popen,
        ):
            popen.return_value.stdout.readline.side_effect = [b"h\n", b"1\n"]
            source_archive.validate_source_file(str(path))

        self.assertEqual(popen.call_args.args[0], ["unzip", "-p", str(path), "2020-Q1.csv"])
        popen.return_value.kill.assert_called_once()


class TestArchiveSourceFile(unittest.TestCase):
    def setUp(self):
        self.path = write_zip(pathlib.Path(tempfile.mkdtemp()) / "2026.zip", {"a.csv": "h\n1\n"})

    def test_uploads_new_file_to_glacier_ir_with_metadata(self):
        with (
            mock.patch("chalicelib.s3.head_etag", return_value=None),
            mock.patch("chalicelib.s3.s3") as client,
        ):
            uploaded = source_archive.archive_source_file(
                str(self.path), "rapid", "2026", item_id="abc123", source_date=date(2026, 11, 5)
            )

        self.assertTrue(uploaded)
        client.upload_file.assert_called_once()
        args, kwargs = client.upload_file.call_args
        self.assertEqual(args, (str(self.path), "tm-mbta-performance", "ArcGIS-archive/rapid/2026.zip"))
        self.assertEqual(
            kwargs["ExtraArgs"],
            {"StorageClass": "GLACIER_IR", "Metadata": {"arcgis-item-id": "abc123", "source-date": "2026-11-05"}},
        )

    def test_skips_identical_file(self):
        with (
            mock.patch("chalicelib.s3.head_etag", return_value=hashlib.md5(self.path.read_bytes()).hexdigest()),
            mock.patch("chalicelib.s3.s3") as client,
        ):
            uploaded = source_archive.archive_source_file(str(self.path), "rapid", "2026")

        self.assertFalse(uploaded)
        client.upload_file.assert_not_called()

    def test_invalid_file_is_not_archived(self):
        self.path.write_bytes(b"not a zip")
        with (
            mock.patch("chalicelib.s3.head_etag") as mock_head,
            mock.patch("chalicelib.s3.s3") as client,
        ):
            uploaded = source_archive.archive_source_file(str(self.path), "rapid", "2026")

        self.assertFalse(uploaded)
        mock_head.assert_not_called()
        client.upload_file.assert_not_called()

    def test_failed_upload_only_warns(self):
        """Without credentials for the archive, a backfill still goes on."""
        with (
            mock.patch("chalicelib.s3.head_etag", return_value=None),
            mock.patch("chalicelib.s3.s3") as client,
        ):
            client.upload_file.side_effect = botocore.exceptions.NoCredentialsError()
            uploaded = source_archive.archive_source_file(str(self.path), "bus", "2026")

        self.assertFalse(uploaded)


class TestRestoreSourceFile(unittest.TestCase):
    def test_downloads_archived_copy(self):
        path = str(pathlib.Path(tempfile.mkdtemp()) / "bus" / "2018.zip")
        with mock.patch("chalicelib.s3.s3") as client:
            client.head_object.return_value = {"ETag": '"abc-23"', "Metadata": {"source-date": "2022-12-19"}}
            self.assertEqual(source_archive.restore_source_file(path, "bus", "2018"), path)

        args, _ = client.download_file.call_args
        self.assertEqual(args, ("tm-mbta-performance", "ArcGIS-archive/bus/2018.zip", path))

    def test_matching_local_file_is_used_without_downloading(self):
        path = write_zip(pathlib.Path(tempfile.mkdtemp()) / "2018.zip", {"a.csv": "h\n1\n"})
        etag = hashlib.md5(path.read_bytes()).hexdigest()
        with mock.patch("chalicelib.s3.s3") as client:
            client.head_object.return_value = {"ETag": f'"{etag}"', "Metadata": {}}
            self.assertEqual(source_archive.restore_source_file(str(path), "bus", "2018"), str(path))

        client.download_file.assert_not_called()

    def test_different_local_file_is_replaced(self):
        """A stale or partly written local file doesn't match the archive's ETag, so the archived copy is downloaded."""
        path = write_zip(pathlib.Path(tempfile.mkdtemp()) / "2018.zip", {"a.csv": "h\n1\n"})
        with mock.patch("chalicelib.s3.s3") as client:
            client.head_object.return_value = {"ETag": '"something-else-23"', "Metadata": {}}
            source_archive.restore_source_file(str(path), "bus", "2018")

        client.download_file.assert_called_once()

    def test_missing_copy_raises_file_not_found(self):
        with mock.patch("chalicelib.s3.s3") as client:
            client.head_object.side_effect = client_error("404")
            with self.assertRaises(FileNotFoundError):
                source_archive.restore_source_file("data/input/1999.zip", "rapid", "1999")
        client.download_file.assert_not_called()
