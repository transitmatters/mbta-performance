import gzip
import hashlib
import io
import os
import tempfile
import unittest
import zlib
from unittest import mock

import botocore.exceptions
import pandas as pd

from .. import s3 as s3_module


class TestS3(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = mock.MagicMock()
        self.mock_cloudfront = mock.MagicMock()

    # --- download ---

    def test_download_uncompressed(self):
        mock_body = mock.Mock()
        mock_body.read.return_value = b"hello world"
        self.mock_s3.get_object.return_value = {"Body": mock_body}

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            result = s3_module.download("my-bucket", "my/key", compressed=False)

        self.mock_s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="my/key")
        self.assertEqual(result, "hello world")

    def test_download_compressed(self):
        original_data = b"some content to compress"
        compressed_data = zlib.compress(original_data)
        mock_body = mock.Mock()
        mock_body.read.return_value = compressed_data
        self.mock_s3.get_object.return_value = {"Body": mock_body}

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            result = s3_module.download("my-bucket", "my/key", compressed=True)

        self.assertEqual(result, "some content to compress")

    def test_download_default_is_compressed(self):
        original_data = b"default compressed"
        compressed_data = zlib.compress(original_data)
        mock_body = mock.Mock()
        mock_body.read.return_value = compressed_data
        self.mock_s3.get_object.return_value = {"Body": mock_body}

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            result = s3_module.download("my-bucket", "my/key")

        self.assertEqual(result, "default compressed")

    # --- upload ---

    def test_upload_uncompressed(self):
        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            s3_module.upload("my-bucket", "my/key", b"raw data", compress=False)

        self.mock_s3.put_object.assert_called_once_with(Bucket="my-bucket", Key="my/key", Body=b"raw data")

    def test_upload_compressed(self):
        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            s3_module.upload("my-bucket", "my/key", b"raw data", compress=True)

        call_kwargs = self.mock_s3.put_object.call_args[1]
        body = call_kwargs["Body"]
        # The body should be zlib-compressed and decompress back to the original
        decompressed = zlib.decompress(body)
        self.assertEqual(decompressed, b"raw data")
        self.assertEqual(call_kwargs["Bucket"], "my-bucket")
        self.assertEqual(call_kwargs["Key"], "my/key")

    def test_upload_json_is_gzipped_for_the_browser(self):
        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            s3_module.upload_json("my-bucket", "my/key.json", b'{"a": 1}')

        call_kwargs = self.mock_s3.put_object.call_args[1]
        self.assertEqual(gzip.decompress(call_kwargs["Body"]), b'{"a": 1}')
        self.assertEqual(call_kwargs["ContentType"], "application/json")
        self.assertEqual(call_kwargs["ContentEncoding"], "gzip")

    def test_upload_default_is_compressed(self):
        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            s3_module.upload("my-bucket", "my/key", b"data")

        call_kwargs = self.mock_s3.put_object.call_args[1]
        body = call_kwargs["Body"]
        # Default is compress=True, so body must be decompressible
        decompressed = zlib.decompress(body)
        self.assertEqual(decompressed, b"data")

    # --- upload_df_as_csv ---

    def test_upload_df_as_csv(self):
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            s3_module.upload_df_as_csv("my-bucket", "my/key.csv", df)

        self.mock_s3.upload_fileobj.assert_called_once()
        call_args = self.mock_s3.upload_fileobj.call_args
        # Positional: (buffer, bucket); Keyword: Key=..., ExtraArgs={...}
        self.assertEqual(call_args[0][1], "my-bucket")
        self.assertEqual(call_args[1]["Key"], "my/key.csv")
        self.assertEqual(call_args[1]["ExtraArgs"]["ContentType"], "text/csv")
        # Verify the buffer contains valid CSV content
        buffer = call_args[0][0]
        content = buffer.read().decode("utf-8")
        self.assertIn("col_a,col_b", content)
        self.assertIn("1,x", content)

    def test_upload_df_as_csv_key_coerced_to_string(self):
        """Key should be coerced to str (the function does str(key))."""
        df = pd.DataFrame({"a": [1]})

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            s3_module.upload_df_as_csv("my-bucket", 12345, df)

        call_args = self.mock_s3.upload_fileobj.call_args
        self.assertEqual(call_args[1]["Key"], "12345")

    # --- upload_df_as_csv_if_changed ---

    def test_upload_df_as_csv_if_changed_skips_matching_etag(self):
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        etag = hashlib.md5(s3_module.df_to_csv_bytes(df)).hexdigest()

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            uploaded = s3_module.upload_df_as_csv_if_changed("my-bucket", "my/key.csv", df, {"my/key.csv": etag})

        self.assertFalse(uploaded)
        self.mock_s3.put_object.assert_not_called()
        self.mock_s3.upload_fileobj.assert_not_called()

    def test_upload_df_as_csv_if_changed_puts_changed_or_new_files(self):
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})

        for remote_etags in ({"my/key.csv": "0" * 32}, {}):
            self.mock_s3.reset_mock()
            with mock.patch("chalicelib.s3.s3", self.mock_s3):
                uploaded = s3_module.upload_df_as_csv_if_changed("my-bucket", "my/key.csv", df, remote_etags)

            self.assertTrue(uploaded)
            # A single put_object, so the new object's ETag is the MD5 of these bytes
            self.mock_s3.put_object.assert_called_once_with(
                Bucket="my-bucket", Key="my/key.csv", Body=s3_module.df_to_csv_bytes(df), ContentType="text/csv"
            )

    def test_upload_df_as_csv_if_changed_heads_the_key_without_remote_etags(self):
        df = pd.DataFrame({"col_a": [1, 2]})
        etag = hashlib.md5(s3_module.df_to_csv_bytes(df)).hexdigest()
        self.mock_s3.head_object.return_value = {"ETag": f'"{etag}"'}

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            uploaded = s3_module.upload_df_as_csv_if_changed("my-bucket", "my/key.csv", df)

        self.assertFalse(uploaded)
        self.mock_s3.head_object.assert_called_once_with(Bucket="my-bucket", Key="my/key.csv")
        self.mock_s3.put_object.assert_not_called()

    def test_upload_df_as_csv_if_changed_never_heads_with_remote_etags(self):
        df = pd.DataFrame({"col_a": [1, 2]})

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            uploaded = s3_module.upload_df_as_csv_if_changed("my-bucket", "my/key.csv", df, {})

        self.assertTrue(uploaded)
        self.mock_s3.head_object.assert_not_called()
        self.mock_s3.put_object.assert_called_once()

    def test_head_etag_treats_403_and_404_as_missing(self):
        for code in ("403", "404"):
            self.mock_s3.head_object.side_effect = botocore.exceptions.ClientError(
                {"Error": {"Code": code}}, "HeadObject"
            )
            with mock.patch("chalicelib.s3.s3", self.mock_s3):
                self.assertIsNone(s3_module.head_etag("my-bucket", "missing"))

    def test_head_etag_raises_other_errors(self):
        self.mock_s3.head_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "SlowDown"}}, "HeadObject"
        )
        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            with self.assertRaises(botocore.exceptions.ClientError):
                s3_module.head_etag("my-bucket", "k")

    def test_upload_df_as_csv_and_if_changed_send_the_same_bytes(self):
        df = pd.DataFrame({"col_a": [1.5, None], "col_b": ["x", "y"]})

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            s3_module.upload_df_as_csv("my-bucket", "k", df)
            s3_module.upload_df_as_csv_if_changed("my-bucket", "k", df, {})

        fileobj_bytes = self.mock_s3.upload_fileobj.call_args[0][0].getvalue()
        self.assertEqual(fileobj_bytes, self.mock_s3.put_object.call_args[1]["Body"])

    # --- download_csv_as_df ---

    def test_download_csv_as_df(self):
        csv_content = "col_a,col_b\n1,x\n2,y\n"
        mock_body = io.BytesIO(csv_content.encode("utf-8"))
        self.mock_s3.get_object.return_value = {"Body": mock_body}

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            result = s3_module.download_csv_as_df("my-bucket", "my/key.csv")

        self.assertIsInstance(result, pd.DataFrame)
        self.assertListEqual(list(result.columns), ["col_a", "col_b"])
        self.assertEqual(len(result), 2)
        self.mock_s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="my/key.csv")

    # --- ls ---

    def test_ls_single_page(self):
        mock_page = {"Contents": [{"Key": "prefix/file1.csv"}, {"Key": "prefix/file2.csv"}]}
        mock_paginator = mock.Mock()
        mock_paginator.paginate.return_value = [mock_page]
        self.mock_s3.get_paginator.return_value = mock_paginator

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            result = s3_module.ls("my-bucket", "prefix/")

        self.assertEqual(result, ["prefix/file1.csv", "prefix/file2.csv"])
        mock_paginator.paginate.assert_called_once_with(Bucket="my-bucket", Prefix="prefix/")

    def test_ls_paginated(self):
        mock_pages = [
            {"Contents": [{"Key": "prefix/file1.csv"}, {"Key": "prefix/file2.csv"}]},
            {"Contents": [{"Key": "prefix/file3.csv"}]},
        ]
        mock_paginator = mock.Mock()
        mock_paginator.paginate.return_value = mock_pages
        self.mock_s3.get_paginator.return_value = mock_paginator

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            result = s3_module.ls("my-bucket", "prefix/")

        self.assertEqual(result, ["prefix/file1.csv", "prefix/file2.csv", "prefix/file3.csv"])

    # --- upload_file ---

    def test_upload_file_puts_bytes_unmodified(self):
        with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as f:
            f.write(b"\x1f\x8bgzipped")
            path = f.name

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            s3_module.upload_file("my-bucket", "Events/k/events.csv.gz", path)

        self.mock_s3.put_object.assert_called_once_with(
            Bucket="my-bucket", Key="Events/k/events.csv.gz", Body=b"\x1f\x8bgzipped", ContentType="text/csv"
        )
        os.remove(path)

    # --- ls_etags ---

    def test_ls_etags_strips_quotes_and_handles_empty_pages(self):
        mock_paginator = mock.Mock()
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "p/a", "ETag": '"abc"'}]},
            {},
            {"Contents": [{"Key": "p/b", "ETag": '"def"'}]},
        ]
        self.mock_s3.get_paginator.return_value = mock_paginator

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            result = s3_module.ls_etags("my-bucket", "p/")

        self.assertEqual(result, {"p/a": "abc", "p/b": "def"})

    def test_ls_etags_parallel_merges_each_sub_prefix(self):
        listings = {"p/a/": {"p/a/1": "e1"}, "p/b/": {"p/b/1": "e2", "p/b/2": "e3"}}

        with (
            mock.patch("chalicelib.s3.ls_prefixes", return_value=["p/a/", "p/b/"]) as mock_prefixes,
            mock.patch("chalicelib.s3.ls_etags", side_effect=lambda bucket, prefix: listings[prefix]),
        ):
            result = s3_module.ls_etags_parallel("my-bucket", "p/", workers=2)

        mock_prefixes.assert_called_once_with("my-bucket", "p/")
        self.assertEqual(result, {"p/a/1": "e1", "p/b/1": "e2", "p/b/2": "e3"})

    # --- ls_prefixes ---

    def test_ls_prefixes(self):
        mock_paginator = mock.Mock()
        mock_paginator.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": "s/Year=2025/"}, {"Prefix": "s/Year=2026/"}]}
        ]
        self.mock_s3.get_paginator.return_value = mock_paginator

        with mock.patch("chalicelib.s3.s3", self.mock_s3):
            result = s3_module.ls_prefixes("my-bucket", "s/")

        mock_paginator.paginate.assert_called_once_with(Bucket="my-bucket", Prefix="s/", Delimiter="/")
        self.assertEqual(result, ["s/Year=2025/", "s/Year=2026/"])

    # --- clear_cf_cache ---

    def test_clear_cf_cache(self):
        distribution = "EXDIST123"
        keys = ["/path/to/file1", "/path/to/file2"]

        with mock.patch("chalicelib.s3.cloudfront", self.mock_cloudfront):
            s3_module.clear_cf_cache(distribution, keys)

        self.mock_cloudfront.create_invalidation.assert_called_once()
        call_kwargs = self.mock_cloudfront.create_invalidation.call_args[1]
        self.assertEqual(call_kwargs["DistributionId"], distribution)
        paths = call_kwargs["InvalidationBatch"]["Paths"]
        self.assertEqual(paths["Quantity"], 2)
        self.assertEqual(paths["Items"], keys)

    def test_clear_cf_cache_single_key(self):
        keys = ["/only/one"]

        with mock.patch("chalicelib.s3.cloudfront", self.mock_cloudfront):
            s3_module.clear_cf_cache("DIST", keys)

        call_kwargs = self.mock_cloudfront.create_invalidation.call_args[1]
        self.assertEqual(call_kwargs["InvalidationBatch"]["Paths"]["Quantity"], 1)
