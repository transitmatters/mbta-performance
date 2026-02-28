import io
import unittest
import zlib
from unittest import mock

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
