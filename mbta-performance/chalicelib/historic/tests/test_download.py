import os
import pathlib
import tempfile
import unittest
from unittest import mock

from .. import download
from ..constants import ARCGIS_IDS


class TestDownload(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Clean up temp directory
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_prep_local_dir(self):
        """Test that prep_local_dir creates necessary directories."""
        with mock.patch("pathlib.Path.mkdir") as mock_mkdir:
            download.prep_local_dir()

            # Verify that mkdir was called for each required directory
            self.assertEqual(mock_mkdir.call_count, 4)

    def test_download_historic_data_success(self):
        """Test successful download of historic data."""
        year = "2024"
        mock_content = b"mock zip content"
        mock_response = mock.Mock(status_code=200, content=mock_content)

        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("builtins.open", mock.mock_open()) as mock_file:
                with mock.patch("os.path.abspath", return_value=f"{self.temp_dir}/{year}.zip"):
                    _ = download.download_historic_data(year)

                    # Verify the file was written
                    mock_file.assert_called_once_with(f"data/input/{year}.zip", "wb")
                    mock_file().write.assert_called_once_with(mock_content)

                    # Verify the correct URL was called
                    expected_url = f"https://www.arcgis.com/sharing/rest/content/items/{ARCGIS_IDS[year]}/data"
                    mock.call(expected_url)

    def test_download_historic_data_invalid_year(self):
        """Test that invalid year raises ValueError."""
        with self.assertRaises(ValueError) as context:
            download.download_historic_data("1999")
        self.assertIn("Year 1999 dataset is not available", str(context.exception))

    def test_download_historic_data_failure(self):
        """Test download failure handling."""
        year = "2024"
        mock_response = mock.Mock(status_code=404)

        with mock.patch("requests.get", return_value=mock_response):
            with self.assertRaises(ValueError) as context:
                download.download_historic_data(year)
            self.assertIn("Failed to fetch historic data", str(context.exception))

    def test_unzip_historic_data(self):
        """Test unzipping of historic data."""
        zip_file = f"{self.temp_dir}/test.zip"
        output_dir = f"{self.temp_dir}/output"

        mock_zip = mock.MagicMock()

        mock_zip_context = mock.MagicMock()
        mock_zip_context.__enter__ = mock.Mock(return_value=mock_zip)
        mock_zip_context.__exit__ = mock.Mock(return_value=False)

        with mock.patch("pathlib.Path.mkdir"):
            with mock.patch("chalicelib.historic.download.ZipFile", return_value=mock_zip_context):
                result = download.unzip_historic_data(zip_file, output_dir)

                # Verify extractall was called
                mock_zip.extractall.assert_called_once_with(output_dir)
                self.assertEqual(result, output_dir)

    def test_unzip_historic_data_with_unsupported_compression(self):
        """Test unzip fallback to system unzip command."""
        zip_file = f"{self.temp_dir}/test.zip"
        output_dir = f"{self.temp_dir}/output"

        mock_zip = mock.MagicMock()
        mock_zip.extractall.side_effect = NotImplementedError("Unsupported compression")
        mock_zip_context = mock.MagicMock()
        mock_zip_context.__enter__ = mock.Mock(return_value=mock_zip)
        mock_zip_context.__exit__ = mock.Mock(return_value=False)

        with mock.patch("pathlib.Path.mkdir"):
            with mock.patch("chalicelib.historic.download.ZipFile", return_value=mock_zip_context):
                with mock.patch("subprocess.Popen") as mock_popen:
                    _ = download.unzip_historic_data(zip_file, output_dir)

                    # Verify subprocess was called with unzip
                    mock_popen.assert_called_once()
                    call_args = mock_popen.call_args[0][0]
                    self.assertEqual(call_args[0], "unzip")

    def test_list_files_in_dir(self):
        """Test listing files in directory recursively."""
        # Create a temporary directory structure
        test_dir = pathlib.Path(self.temp_dir) / "test"
        test_dir.mkdir()
        (test_dir / "file1.csv").touch()
        (test_dir / "file2.csv").touch()
        subdir = test_dir / "subdir"
        subdir.mkdir()
        (subdir / "file3.csv").touch()

        result = download.list_files_in_dir(str(test_dir))

        # Verify all files were found
        self.assertEqual(len(result), 3)
        self.assertTrue(any("file1.csv" in f for f in result))
        self.assertTrue(any("file2.csv" in f for f in result))
        self.assertTrue(any("file3.csv" in f for f in result))

    def test_download_bus_data_success(self):
        """Test successful download of bus data."""
        year = "2024"
        mock_content = b"mock bus zip content"
        mock_response = mock.Mock(status_code=200, content=mock_content)

        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("builtins.open", mock.mock_open()) as mock_file:
                with mock.patch("os.path.abspath", return_value=f"{self.temp_dir}/{year}.zip"):
                    _ = download.download_bus_data(year)

                    # Verify the file was written
                    mock_file.assert_called_once_with(f"data/input/bus/{year}.zip", "wb")
                    mock_file().write.assert_called_once_with(mock_content)

    def test_download_bus_data_invalid_year(self):
        """Test that invalid year raises ValueError for bus data."""
        with self.assertRaises(ValueError) as context:
            download.download_bus_data("2000")
        self.assertIn("Year 2000 bus dataset is not available", str(context.exception))

    def test_download_bus_data_failure(self):
        """Test bus data download failure handling."""
        year = "2024"
        mock_response = mock.Mock(status_code=500)

        with mock.patch("requests.get", return_value=mock_response):
            with self.assertRaises(ValueError) as context:
                download.download_bus_data(year)
            self.assertIn("Failed to fetch bus data", str(context.exception))

    def test_unzip_bus_data(self):
        """Test unzipping of bus data."""
        zip_file = f"{self.temp_dir}/test.zip"
        output_dir = f"{self.temp_dir}/output"

        mock_zip = mock.MagicMock()
        mock_zip_context = mock.MagicMock()
        mock_zip_context.__enter__ = mock.Mock(return_value=mock_zip)
        mock_zip_context.__exit__ = mock.Mock(return_value=False)

        with mock.patch("pathlib.Path.mkdir"):
            with mock.patch("chalicelib.historic.download.ZipFile", return_value=mock_zip_context):
                _ = download.unzip_bus_data(zip_file, output_dir)

                # Verify extractall was called
                mock_zip.extractall.assert_called_once_with(output_dir)

    def test_process_bus_file_names_2021_plus(self):
        """Test processing of bus file names for years 2021 and later."""
        year = "2021"
        year_dir = pathlib.Path(self.temp_dir) / year
        year_dir.mkdir()

        # Create mock subdirectory structure
        mbta_dir = year_dir / f"MBTA_Bus_Arrival_Departure_Times_{year}"
        mbta_dir.mkdir()
        test_file = mbta_dir / "test.csv"
        test_file.touch()

        download.process_bus_file_names(year, str(year_dir))

        # Verify file was moved
        self.assertTrue((year_dir / "test.csv").exists())
        self.assertFalse(mbta_dir.exists())

    def test_process_bus_file_names_2020(self):
        """Test processing of bus file names for 2020."""
        year = "2020"
        year_dir = pathlib.Path(self.temp_dir) / year
        year_dir.mkdir()

        # Create files with old names
        old_files = {
            "Bus Arrival Departure Times Jan-Mar 2020.csv": "2020-Q1.csv",
            "Bus Arrival Departure Times Apr-June 2020.csv": "2020-Q2.csv",
        }

        for old_name in old_files:
            (year_dir / old_name).touch()

        download.process_bus_file_names(year, str(year_dir))

        # Verify files were renamed
        for new_name in old_files.values():
            self.assertTrue((year_dir / new_name).exists())

    def test_process_bus_file_names_2019(self):
        """Test processing of bus file names for 2019."""
        year = "2019"
        year_dir = pathlib.Path(self.temp_dir) / year
        year_dir.mkdir()

        # Create subdirectory and files
        mbta_dir = year_dir / "MBTA Bus Arrival Departure Times 2019"
        mbta_dir.mkdir()
        old_file = mbta_dir / "MBTA Bus Arrival Departure Jan-Mar 2019.csv"
        old_file.touch()

        download.process_bus_file_names(year, str(year_dir))

        # Verify file was renamed and moved
        self.assertTrue((year_dir / "2019-Q1.csv").exists())

    def test_process_bus_file_names_2018(self):
        """Test processing of bus file names for 2018."""
        year = "2018"
        year_dir = pathlib.Path(self.temp_dir) / year
        year_dir.mkdir()

        # Create subdirectory and files
        mbta_dir = year_dir / "MBTA Bus Arrival Departure Times 2018"
        mbta_dir.mkdir()
        old_file = mbta_dir / "MBTA Bus Arrival Departure Aug-Sept 2018.csv"
        old_file.touch()

        download.process_bus_file_names(year, str(year_dir))

        # Verify file was renamed and moved
        self.assertTrue((year_dir / "2018-Q3.csv").exists())

    def test_clean_unicode_bom(self):
        """Test removal of Unicode BOM from file."""
        test_file = pathlib.Path(self.temp_dir) / "test.csv"

        # Write file with BOM
        with open(test_file, "wb") as f:
            f.write(b"\xef\xbb\xbftest content")

        download.clean_unicode_bom(str(test_file))

        # Verify BOM was removed
        with open(test_file, "rb") as f:
            content = f.read()
        self.assertEqual(content, b"test content")

    def test_clean_unicode_bom_without_bom(self):
        """Test clean_unicode_bom with file that has no BOM."""
        test_file = pathlib.Path(self.temp_dir) / "test.csv"

        # Write file without BOM
        original_content = b"test content"
        with open(test_file, "wb") as f:
            f.write(original_content)

        download.clean_unicode_bom(str(test_file))

        # Verify content unchanged
        with open(test_file, "rb") as f:
            content = f.read()
        self.assertEqual(content, original_content)

    def test_unzip_bus_data_with_unsupported_compression(self):
        """Test unzip_bus_data fallback to system unzip when NotImplementedError is raised."""
        zip_file = f"{self.temp_dir}/test.zip"
        output_dir = f"{self.temp_dir}/output"

        mock_zip = mock.MagicMock()
        mock_zip.extractall.side_effect = NotImplementedError("Unsupported compression")
        mock_zip_context = mock.MagicMock()
        mock_zip_context.__enter__ = mock.Mock(return_value=mock_zip)
        mock_zip_context.__exit__ = mock.Mock(return_value=False)

        with mock.patch("pathlib.Path.mkdir"):
            with mock.patch("chalicelib.historic.download.ZipFile", return_value=mock_zip_context):
                with mock.patch("subprocess.run") as mock_run:
                    result = download.unzip_bus_data(zip_file, output_dir)

                    mock_run.assert_called_once()
                    call_args = mock_run.call_args[0][0]
                    self.assertEqual(call_args[0], "unzip")
                    self.assertEqual(result, output_dir)

    def test_download_all_bus_data(self):
        """Test download_all_bus_data function."""
        mock_response = mock.Mock(status_code=200, content=b"mock content")

        with mock.patch("requests.get", return_value=mock_response):
            with mock.patch("chalicelib.historic.download.prep_local_dir"):
                with mock.patch("chalicelib.historic.download.download_bus_data", return_value="/fake/path.zip"):
                    with mock.patch("chalicelib.historic.download.unzip_bus_data"):
                        with mock.patch("chalicelib.historic.download.process_bus_file_names"):
                            with mock.patch("pathlib.Path.exists", return_value=False):
                                # Mock the clean_unicode_bom to avoid file operations
                                with mock.patch("chalicelib.historic.download.clean_unicode_bom"):
                                    download.download_all_bus_data()

    def test_download_all_bus_data_cleans_unicode_bom(self):
        """Test download_all_bus_data calls clean_unicode_bom when 2020-Q3.csv exists."""
        with mock.patch("chalicelib.historic.download.prep_local_dir"):
            with mock.patch("chalicelib.historic.download.download_bus_data", return_value="/fake/path.zip"):
                with mock.patch("chalicelib.historic.download.unzip_bus_data"):
                    with mock.patch("chalicelib.historic.download.process_bus_file_names"):
                        with mock.patch("pathlib.Path.exists", return_value=True):
                            with mock.patch("chalicelib.historic.download.clean_unicode_bom") as mock_clean:
                                download.download_all_bus_data()

                                # clean_unicode_bom should have been called at least once
                                mock_clean.assert_called()
