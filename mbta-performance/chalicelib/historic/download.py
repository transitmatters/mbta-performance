import pathlib
import requests
import os
from zipfile import ZipFile
import subprocess
import shutil
from .constants import ARCGIS_IDS, BUS_ARCGIS_IDS


def prep_local_dir():
    pathlib.Path("data").mkdir(exist_ok=True)
    pathlib.Path("data/input").mkdir(exist_ok=True)
    pathlib.Path("data/input/bus").mkdir(exist_ok=True)
    pathlib.Path("data/output").mkdir(exist_ok=True)


def download_historic_data(year: str):
    if year not in ARCGIS_IDS.keys():
        raise ValueError(f"Year {year} dataset is not available. Supported years are {list(ARCGIS_IDS.keys())}")

    url = f"https://www.arcgis.com/sharing/rest/content/items/{ARCGIS_IDS[year]}/data"
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch historic data from {url}. Status code: {response.status_code}")

    with open(f"data/input/{year}.zip", "wb") as f:
        f.write(response.content)
    return os.path.abspath(f"data/input/{year}.zip")


def unzip_historic_data(zip_file: str, output_dir: str):
    pathlib.Path(output_dir).mkdir(exist_ok=True)

    try:
        with ZipFile(zip_file, "r") as zip_ref:
            # Extract all the contents of zip file in different directory
            zip_ref.extractall(output_dir)
    except NotImplementedError:
        print("Zip file extraction failed. Likely due to unsupported compression method.")
        print("Attempting to extract using unzip")
        subprocess.Popen(["unzip", "-o", "-d", output_dir, zip_file])

    return output_dir


def list_files_in_dir(dir: str):
    csv_files = []
    files = os.listdir(dir)
    for file in files:
        if os.path.isfile(os.path.join(dir, file)):
            csv_files.append(os.path.join(dir, file))
        elif os.path.isdir(os.path.join(dir, file)):
            csv_files += list_files_in_dir(os.path.join(dir, file))
    return csv_files


def download_bus_data(year: str):
    """Download bus data for a specific year from ArcGIS."""
    if year not in BUS_ARCGIS_IDS.keys():
        raise ValueError(f"Year {year} bus dataset is not available. Supported years are {list(BUS_ARCGIS_IDS.keys())}")

    url = f"https://www.arcgis.com/sharing/rest/content/items/{BUS_ARCGIS_IDS[year]}/data"
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch bus data from {url}. Status code: {response.status_code}")

    with open(f"data/input/bus/{year}.zip", "wb") as f:
        f.write(response.content)
    return os.path.abspath(f"data/input/bus/{year}.zip")


def unzip_bus_data(zip_file: str, output_dir: str):
    """Extract bus data for a specific year."""
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    try:
        with ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(output_dir)
    except NotImplementedError:
        print("Bus zip file extraction failed. Likely due to unsupported compression method.")
        print("Attempting to extract using unzip")
        subprocess.run(["unzip", "-o", "-d", output_dir, zip_file])

    return output_dir


def process_bus_file_names(year: str, year_dir: str):
    """Rename bus files to have consistent names and remove spaces."""
    year_path = pathlib.Path(year_dir)

    # Handle years 2021+ that have files in subdirectories
    if int(year) >= 2021:
        # Look for the MBTA subdirectory pattern
        mbta_pattern = f"MBTA_Bus_Arrival_Departure_Times_{year}"
        mbta_dir = year_path / mbta_pattern

        if mbta_dir.exists():
            print(f"Moving files from {mbta_pattern} subdirectory...")
            for file in mbta_dir.glob("*.csv"):
                shutil.move(str(file), str(year_path / file.name))
            mbta_dir.rmdir()
            print(f"Moved files from {mbta_pattern} to main directory")

    elif year == "2020":
        # Rename quarterly files
        rename_mapping = {
            "Bus Arrival Departure Times Jan-Mar 2020.csv": "2020-Q1.csv",
            "Bus Arrival Departure Times Apr-June 2020.csv": "2020-Q2.csv",
            "Bus Arrival Departure Times Jul-Sep 2020.csv": "2020-Q3.csv",
            "Bus Arrival Departure Times Oct-Dec 2020.csv": "2020-Q4.csv",
        }

        for old_name, new_name in rename_mapping.items():
            old_path = year_path / old_name
            new_path = year_path / new_name
            if old_path.exists():
                shutil.move(str(old_path), str(new_path))

    elif year == "2019":
        # Rename quarterly files in subdirectory
        mbta_dir = year_path / "MBTA Bus Arrival Departure Times 2019"
        if mbta_dir.exists():
            rename_mapping = {
                "MBTA Bus Arrival Departure Jan-Mar 2019.csv": "2019-Q1.csv",
                "MBTA Bus Arrival Departure Apr-June 2019.csv": "2019-Q2.csv",
                "MBTA Bus Arrival Departure Jul-Sept 2019.csv": "2019-Q3.csv",
                "MBTA Bus Arrival Departure Oct-Dec 2019.csv": "2019-Q4.csv",
            }

            for old_name, new_name in rename_mapping.items():
                old_path = mbta_dir / old_name
                new_path = year_path / new_name
                if old_path.exists():
                    shutil.move(str(old_path), str(new_path))

            # Remove empty subdirectory
            if not any(mbta_dir.iterdir()):
                mbta_dir.rmdir()

    elif year == "2018":
        # Rename quarterly files in subdirectory
        mbta_dir = year_path / "MBTA Bus Arrival Departure Times 2018"
        if mbta_dir.exists():
            rename_mapping = {
                "MBTA Bus Arrival Departure Aug-Sept 2018.csv": "2018-Q3.csv",
                "MBTA Bus Arrival Departure Oct-Dec 2018.csv": "2018-Q4.csv",
            }

            for old_name, new_name in rename_mapping.items():
                old_path = mbta_dir / old_name
                new_path = year_path / new_name
                if old_path.exists():
                    shutil.move(str(old_path), str(new_path))

            # Remove empty subdirectory
            if not any(mbta_dir.iterdir()):
                mbta_dir.rmdir()


def clean_unicode_bom(file_path: str):
    """Remove Unicode BOM from a file."""
    with open(file_path, "rb") as f:
        content = f.read()

    # Remove BOM if present
    if content.startswith(b"\xef\xbb\xbf"):
        content = content[3:]

    with open(file_path, "wb") as f:
        f.write(content)


def download_all_bus_data():
    """Download all bus data files (2018-2025)."""
    prep_local_dir()

    # Download bus data for each year
    for year in BUS_ARCGIS_IDS.keys():
        print(f"Downloading bus data for {year}...")
        zip_file = download_bus_data(year)
        year_dir = f"data/input/bus/{year}"
        unzip_bus_data(zip_file, year_dir)
        process_bus_file_names(year, year_dir)

        # Clean Unicode BOM from 2020-Q3.csv if it exists
        q3_file = pathlib.Path(year_dir) / "2020-Q3.csv"
        if q3_file.exists():
            clean_unicode_bom(str(q3_file))

    print("All bus data downloaded and processed successfully!")
