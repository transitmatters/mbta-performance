import pathlib
import requests
import os
from zipfile import ZipFile
import subprocess
from .constants import ARCGIS_IDS


def prep_local_dir():
    pathlib.Path("data").mkdir(exist_ok=True)
    pathlib.Path("data/input").mkdir(exist_ok=True)
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
