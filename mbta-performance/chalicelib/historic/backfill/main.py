from ..constants import ARCGIS_IDS, HISTORIC_COLUMNS_LAMP, HISTORIC_COLUMNS_PRE_LAMP
from ..download import download_historic_data, list_files_in_dir, prep_local_dir, unzip_historic_data
from ..process import process_events


def backfill_single_year(year: str):
    print(f"Backfilling year {year}")
    # download the data
    zip_file = download_historic_data(year)
    # unzip the data
    input_dir = unzip_historic_data(zip_file, f"data/input/{year}")
    # process the data
    for file in list_files_in_dir(input_dir):
        # in 2024 data moved to LAMP and the format changed
        if int(year) >= 2024:
            process_events(file, "data/output", columns=HISTORIC_COLUMNS_LAMP)
        else:
            process_events(file, "data/output", columns=HISTORIC_COLUMNS_PRE_LAMP)
    print(f"Finished backfilling year {year}")


def backfill_all_years():
    """Backfill all years of MBTA data we can"""

    prep_local_dir()

    for year in reversed(ARCGIS_IDS.keys()):
        backfill_single_year(year)


if __name__ == "__main__":
    backfill_all_years()
