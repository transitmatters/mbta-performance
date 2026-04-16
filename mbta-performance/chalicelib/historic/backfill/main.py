from datetime import date

from ..constants import ARCGIS_IDS, HISTORIC_COLUMNS_LAMP, HISTORIC_COLUMNS_PRE_LAMP
from ..download import download_historic_data, list_files_in_dir, prep_local_dir, unzip_historic_data
from ..process import process_events


def backfill_single_year(year: str, start_date: date = None, end_date: date = None):
    print(f"Backfilling year {year}")
    # download the data
    zip_file = download_historic_data(year)
    # unzip the data
    input_dir = unzip_historic_data(zip_file, f"data/input/{year}")
    # process the data
    for file in list_files_in_dir(input_dir):
        # in 2024 data moved to LAMP and the format changed
        if int(year) >= 2024:
            process_events(
                file, "data/output", columns=HISTORIC_COLUMNS_LAMP, start_date=start_date, end_date=end_date
            )
        else:
            process_events(
                file, "data/output", columns=HISTORIC_COLUMNS_PRE_LAMP, start_date=start_date, end_date=end_date
            )
    print(f"Finished backfilling year {year}")


def backfill_all_years():
    """Backfill all years of MBTA data we can"""

    prep_local_dir()

    for year in reversed(ARCGIS_IDS.keys()):
        backfill_single_year(year)


def backfill_date_range(start_date: date, end_date: date):
    """Backfill only data within [start_date, end_date], spanning whichever years are needed."""
    prep_local_dir()

    for year in range(start_date.year, end_date.year + 1):
        year_str = str(year)
        if year_str not in ARCGIS_IDS:
            print(f"Skipping year {year_str}: no ARCGIS_ID configured")
            continue
        backfill_single_year(year_str, start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    backfill_all_years()
