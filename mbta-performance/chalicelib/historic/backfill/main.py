import threading
from ..constants import ARCGIS_IDS
from ..download import download_historic_data, list_files_in_dir, prep_local_dir, unzip_historic_data
from ..process import process_events


def single_year_thread(year: str):
    print(f"Backfilling year {year}")
    # download the data
    zip_file = download_historic_data(year)
    # unzip the data
    input_dir = unzip_historic_data(zip_file, f"data/input/{year}")
    # process the data
    for file in list_files_in_dir(input_dir):
        process_events(file, "data/output")
    print(f"Finished backfilling year {year}")


def backfill_all_years():
    """Backfill all years of MBTA data we can"""

    prep_local_dir()

    year_threads: list[threading.Thread] = []
    for year in ARCGIS_IDS.keys():
        year_thread = threading.Thread(
            target=single_year_thread,
            args=(year,),
            name=year,
        )
        year_threads.append(year_thread)
        year_thread.start()

    for year_thread in year_threads:
        year_thread.join()


if __name__ == "__main__":
    backfill_all_years()
