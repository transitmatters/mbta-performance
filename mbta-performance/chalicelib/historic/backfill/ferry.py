from ..process import process_ferry
from ..arcgis import download_latest_ferry_data, ferry_update_cache


def backfill_ferry_data():
    print("Processing Ferry Data")
    ferry_update_cache()
    # download the data
    csv_file = download_latest_ferry_data()

    process_ferry(csv_file, "data/output")
    print("Finished Processing Ferry Data")


if __name__ == "__main__":
    backfill_ferry_data()
