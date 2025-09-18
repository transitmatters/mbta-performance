from ..process import process_bus_events

from ..download import download_all_bus_data
import pathlib


def backfill_bus_data(years: list = None, routes: list = None, output_dir: str = "data/output", nozip: bool = False):
    """
    Process all bus data files for specified years and routes.
    This replaces the bash script functionality.

    Args:
        years: List of years to process (default: 2018-2025)
        routes: List of route IDs to process (default: common bus routes)
        output_dir: Output directory for processed data
        nozip: Whether to skip gzipping files
    """
    if years is None:
        years = list(range(2018, 2026))  # 2018-2025

    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    for year in years:
        year_dir = f"data/input/bus/{year}"
        if not pathlib.Path(year_dir).exists():
            print(f"Warning: No data found for year {year} in {year_dir}")
            continue

        print(f"Processing bus data for year {year}...")

        # Find all CSV files in the year directory
        csv_files = list(pathlib.Path(year_dir).glob("*.csv"))

        if not csv_files:
            print(f"Warning: No CSV files found in {year_dir}")
            continue

        for csv_file in csv_files:
            print(f"  Processing {csv_file.name}...")
            try:
                process_bus_events(str(csv_file), output_dir, routes, nozip)
            except Exception as e:
                print(f"    Error processing {csv_file.name}: {e}")
                continue

    print("Bus data processing completed!")


if __name__ == "__main__":
    download_all_bus_data()
    backfill_bus_data()
