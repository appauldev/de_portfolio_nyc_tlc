import os
from dagster import op, get_dagster_logger
import pandas as pd


@op
def get_list_of_csv() -> list[str]:
    log = get_dagster_logger()
    CSV_FOLDER = os.path.join(os.path.dirname(__file__), "data/csv")
    log.info(CSV_FOLDER)
    csv_files = [
        file.path
        for file in os.scandir(CSV_FOLDER)
        if file.is_file() and file.name.endswith(".csv")
    ]

    log.info(csv_files)
    for csv in csv_files:
        log.info(f"File name: {csv}")

    return csv_files


@op
def verify_row_count(csv_files: list[str]):
    log = get_dagster_logger()
    total_count = 0
    VERIFIED_COUNT = 39_656_098
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        total_count += df.shape[0]
    log.info(f"Total rows of downloaded csv: {total_count}")
    assert total_count == VERIFIED_COUNT
