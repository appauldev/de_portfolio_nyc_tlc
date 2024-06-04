from dagster import Definitions, load_assets_from_modules, multiprocess_executor

from .assets.yellow_taxi_data import csv_assets, parquet_assets, table_assets

from .jobs import (
    convert_to_parquet_YT_2022_job,
    fetch_YT_csv_2022_job,
)

yellow_taxi_assets = load_assets_from_modules(
    [csv_assets, parquet_assets, table_assets], group_name="YELLOW_TAXI_YT"
)
all_jobs = [
    convert_to_parquet_YT_2022_job,
    fetch_YT_csv_2022_job,
]


defs = Definitions(
    assets=[*yellow_taxi_assets],
    resources={},
    jobs=all_jobs,
    executor=multiprocess_executor,
)
