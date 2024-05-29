from dagster import Definitions, load_assets_from_modules, multiprocess_executor

from .assets.yellow_taxi_data import csv_assets, parquet_assets, csv_asset_graph

from .jobs import (
    fetch_yellow_taxi_csv_2022_job,
    convert_to_parquet_yellow_taxi_2022_job,
)

yellow_taxi_assets = load_assets_from_modules(
    [csv_assets, parquet_assets, csv_asset_graph], group_name="YELLOW_TAXI"
)
all_jobs = [
    fetch_yellow_taxi_csv_2022_job,
    convert_to_parquet_yellow_taxi_2022_job,
]


defs = Definitions(
    assets=[*yellow_taxi_assets],
    resources={},
    jobs=all_jobs,
    executor=multiprocess_executor,
)
