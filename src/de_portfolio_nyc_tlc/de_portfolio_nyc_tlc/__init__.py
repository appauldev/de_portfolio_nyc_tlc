from dagster import Definitions, load_assets_from_modules, multiprocess_executor
from dagster_duckdb import DuckDBResource

from .assets.yellow_taxi_data import (
    csv_assets,
    parquet_assets,
    table_assets,
    dim_table_assets,
)

from .jobs import (
    convert_to_parquet_YT_2022_job,
    fetch_YT_csv_2022_job,
)

yellow_taxi_assets = load_assets_from_modules(
    [csv_assets, parquet_assets, table_assets, dim_table_assets],
    group_name="YELLOW_TAXI_YT",
)
all_jobs = [
    convert_to_parquet_YT_2022_job,
    fetch_YT_csv_2022_job,
]


defs = Definitions(
    assets=[*yellow_taxi_assets],
    resources={
        "duckdb": DuckDBResource(
            database="de_portfolio_nyc_tlc/assets/yellow_taxi_data/models/taxi_trip_records.duckdb"
        )
    },
    jobs=all_jobs,
    executor=multiprocess_executor,
)
