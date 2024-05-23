# fmt: off
from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition

yellow_taxi_monthly_2022 = AssetSelection.keys("yellow_taxi_monthly_csv_2022")

yellow_taxi_monthly_2022_parquet = AssetSelection.keys("yellow_taxi_monthly_parquet_2022")

fetch_yellow_taxi_csv_2022_job = define_asset_job(
    name="fetch_yellow_taxi_csv_2022_job",
    partitions_def=monthly_partition,
    selection=yellow_taxi_monthly_2022,
    tags={
        "partitioning_limit": "medium",
        }
)

convert_to_parquet_yellow_taxi_2022_job = define_asset_job(
    name="convert_to_parquet_yellow_taxi_2022_job",
    partitions_def=monthly_partition,
    selection=yellow_taxi_monthly_2022_parquet,
    tags={
        "partitioning_limit": "medium"
    },
)
