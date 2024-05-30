# fmt: off
from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition

YT_monthly_parquet_2022 = AssetSelection.assets("YT_monthly_parquet_2022")

YT_monthly_csv_2022 = AssetSelection.assets("YT_monthly_csv_2022")


fetch_YT_csv_2022_job = define_asset_job(
    name="fetch_YT_csv_2022_job",
    partitions_def=monthly_partition,
    selection=YT_monthly_csv_2022,
    tags={
        "partitioning_limit": "medium",
        }
)


convert_to_parquet_YT_2022_job = define_asset_job(
    name="convert_to_parquet_YT_2022_job",
    partitions_def=monthly_partition,
    selection=YT_monthly_parquet_2022,
    tags={
        "partitioning_limit": "high"
    },
)
