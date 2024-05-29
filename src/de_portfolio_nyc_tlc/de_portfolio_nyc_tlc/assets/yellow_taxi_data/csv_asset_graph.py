from dagster import Output, graph_asset

from ...partitions import monthly_partition

from .ops import csv_assets_ops as ops

from pathlib import Path


@graph_asset(partitions_def=monthly_partition)
def YT_monthly_2022():
    start_date, end_date = ops.get_monthly_range()

    print(f"value of {start_date=} {end_date=}")
    # print(f"type of {type(date_range)}")
    # month_num: str = start_date.split("-")[1]

    f = ops.generate_file_name(start_date)

    return f
