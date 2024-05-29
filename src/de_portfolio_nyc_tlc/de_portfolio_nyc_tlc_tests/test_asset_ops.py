from pathlib import Path
from ..de_portfolio_nyc_tlc.assets.yellow_taxi_data.ops import csv_assets_ops as ops


def test_op_generate_file_name():
    start_date = "2022-01-01"
    CSV_DATA_TEST_FOLDER = Path(f"{Path.cwd()}/assets/data/csv/test")
    value = ops.generate_file_name(start_date, CSV_DATA_TEST_FOLDER)

    assert str(value) != ""
    assert str(value).endswith(f"2022-{start_date.split("-")[1]}.csv")
