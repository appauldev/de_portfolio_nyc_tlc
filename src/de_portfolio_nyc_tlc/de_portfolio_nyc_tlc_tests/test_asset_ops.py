from pathlib import Path

from dagster import build_op_context
from ..de_portfolio_nyc_tlc.assets.yellow_taxi_data.helpers import (
    csv_asset_helpers as helper,
)

from pytest import fixture, mark

from pandas import DataFrame


@fixture
def test_folder(tmp_path):
    return Path(f"{tmp_path}/assets/data/csv/")


def test_create_file_save_path(test_folder):
    # arrange
    start_date = "2022-01-01"

    # act
    FILE_SAVE_PATH = helper.create_file_save_path(start_date, test_folder)
    records = [{"key1": 1, "key2": 2}, {"key1": 3, "key2": 4}, {"key1": 5, "key2": 6}]
    df = DataFrame.from_records(records)
    df.to_csv(f"{FILE_SAVE_PATH}", index=False)

    # assert
    assert Path(FILE_SAVE_PATH).is_file() and FILE_SAVE_PATH.endswith(
        f"2022-{start_date.split("-")[1]}.csv"
    )


@mark.parametrize(
    "start_date, expected_range",
    [
        ("2022-01-01", ("2022-01-01", "2022-02-01")),
        ("2022-02-01", ("2022-02-01", "2022-03-01")),
        ("2022-03-01", ("2022-03-01", "2022-04-01")),
        ("2022-04-01", ("2022-04-01", "2022-05-01")),
        ("2022-05-01", ("2022-05-01", "2022-06-01")),
        ("2022-06-01", ("2022-06-01", "2022-07-01")),
        ("2022-07-01", ("2022-07-01", "2022-08-01")),
        ("2022-08-01", ("2022-08-01", "2022-09-01")),
        ("2022-09-01", ("2022-09-01", "2022-10-01")),
        ("2022-10-01", ("2022-10-01", "2022-11-01")),
        ("2022-11-01", ("2022-11-01", "2022-12-01")),
        ("2022-12-01", ("2022-12-01", "2023-01-01")),
    ],
)
def test_get_monthly_range(start_date, expected_range):
    # arrange

    # act
    output_range = helper.get_monthly_range(start_date)

    # assert
    assert output_range == expected_range
