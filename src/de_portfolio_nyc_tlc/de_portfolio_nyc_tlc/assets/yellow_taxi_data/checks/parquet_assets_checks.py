from dataclasses import dataclass
from typing import Callable
from dagster import AssetCheckSpec
from pandas import DataFrame

asset = "YT_monthly_parquet_2022"


@dataclass
class CheckConditions:
    df: DataFrame

    def trip_distances_are_positive(self):
        return bool((self.df["passenger_count"] > 0).all())

    def only_paid_trips(self):
        return bool((self.df["total_amount"] > 0).all())


asset_check_list: list[AssetCheckSpec] = [
    AssetCheckSpec(
        name="trip_distances_are_positive",
        description="Only accept taxi trip records that have a reported movement > 0 miles",
        asset=asset,
    ),
    AssetCheckSpec(
        name="only_paid_trips",
        description="Only accept taxi trip records that are paid",
        asset=asset,
    ),
]

# fmt: off
def create_check_spec(
    name: str, 
    description: str,
    asset: str,
    condition: Callable[[DataFrame], bool],
    **asset_check_spec_args,
):

    return {
        "check_spec": AssetCheckSpec(
            name=name,
            description=description,
            asset=asset,
            **asset_check_spec_args),
        "condition": condition,
    }


def trip_distances_are_positive(df: DataFrame):
    return bool((df["passenger_count"] > 0).all())


def only_paid_trips(df: DataFrame):
    return bool((df["total_amount"] > 0).all())


check_specs = [
    create_check_spec(
        name="trip_distances_are_positive",
        description="Only accept taxi trip records that have a reported movement > 0 miles",
        asset=asset,
        condition=trip_distances_are_positive,
    ),
    create_check_spec(
        name="only_paid_trips",
        condition=only_paid_trips,
        asset=asset,
        description="Only accept taxi trip records that are paid",
    ),
]
