from dataclasses import dataclass
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
