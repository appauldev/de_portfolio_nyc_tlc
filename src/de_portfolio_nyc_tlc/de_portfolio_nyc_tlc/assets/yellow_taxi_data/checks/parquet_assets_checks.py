from dagster import AssetCheckSpec
from pandas import DataFrame
from . import CheckSpec

asset = "YT_monthly_parquet_2022"


# asset check conditions
def trip_distances_are_positive(df: DataFrame):
    return bool((df["passenger_count"] > 0).all())


def only_paid_trips(df: DataFrame):
    return bool((df["total_amount"] > 0).all())


def dataframe_is_not_empty_and_invalid(df: DataFrame):
    return not df.empty and df is not None and isinstance(df, DataFrame)


check_spec_list: list[CheckSpec] = []

check_spec_list.append(
    CheckSpec(
        AssetCheckSpec(
            name="dataframe_is_not_null_or_empty",
            description="Verify that the dataframe has contents",
            asset=asset,
        ),
        condition=dataframe_is_not_empty_and_invalid,
    )
)

check_spec_list.append(
    CheckSpec(
        AssetCheckSpec(
            name="trip_distances_are_positive",
            description="Only accept taxi trip records that have recorded movement > 0 miles",
            asset=asset,
        ),
        condition=trip_distances_are_positive,
    )
)
check_spec_list.append(
    CheckSpec(
        AssetCheckSpec(
            name="only_paid_trips",
            description="Only accept taxi trip records that are paid",
            asset=asset,
        ),
        condition=only_paid_trips,
    )
)
