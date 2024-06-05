from pandas import DataFrame
from . import AssetCheckSpec, CheckSpec


asset = "table_YT_trip_records_2022"


# asset check conditions
def table_is_not_empty(count_trip_records: int):
    return count_trip_records > 0


check_spec_list: list[CheckSpec] = []

check_spec_list.append(
    CheckSpec(
        AssetCheckSpec(
            name="table_is_not_empty",
            description="Verify that the created table has contents",
            asset=asset,
        ),
        condition=table_is_not_empty,
    )
)
