from duckdb import DuckDBPyConnection
from pandas import DataFrame
from . import AssetCheckSpec, CheckSpec
from ..constants import table_names

asset = "table_YT_trip_records_2022"


# asset check conditions
def table_is_not_empty(conn: DuckDBPyConnection):
    result = conn.sql(
        f"""--sql
        SELECT COUNT(*) as count_total_records
        FROM {table_names.YELLOW_TAXI_TRIPS}
        """
    ).df()

    return bool(result.at[0, "count_total_records"] > 0)


check_spec_list: list[CheckSpec] = []

check_spec_list.append(
    CheckSpec(
        AssetCheckSpec=AssetCheckSpec(
            name="table_is_not_empty",
            description="Verify that the created table has contents",
            asset=asset,
        ),
        condition=table_is_not_empty,
    )
)
