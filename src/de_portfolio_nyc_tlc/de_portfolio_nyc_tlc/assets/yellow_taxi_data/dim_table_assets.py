from dagster import MaterializeResult, asset
from dagster_duckdb import DuckDBResource

from .table_assets import table_YT_trip_records_2022
from .constants import TABLE_YELLOW_TAXI_TRIPS


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing the date and time of the taxi trip
    """,
)
def dim_trip_datetime(duckdb: DuckDBResource) -> MaterializeResult:

    with duckdb.get_connection() as conn:
        tbl_name = TABLE_YELLOW_TAXI_TRIPS
        query = """
        SELECT
            pickup_dtime,
            dropoff_dtime
        FROM {tbl_name}""".format(
            tbl_name=tbl_name
        )
        conn.sql(query).show()

    return MaterializeResult(metadata={})


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing the details about the pickup and dropoff location of the taxi trip
    """,
)
def dim_trip_location(duckdb: DuckDBResource) -> MaterializeResult:

    with duckdb.get_connection() as conn:
        tbl_name = TABLE_YELLOW_TAXI_TRIPS
        query = """
        SELECT
            pickup_lid,
            dropoff_lid,
        FROM {tbl_name}""".format(
            tbl_name=tbl_name
        )
        conn.sql(query).show()

    return MaterializeResult(metadata={})


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing the details of the taxi trip payment transaction
    """,
)
def dim_transaction_fees(duckdb: DuckDBResource) -> MaterializeResult:

    with duckdb.get_connection() as conn:
        tbl_name = TABLE_YELLOW_TAXI_TRIPS
        query = """
        SELECT
            rate_code_id,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee
        FROM {tbl_name}""".format(
            tbl_name=tbl_name
        )
        conn.sql(query).show()

    return MaterializeResult(metadata={})


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing miscelleneous details about the trip
    """,
)
def dim_taxi_misc_details(duckdb: DuckDBResource) -> MaterializeResult:

    with duckdb.get_connection() as conn:
        tbl_name = TABLE_YELLOW_TAXI_TRIPS
        query = """
        SELECT
            vendor_id,
            store_and_fwd_flag,
        FROM {tbl_name}""".format(
            tbl_name=tbl_name
        )
        conn.sql(query).show()

    return MaterializeResult(metadata={})
