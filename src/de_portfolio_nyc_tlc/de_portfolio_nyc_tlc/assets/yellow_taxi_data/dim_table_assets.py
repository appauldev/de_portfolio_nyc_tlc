from dagster import MaterializeResult, MetadataValue, asset
from dagster_duckdb import DuckDBResource

from .table_assets import table_YT_trip_records_2022
from .constants import table_names


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing the date and time of the taxi trip
    """,
)
def dim_trip_datetime(duckdb: DuckDBResource) -> MaterializeResult:
    with duckdb.get_connection() as conn:
        main_table = table_names.TABLE_YELLOW_TAXI_TRIPS
        dim_trip_datetime = table_names.TABLE_DIM_TRIP_DATETIME

        query_create_dim_table = f"""--sql
        CREATE OR REPLACE SEQUENCE dim_trip_datetime_seq START 1;

        CREATE OR REPLACE TABLE {dim_trip_datetime} (
            datetime_key BIGINT DEFAULT NEXTVAL('dim_trip_datetime_seq'),
            pickup_dtime TIMESTAMP,
            pickup_date DATE,
            pickup_hour TINYINT,
            pickup_dow TINYINT,
            dropoff_dtime TIMESTAMP,
            dropoff_date DATE,
            dropoff_hour TINYINT,
            dropoff_dow TINYINT
        );

        INSERT INTO {dim_trip_datetime} (
            pickup_dtime,
            pickup_date,
            pickup_hour,
            pickup_dow,
            dropoff_dtime,
            dropoff_date,
            dropoff_hour,
            dropoff_dow
        )
            SELECT DISTINCT
                pickup_dtime,
                pickup_dtime::DATE,
                DATEPART('hour', pickup_dtime),
                DATEPART('dow', pickup_dtime),
                dropoff_dtime,
                dropoff_dtime::DATE,
                DATEPART('hour', dropoff_dtime),
                DATEPART('dow', dropoff_dtime)
            FROM {main_table};
       """

        conn.sql(query_create_dim_table)
        conn.sql(
            f"""--sql
                 SELECT * FROM {dim_trip_datetime} LIMIT 10
            """
        ).show()
        result = conn.sql(f"SELECT * FROM {dim_trip_datetime} LIMIT 10").to_df()
        print(result.dtypes)
        # metadata
        count = conn.sql(
            f"""--sql
                SELECT COUNT(*) AS total_count FROM {dim_trip_datetime}
            """
        ).to_df()
        print(f"{count.at[0, "total_count"]}")

    return MaterializeResult(
        metadata={
            "Total count of records": MetadataValue.text(
                f"{count.at[0, "total_count"]:,}"
            )
        }
    )


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing the details about the pickup and dropoff location of the taxi trip
    """,
)
def dim_trip_location(duckdb: DuckDBResource) -> MaterializeResult:

    with duckdb.get_connection() as conn:
        tbl_name = table_names.TABLE_YELLOW_TAXI_TRIPS
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
        main_table = table_names.TABLE_YELLOW_TAXI_TRIPS
        dim_transaction_fees = table_names.TABLE_DIM_TRANSACTION_FEES

        query_create_dim_table = f"""--sql
        CREATE OR REPLACE SEQUENCE dim_transaction_fees_seq START 1;

        CREATE OR REPLACE TABLE {dim_transaction_fees} (
            transaction_key BIGINT DEFAULT NEXTVAL('dim_transaction_fees_seq'),
            total_amount DOUBLE,
            payment_type TINYINT,
            rate_code_id TINYINT,
            fare_amount DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            congestion_surcharge DOUBLE,
            extra DOUBLE,
            airport_fee DOUBLE
        );

        INSERT INTO {dim_transaction_fees} (
            total_amount,
            payment_type,
            rate_code_id,
            fare_amount,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            congestion_surcharge,
            extra,
            airport_fee
        )
            SELECT DISTINCT
                total_amount,
                payment_type,
                rate_code_id,
                fare_amount,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                congestion_surcharge,
                extra,
                airport_fee
            FROM {main_table}
        """

        conn.sql(query_create_dim_table)
        conn.sql(
            f"""--sql
                 SELECT * FROM {dim_transaction_fees} LIMIT 10
            """
        ).show()
        result = conn.sql(f"SELECT * FROM {dim_transaction_fees} LIMIT 10").to_df()
        print(result.dtypes)
        # Metadata
        count = conn.sql(
            f"""--sql
            SELECT COUNT(*) AS total_count FROM {dim_transaction_fees}
            """
        ).to_df()
        print(f"{count.at[0, "total_count"]}")

    return MaterializeResult(
        metadata={
            "Count of total records": MetadataValue.text(
                f"{count.at[0, "total_count"]:,}"
            )
        }
    )


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing miscelleneous details about the trip
    """,
)
def dim_taxi_misc_details(duckdb: DuckDBResource) -> MaterializeResult:

    with duckdb.get_connection() as conn:
        tbl_name = table_names.TABLE_YELLOW_TAXI_TRIPS
        query = """
        SELECT
            vendor_id,
            store_and_fwd_flag,
        FROM {tbl_name}""".format(
            tbl_name=tbl_name
        )
        conn.sql(query).show()

    return MaterializeResult(metadata={})
