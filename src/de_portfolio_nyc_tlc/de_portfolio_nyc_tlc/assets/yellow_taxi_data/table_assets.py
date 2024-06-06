from dagster import AssetCheckResult, MaterializeResult, MetadataValue, asset
from dagster_duckdb import DuckDBResource
import os


from .constants import table_names

from .checks import table_asset_checks as checks


from .parquet_assets import YT_monthly_parquet_2022

from .csv_assets import taxi_zone_lookup_csv


@asset(
    deps=[YT_monthly_parquet_2022],
    description="""
        The table resulting from the combined parquet assets
        """,
    check_specs=[check_spec.AssetCheckSpec for check_spec in checks.check_spec_list],
)
def table_YT_trip_records_2022(duckdb: DuckDBResource) -> MaterializeResult:
    CURRENT_DIR = os.path.dirname(__file__)
    PARQUET_FILES = [
        entry.path
        for entry in os.scandir(os.path.join(CURRENT_DIR, "data", "parquet"))
        if entry.is_file() and entry.name.endswith(".parquet")
    ]
    # sort files from JAN-DEC
    PARQUET_FILES = sorted(PARQUET_FILES)

    main_table = table_names.TABLE_YELLOW_TAXI_TRIPS

    # persist the duckdb data
    with duckdb.get_connection() as conn:
        # query to create the main table
        # Ideally, we would just directly import the parquet files to duckdb and let it infer the
        #   dtypes of the columns, and after that, alter the table to add a primary key constraint
        #   However, duckdb is yet to support ADD/DROP CONSTRAINT statement: https://duckdb.org/docs/sql/statements/alter_table#add--drop-constraint
        # Convenient dtype such as auto-incrementing serial values is not yet supported, but we can
        #   use the CREATE SEQUENCE seq statement to have similar results
        query_create_table = f"""--sql
        CREATE OR REPLACE SEQUENCE pk_seq START 1;

        CREATE OR REPLACE TABLE {main_table} (
            trip_id BIGINT DEFAULT NEXTVAL('pk_seq'),
            vendor_id TINYINT,
            pickup_dtime TIMESTAMP,
            dropoff_dtime TIMESTAMP,
            passenger_count TINYINT,
            trip_distance DOUBLE,
            rate_code_id TINYINT,
            store_and_fwd_flag TINYINT,
            pickup_lid SMALLINT,
            dropoff_lid SMALLINT,
            payment_type TINYINT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            airport_fee DOUBLE,
            __index_level_0__ BIGINT
        );

        INSERT INTO {main_table} (
            vendor_id,
            pickup_dtime,
            dropoff_dtime,
            passenger_count,
            trip_distance,
            rate_code_id,
            store_and_fwd_flag,
            pickup_lid,
            dropoff_lid,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            __index_level_0__
        )
            SELECT * FROM read_parquet({PARQUET_FILES});
        """

        # execute
        conn.sql(query_create_table)
        # verify
        conn.sql(f"SELECT * FROM {main_table} LIMIT 10;").show()

        # metadata
        result = conn.sql(
            f"SELECT COUNT(*) as count_total_records FROM {main_table}"
        ).df()

        print(f"{result}")
        print(f"{result.at[0, "count_total_records"]}")

        return MaterializeResult(
            metadata={
                "Count of total records": MetadataValue.text(
                    f"{result.at[0, "count_total_records"]:,}"
                )
            },
            check_results=[
                AssetCheckResult(
                    check_name=check_spec.AssetCheckSpec.name,
                    passed=check_spec.condition(conn),
                )
                for check_spec in checks.check_spec_list
            ],
        )


@asset(
    deps=[taxi_zone_lookup_csv],
    description="""
        The lookup table for the pickup and dropoff zones of the taxi trips
        """,
)
def taxi_zone_lookup_table(duckdb: DuckDBResource) -> MaterializeResult:

    taxi_zone_file_path = os.path.join(
        os.path.dirname(__file__), "data/csv", "taxi_zone_lookup.csv"
    )
    taxi_zone_lookup = table_names.TABLE_TAXI_ZONE_LOOKUP
    with duckdb.get_connection() as conn:

        query_create_taxi_zone_lookup_table = f"""--sql
            CREATE OR REPLACE TABLE {taxi_zone_lookup} (
                location_id SMALLINT PRIMARY KEY,
                borough TEXT,
                zone TEXT,
                service_zone TEXT
            );

            INSERT INTO {taxi_zone_lookup}
                SELECT * FROM read_csv('{taxi_zone_file_path}');
        """
        conn.sql(query_create_taxi_zone_lookup_table)
        conn.sql(
            f"""--sql
                SELECT * FROM {taxi_zone_lookup};
            """
        ).show()

        return MaterializeResult(metadata={})
