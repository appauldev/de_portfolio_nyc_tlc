from dagster import MaterializeResult, MetadataValue, asset
import duckdb
import pandas as pd
import os


@asset(deps=["YT_monthly_parquet_2022"])
def staging_table_YT_2022():
    CURRENT_DIR = os.path.dirname(__file__)
    PARQUET_FILES = [
        entry.path
        for entry in os.scandir(os.path.join(CURRENT_DIR, "data", "parquet"))
        if entry.is_file() and entry.name.endswith(".parquet")
    ]
    # sort files from JAN-DEC
    PARQUET_FILES = sorted(PARQUET_FILES)

    MODELS_DIR = "models"
    MAIN_DB = "taxi_trip_records.duckdb"
    MAIN_TABLE_PATH = os.path.join(CURRENT_DIR, MODELS_DIR, MAIN_DB)

    tbl_name = "taxi_trip_records"

    # persist the duckdb data
    conn = duckdb.connect(MAIN_TABLE_PATH)

    # query to create the main table
    # Ideally, we would just directly import the parquet files to duckdb and let it infer the
    #   dtypes of the columns, and after that, alter the table to add a primary key constraint
    #   However, duckdb is yet to support ADD/DROP CONSTRAINT statement: https://duckdb.org/docs/sql/statements/alter_table#add--drop-constraint
    # Convenient dtype such as auto-incrementing serial values is not yet supported, but we can
    #   use the CREATE SEQUENCE seq statement to have similar results
    query_create_table = """
    CREATE OR REPLACE SEQUENCE pk_seq START 1;

    CREATE OR REPLACE TABLE {tbl_name} (
        trip_id BIGINT DEFAULT NEXTVAL('pk_seq') PRIMARY KEY,
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

    INSERT INTO {tbl_name} (
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
    """.format(
        PARQUET_FILES=PARQUET_FILES, tbl_name=tbl_name
    )

    # execute
    conn.sql(query_create_table)
    # verify
    conn.sql(f"SELECT * FROM {tbl_name} LIMIT 10;").show()

    # metadata
    result = conn.sql(f"SELECT COUNT(*) as count_total_records FROM {tbl_name}").df()

    result["count_total_records"] = result["count_total_records"]
    print(f"{result}")
    print(f"{result.at[0, "count_total_records"]}")

    return MaterializeResult(
        metadata={
            "Count of total records": MetadataValue.text(
                str(f"{result.at[0, "count_total_records"]:,}")
            )
        }
    )
