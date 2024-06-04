from pathlib import Path
from dagster import asset
import duckdb
import os


@asset(deps=["YT_monthly_parquet_2022"])
def staging_YT_2022():
    CURRENT_DIR = os.path.dirname(__file__)
    PARQUET_FILES = [
        entry.path
        for entry in os.scandir(os.path.join(CURRENT_DIR, "data", "parquet"))
        if entry.is_file() and entry.name.endswith(".parquet")
    ]
    MODELS_DIR = "models"
    MAIN_TABLE = "taxi_trip_records.duckdb"

    conn = duckdb.connect(os.path.join(CURRENT_DIR, MODELS_DIR, MAIN_TABLE))
    # query to create the main table
    query_create_table = """
    CREATE OR REPLACE TABLE taxi_trip_records AS
        SELECT * FROM read_parquet({PARQUET_FILES});
    """.format(
        PARQUET_FILES=PARQUET_FILES
    )
    # execute
    conn.sql(query_create_table)
    # verify
    conn.sql("SELECT * FROM taxi_trip_records LIMIT 10;").show()
