from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    AssetCheckResult,
)
from numpy import int16, int8

from ...partitions import monthly_partition

from .checks import parquet_assets_checks as checks

import pandas as pd

import os


@asset(
    deps=["YT_monthly_csv_2022"],
    partitions_def=monthly_partition,
    description="""
    The generated parquet files from the raw csv.
    Initial cleaning and filtering were done with the data such as:\n
    * Dropping records with missing `passenger_count` and `total_amount`
    * Dropping records with with invalid trip distance, i.e., `trip_distance > 0`, are retained
    """,
    dagster_type={},
    check_specs=[check_spec.AssetCheckSpec for check_spec in checks.check_spec_list],
)
def YT_monthly_parquet_2022(
    context: AssetExecutionContext,
) -> MaterializeResult:
    month_num = context.partition_key.split("-")[1]

    # prepare the saving destination
    DATA_FOLDER = os.path.join(os.path.dirname(__file__), "data")
    CSV_FOLDER = os.path.join(DATA_FOLDER, "csv")
    PARQUET_FOLDER = os.path.join(DATA_FOLDER, "parquet")

    CSV_FILE = os.path.join(CSV_FOLDER, f"2022-{month_num}.csv")
    PARQUET_FILE = os.path.join(PARQUET_FOLDER, f"2022-{month_num}.parquet")

    # cols = [
    #     "vendorid",
    #     "tpep_pickup_datetime",
    #     "tpep_dropoff_datetime",
    #     "passenger_count",
    #     "trip_distance",
    #     "ratecodeid",
    #     "store_and_fwd_flag",
    #     "pulocationid",
    #     "dolocationid",
    #     "payment_type",
    #     "fare_amount",
    #     "extra",
    #     "mta_tax",
    #     "tip_amount",
    #     "tolls_amount",
    #     "improvement_surcharge",
    #     "total_amount",
    #     "congestion_surcharge",
    #     "airport_fee",
    # ]

    # read the csv files and indicate the timestamp columns
    # NOTE: Ideally, the dtype would be set when reading the csv, but some columns have invalid data that results into an error
    #  Hence, some of the columns will undergo initial cleaning and transformation before they are converted to a more appropriate data type
    timestamp_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    df = pd.read_csv(CSV_FILE, parse_dates=timestamp_cols)
    # for metadata
    raw_csv_length = len(df)

    # INITIAL CLEANING AND TRANSFORMATIONS
    # clean some of the numeric columns by converting invalid values to NaN
    cols_to_numeric = [
        "vendorid",
        "passenger_count",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "payment_type",
        "total_amount",
        "trip_distance",
    ]
    for col in cols_to_numeric:
        # Convert invalid numeric values to NaNs
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # drop invalid trip records w/ NaN values after the type conversion
    df = df.dropna(subset=["passenger_count", "total_amount", "trip_distance"])

    # Convert columns to ints
    cols_to_int = [
        "vendorid",
        "passenger_count",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "payment_type",
    ]
    for col in cols_to_int:
        # Replace NaNs to prevent error during dtype conversion
        df.fillna({col: -1}, inplace=True)

        if col == "pulocationid" or col == "dolocationid":
            df[col] = df[col].astype(int16)
            continue

        df[col] = df[col].astype(int8)

    # convert the flag column to int
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].mask(
        df["store_and_fwd_flag"] == "Y", 1
    )
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].mask(
        df["store_and_fwd_flag"] == "N", 0
    )
    df["store_and_fwd_flag"] = pd.to_numeric(df["store_and_fwd_flag"], errors="coerce")
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].fillna(-1)
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype(int8)

    # filter valid trips
    trips_w_passengers = df["passenger_count"] > 0
    only_paid_trips = df["total_amount"] > 0
    trips_w_valid_distance = df["trip_distance"] > 0

    df = df[trips_w_passengers & trips_w_valid_distance & only_paid_trips]

    # rename columns
    col_names = {
        "vendorid": "vendor_id",
        "tpep_pickup_datetime": "pickup_dtime",
        "tpep_dropoff_datetime": "dropoff_dtime",
        "ratecodeid": "rate_code_id",
        "pulocationid": "pickup_lid",
        "dolocationid": "dropoff_lid",
    }
    df.rename(columns=col_names, inplace=True)

    # prepare materialization metadata
    num_records = len(df)
    col_info_json = {}
    for index, value in df.dtypes.items():
        col_info_json[str(index)] = str(value)

    print(df)
    print(df.dtypes)

    # save dataframe as parquet
    df.to_parquet(PARQUET_FILE)

    return MaterializeResult(
        metadata={
            "Number of records - parquet": MetadataValue.int(num_records),
            "Number of records - raw csv": MetadataValue.int(raw_csv_length),
            "Column info": MetadataValue.json(col_info_json),
        },
        check_results=[
            AssetCheckResult(
                check_name=check_spec.AssetCheckSpec.name,
                passed=check_spec.condition(df),
            )
            for check_spec in checks.check_spec_list
        ],
    )
