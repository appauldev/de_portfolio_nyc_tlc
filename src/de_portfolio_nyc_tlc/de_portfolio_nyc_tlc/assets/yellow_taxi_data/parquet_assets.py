from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue

from ...utils.log_utils import log_w_header
from ...partitions import monthly_partition

import pandas as pd

import os


@asset(
    deps=["YT_monthly_csv_2022"],
    partitions_def=monthly_partition,
    description="""The generated parquet files from the raw csv.
    Initial cleaning and filtering were done with the data such as:\n
    * Dropping records with missing `passenger_count` and `total_amount`
    * Dropping records with with invalid trip distance, i.e., `trip_distance > 0`, are retained""",
)
def YT_monthly_parquet_2022(
    context: AssetExecutionContext,
) -> MaterializeResult:
    month_num = context.partition_key.split("-")[1]
    # prepend '0' to single digit months
    # month_num = f"0{month_num}" if len(month_num) == 1 else month_num

    # prepare the saving destination
    DATA_FOLDER = os.path.join(os.path.dirname(__file__), "data")
    CSV_FOLDER = os.path.join(DATA_FOLDER, "csv")
    PARQUET_FOLDER = os.path.join(DATA_FOLDER, "parquet")

    CSV_FILE = os.path.join(CSV_FOLDER, f"2022-{month_num}.csv")
    PARQUET_FILE = os.path.join(PARQUET_FOLDER, f"2022-{month_num}.parquet")

    # test = [
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
    # NOTE: setting the dtype for loading the csv results into an error because some columns have
    #  malformed data (like a string in the total_amount column).
    #  Hence, some of the columns will undergo initial cleaning and transformation before they are converted to a more appropriate data type
    timestamp_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    df = pd.read_csv(CSV_FILE, parse_dates=timestamp_cols)
    raw_csv_length = len(df)

    # clean total_amount column by converting the data to a numeric value.
    # errors="coerce" will convert non-numeric values to NaN
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")

    # drop records that have no passenger count and no total amount
    df = df.dropna(subset=["passenger_count", "total_amount"])

    # only keep records with a valid trip_distance, ie, trip_distance is positive
    df = df[df["trip_distance"] >= 0]

    # convert columns to appropriate data types
    cols_to_convert = [
        "vendorid",
        "passenger_count",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "payment_type",
    ]

    for col in cols_to_convert:
        log_w_header(f"Converting {col=}", "/")
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # df[
    #     [
    #         "vendorid",
    #         "passenger_count",
    #         "ratecodeid",
    #         "pulocationid",
    #         "dolocationid",
    #         "payment_type",
    #     ]
    # ] = df[
    #     [
    #         "vendorid",
    #         "passenger_count",
    #         "ratecodeid",
    #         "pulocationid",
    #         "dolocationid",
    #         "payment_type",
    #     ]
    # ].astype(
    #     int
    # )
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype(str)

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

    # save dataframe as parquet
    df.to_parquet(PARQUET_FILE)

    return MaterializeResult(
        metadata={
            "Number of records - parquet": MetadataValue.int(num_records),
            "Number of records - raw csv": MetadataValue.int(raw_csv_length),
            "Column info": MetadataValue.json(col_info_json),
        }
    )
