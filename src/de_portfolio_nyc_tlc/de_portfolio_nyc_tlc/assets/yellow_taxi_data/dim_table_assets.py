from dagster import MaterializeResult, MetadataValue, asset
from dagster_duckdb import DuckDBResource

from .table_assets import table_YT_trip_records_2022, taxi_zone_lookup_table
from .constants import table_names

from .helpers import table_helpers as helper


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing the date and time of the taxi trip
    """,
)
def dim_trip_datetime(duckdb: DuckDBResource) -> MaterializeResult:
    with duckdb.get_connection() as conn:
        query_create_dim_trip_datetime = f"""--sql
        CREATE OR REPLACE SEQUENCE dim_trip_datetime_seq START 1;

        CREATE OR REPLACE TABLE {table_names.DIM_TRIP_DATETIME} (
            datetime_key BIGINT DEFAULT NEXTVAL('dim_trip_datetime_seq') PRIMARY KEY,
            pickup_dtime TIMESTAMP,
            pickup_date DATE,
            pickup_hour TINYINT,
            pickup_dow TINYINT,
            dropoff_dtime TIMESTAMP,
            dropoff_date DATE,
            dropoff_hour TINYINT,
            dropoff_dow TINYINT
        );

        INSERT INTO {table_names.DIM_TRIP_DATETIME} (
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
            FROM {table_names.YELLOW_TAXI_TRIPS};
       """
        # execute
        conn.sql(query_create_dim_trip_datetime)
        # metadata
        metadata = helper.get_table_metadata(
            conn=conn, table_name=table_names.DIM_TRIP_DATETIME
        )

    return MaterializeResult(metadata=metadata)


@asset(
    deps=[table_YT_trip_records_2022, taxi_zone_lookup_table],
    description="""
    The dimension table containing the details about the pickup and dropoff location of the taxi trip
    """,
)
def dim_trip_location(duckdb: DuckDBResource) -> MaterializeResult:

    with duckdb.get_connection() as conn:
        query_create_dim_trip_location = f"""--sql
        CREATE OR REPLACE SEQUENCE dim_trip_location_seq START 1;

        CREATE OR REPLACE TABLE {table_names.DIM_TRIP_LOCATION} (
            trip_location_key BIGINT DEFAULT NEXTVAL('dim_trip_location_seq') PRIMARY KEY,
            pickup_lid SMALLINT,
            pickup_borough TEXT,
            pickup_zone TEXT,
            pickup_service_zone TEXT,
            dropoff_lid SMALLINT,
            dropoff_borough TEXT,
            dropoff_zone TEXT,
            dropoff_service_zone TEXT
        );

        INSERT INTO {table_names.DIM_TRIP_LOCATION} (
            pickup_lid,
            pickup_borough,
            pickup_zone,
            pickup_service_zone,
            dropoff_lid,
            dropoff_borough,
            dropoff_zone,
            dropoff_service_zone
        )
        SELECT DISTINCT
            trips.pickup_lid,
            taxi_zone_pickup.borough,
            taxi_zone_pickup.zone,
            taxi_zone_pickup.service_zone,
            trips.dropoff_lid,
            taxi_zone_dropoff.borough,
            taxi_zone_dropoff.zone,
            taxi_zone_dropoff.service_zone,
        FROM {table_names.YELLOW_TAXI_TRIPS} as trips
            INNER JOIN {table_names.TAXI_ZONE_LOOKUP} as taxi_zone_pickup
                ON trips.pickup_lid = taxi_zone_pickup.location_id
            INNER JOIN {table_names.TAXI_ZONE_LOOKUP} as taxi_zone_dropoff
                ON trips.dropoff_lid = taxi_zone_dropoff.location_id
        """
        # execute
        conn.sql(query_create_dim_trip_location)
        # metadata
        metadata = helper.get_table_metadata(
            conn=conn, table_name=table_names.DIM_TRIP_LOCATION
        )

    return MaterializeResult(metadata=metadata)


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing the details of the taxi trip payment transaction
    """,
)
def dim_transaction_fees(duckdb: DuckDBResource) -> MaterializeResult:

    with duckdb.get_connection() as conn:
        query_create_dim_transaction_fees = f"""--sql
        CREATE OR REPLACE SEQUENCE dim_transaction_fees_seq START 1;

        CREATE OR REPLACE TABLE {table_names.DIM_TRANSACTION_FEES} (
            transaction_key BIGINT DEFAULT NEXTVAL('dim_transaction_fees_seq') PRIMARY KEY,
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

        INSERT INTO {table_names.DIM_TRANSACTION_FEES} (
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
            FROM {table_names.YELLOW_TAXI_TRIPS}
        """
        # execute
        conn.sql(query_create_dim_transaction_fees)
        # metadata
        metadata = helper.get_table_metadata(
            conn=conn, table_name=table_names.DIM_TRANSACTION_FEES
        )

    return MaterializeResult(metadata=metadata)


@asset(
    deps=[table_YT_trip_records_2022],
    description="""
    The dimension table containing miscelleneous details about the trip
    """,
)
def dim_trip_misc_details(duckdb: DuckDBResource) -> MaterializeResult:

    with duckdb.get_connection() as conn:
        query_create_dim_trip_misc_details = f"""--sql
        CREATE OR REPLACE SEQUENCE dim_trip_misc_details_seq START 1;

        CREATE OR REPLACE TABLE {table_names.DIM_TRIP_MISC_DETAILS} (
            misc_detail_key BIGINT DEFAULT NEXTVAL('dim_trip_misc_details_seq') PRIMARY KEY,
            vendor_id TINYINT,
            store_and_fwd_flag TINYINT
        );

        INSERT INTO {table_names.DIM_TRIP_MISC_DETAILS} (
            vendor_id,
            store_and_fwd_flag
        )
            SELECT DISTINCT
                vendor_id,
                store_and_fwd_flag,
            FROM {table_names.YELLOW_TAXI_TRIPS}
        """
        # execute
        conn.sql(query_create_dim_trip_misc_details)
        # metadata
        metadata = helper.get_table_metadata(
            conn=conn, table_name=table_names.DIM_TRIP_MISC_DETAILS
        )

        return MaterializeResult(metadata=metadata)
