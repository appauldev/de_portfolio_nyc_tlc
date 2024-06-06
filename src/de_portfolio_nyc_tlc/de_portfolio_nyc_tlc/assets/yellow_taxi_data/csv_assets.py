from pathlib import Path
from dagster import (
    AssetExecutionContext,
    asset,
    MaterializeResult,
    MetadataValue,
)
import os
import httpx

from .helpers import csv_asset_helpers as helper

from ...utils.log_utils import log_w_header

from ...partitions import monthly_partition
from .. import constants


@asset(
    partitions_def=monthly_partition,
    description="""The raw csv files for the year 2022. The files are downloaded via stream and partitioned by month to take
    advantage of the resource's API and to prevent connection timeouts""",
)
async def YT_monthly_csv_2022(
    context: AssetExecutionContext,
) -> MaterializeResult:

    start_date, end_date = helper.get_monthly_range(context.partition_key)
    print(f"{start_date} {end_date}")
    CSV_FILE_NAME = helper.create_file_save_path(
        start_date, Path(f"{os.path.dirname(__file__)}/data/csv")
    )

    # API request details
    # See this doc for $where and other API queries: https://dev.socrata.com/docs/queries/
    app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
    # request the file as JSON, but can also be requested as csv
    file_type = "json"
    where_query = f"tpep_pickup_datetime >= '{start_date}' AND tpep_pickup_datetime < '{end_date}'"
    response_limit = 500_000
    offset = 0
    timeout = httpx.Timeout(90)
    # construct the API request url
    url = (
        f"{constants.YELLOW_TAXI_TRIPS_2022_URL}.{file_type}?"
        + f"$$app_token={app_token}"
        + f"&$where={where_query}"
        + f"&$limit={response_limit}"
    )
    # make the `offset` query params formattable since it will change value in the request loop
    url = url + "&$offset={offset}"

    async with httpx.AsyncClient() as client:
        try:
            # for logging and the returned metadata
            total_records_saved = 0
            partition_name = CSV_FILE_NAME.split("/")[-1]

            # start the request loop
            should_continue_request_loop = True
            while should_continue_request_loop:
                log_w_header(
                    f"{partition_name}: requesting with {response_limit=} and {offset=}",
                    "/",
                )
                # read the response as stream and set the value for the offset query params
                async with client.stream(
                    "GET",
                    url.format(offset=offset),
                    timeout=timeout,
                ) as response:
                    # handle response
                    accumulator_limit = 100_000
                    result = await helper.handle_stream_data_response(
                        response=response,
                        response_limit=response_limit,
                        accumulator_limit=accumulator_limit,
                        file_name=CSV_FILE_NAME,
                        total_records_saved=total_records_saved,
                    )
                    should_continue_request_loop = result[0]
                    total_records_saved = result[1]
                    log_w_header(
                        f"{partition_name}: End of response stream. Saved {total_records_saved} rows for this request ({should_continue_request_loop=})",
                        "x",
                    )

                    # continue requesting for data if you are still reaching the line limit
                    if should_continue_request_loop:
                        offset += response_limit

        except httpx.HTTPError as exc:
            print(f"HTTP Exception for {exc.request.url}")
            print(f"Error message: {exc}")
            print(f"Response: {response=}")
            print(f"Server error: {response.is_server_error}")
            print(f"Client error: {response.is_client_error}")
            raise Exception("HTTP Error")
        finally:
            log_w_header(
                f"{partition_name}: Total records fetched: {total_records_saved}"
            )
    return MaterializeResult(
        metadata={
            "Data source documentation": MetadataValue.url(
                "https://dev.socrata.com/foundry/data.cityofnewyork.us/qp3b-zxtp"
            ),
            "Number of fetched records": MetadataValue.int(total_records_saved),
        }
    )


@asset
async def taxi_zone_lookup_csv():
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    file_save_path = os.path.join(
        os.path.dirname(__file__), "data/csv", "taxi_zone_lookup.csv"
    )

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)

            with open(file_save_path, "wb") as f:
                f.write(response.content)
        except Exception as e:
            print(e)
