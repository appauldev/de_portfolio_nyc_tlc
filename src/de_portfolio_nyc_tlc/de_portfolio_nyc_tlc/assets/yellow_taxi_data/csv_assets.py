from dagster import (
    AssetExecutionContext,
    graph_asset,
    asset,
    MaterializeResult,
    MetadataValue,
)
import pandas as pd
import os
import httpx
import json
import time
from datetime import timedelta

from .ops.csv_assets_ops import handle_stream_data_response

from ...utils.log_utils import log_w_header

from ...partitions import monthly_partition
from .. import constants


@asset(
    partitions_def=monthly_partition,
    description="""The raw csv files for the year 2022. The files are downloaded via stream and partitioned by month to take
    advantage of the resource's API and to prevent connection timeouts""",
)
async def yellow_taxi_monthly_csv_2022(
    context: AssetExecutionContext,
) -> MaterializeResult:

    # Values for the API request queries
    # start_date and end_date will be used for the sql-like query $where
    start_date = context.asset_partition_key_for_output()
    end_date = (
        monthly_partition.get_next_partition_key(start_date)
        if start_date.split("-")[1] != "12"
        else "2024-01-01"
    )

    # Values for saving the streamed file
    # where to save
    month_num = int(context.asset_partition_key_for_output().split("-")[1])
    CSV_DATA_FOLDER = os.path.join(os.path.dirname(__file__), "data", "csv")
    FILE_NAME = os.path.join(CSV_DATA_FOLDER, f"2022-{month_num}.csv")
    # file name tag

    # API request details
    # See this doc for $where and other API queries: https://dev.socrata.com/docs/queries/
    app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
    # request the file as JSON, but can also be requested as csv
    # I am requesting it as JSON for learning purposes!
    file_type = "json"
    where_query = f"tpep_pickup_datetime >= '{start_date}' AND tpep_pickup_datetime < '{end_date}'"
    response_limit = 500000
    offset = 0
    timeout = httpx.Timeout(90)
    # construct the API request url
    url = (
        f"{constants.YELLOW_TAXI_TRIPS_2022_URL}.{file_type}?"
        + f"$$app_token={app_token}"
        + f"&$where={where_query}"
        + f"&$limit={response_limit}"
        + f"&$offset={offset}"
    )

    async with httpx.AsyncClient() as client:
        try:
            log_w_header(f"starting stream download for month {month_num}", "/")

            # for logging
            total_rows_loaded = 0
            start_req_time = 0.0
            end_req_time = 0.0
            stream_total_time = 0.0

            # start the request loop
            # Limit the records request to N rows where N is response_limit and
            #  use offset to get the next set of rows, if there will be any
            should_stream_data = True
            while should_stream_data:
                log_w_header(f"requesting with {response_limit=} and {offset=}", "*")
                start_req_time = time.time()
                async with client.stream(
                    "GET",
                    url,
                    timeout=timeout,
                ) as response:
                    # log request time
                    end_req_time = time.time()
                    log_w_header(
                        f"Request took {(end_req_time - start_req_time):.2f} seconds",
                        ".",
                    )

                    # Check for exceptions
                    response.raise_for_status()

                    # save every X lines to csv where X = line_limit, more on this below
                    line_accumulator = []
                    line_limit = 100000
                    append_flag = True if offset == 0 else False

                    # log time for streaming data
                    start_stream_time = time.time()
                    async for line in response.aiter_lines():
                        # end the request loop when there are no more rows to fetch
                        # this works when `response_limit` % `line_limit` != 0 or
                        #  when the count of record of the previous request
                        #  is divisible by line_limit
                        if line == "[]":
                            log_w_header(
                                "Received an empty response for stream data", "x"
                            )
                            should_stream_data = False
                            break

                        # json line received, clean the data!
                        # remove the json brackets and json comma separators
                        line = line.strip(",[]")
                        # convert the line to a dictionary and add to the line_accumulator list
                        line_accumulator.append(json.loads(line))

                        # save the collected lines
                        # Use the offset variable to determine if the list of rows will be written as new or will be appended
                        # offset can be also used to prevent writing the csv header for appended rows
                        # NOTE: if `len(line_accumulator)` < `line_limit` by the end of the for loop, that means we have exhausted the response
                        #  See the corresponding conditional statement below for this case.
                        if len(line_accumulator) == line_limit:
                            df = pd.DataFrame.from_records(line_accumulator)
                            df.to_csv(
                                f"{CSV_DATA_FOLDER}/2022-{month_num}.csv",
                                mode="w" if append_flag else "a",
                                header=append_flag,
                                index=False,
                            )
                            # log the current number of records collected and the stream time
                            total_rows_loaded += line_limit
                            end_stream_time = time.time()
                            stream_total_time += end_stream_time - start_stream_time
                            log_w_header(
                                f"2022-{month_num}: Saved {total_rows_loaded} rows (took {(end_stream_time - start_stream_time):.2f} seconds to save {line_limit} records)",
                                ".",
                            )
                            # clear line_accumulator[] for the next set of records
                            line_accumulator.clear()
                            start_stream_time = time.time()
                    # end of for loop

                    # add the remaining lines if there are any
                    # this also means that there are no more stream data for the current request
                    #  since the streamed data did not reach the line_limit
                    # NOTE: Ending the request loop will only work correctly when response_limit % line_limit == 0
                    #  This means if the response_limit is not divisible by line_limit, the request loop
                    #  will terminate after the first request even if there is still data for our API request
                    if len(line_accumulator) > 0:
                        df = pd.DataFrame.from_records(line_accumulator)
                        df.to_csv(
                            FILE_NAME,
                            mode="w" if offset == 0 else "a",
                            header=True if offset == 0 else False,
                            index=False,
                        )
                        # update the counter and clear the line_accumulator list
                        total_rows_loaded += len(line_accumulator)
                        end_stream_time = time.time()
                        stream_total_time += end_stream_time - start_stream_time

                        print(
                            f"2022-{month_num}: Saved {len(line_accumulator)} rows from clean up (took {(end_stream_time - start_stream_time):.2f} seconds to save {line_limit} records)",
                            ".",
                        )

                        log_w_header("No more stream data", "x")

                        # NOTE: Important! Again, the stream download request loop will end prematurely
                        #  if `response_limit` is not divisible by `line_limit` since the last batch of lines
                        #  that will be streamed will always be less than `line_limit` during the first request.
                        if response_limit % line_limit == 0:
                            should_stream_data = False

                        line_accumulator.clear()

                    log_w_header(f"2022-{month_num}: Offset {offset} done!", "*")
                    print(f"Current total rows: {total_rows_loaded}")
                    print(
                        f"Took {timedelta(seconds=stream_total_time)} to stream and save the {response_limit} records"
                    )
                    stream_total_time = 0.0
                    # continue requesting for streaming data if you are still reaching the line limit
                    if should_stream_data:
                        offset += response_limit
                    # print(f"fetched within {(end_time - start_time):.2f} seconds")
                    # print(line_accumulator)
        except httpx.HTTPError as exc:
            print(f"HTTP Exception for {exc.request.url}")
            print(f"Error message: {exc}")
            print(f"Response: {response=}")
            print(f"Server error: {response.is_server_error}")
            print(f"Client error: {response.is_client_error}")
            raise Exception("HTTP Error")
        finally:
            log_w_header(f"total rows downloaded: {total_rows_loaded}", "/")

    return MaterializeResult(
        metadata={
            "Data source documentation": MetadataValue.url(
                "https://dev.socrata.com/foundry/data.cityofnewyork.us/qp3b-zxtp"
            ),
            "Number of fetched records": MetadataValue.int(total_rows_loaded),
        }
    )


# @graph_asset
# def YT_monthly_csv_2022():
#     pass
