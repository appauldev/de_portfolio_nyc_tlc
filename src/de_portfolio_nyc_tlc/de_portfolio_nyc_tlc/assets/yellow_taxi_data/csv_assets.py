from dagster import AssetExecutionContext, asset, MaterializeResult, MetadataValue
import pandas as pd
import os
import httpx
import json
import time
from datetime import timedelta

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
    # file type to stream
    DATA_TYPE_TO_STREAM = "json"

    # Values for saving the streamed file
    # where to save
    SAVE_DIR = os.path.join(os.path.dirname(__file__), "data")
    # file name tag
    month_num = int(context.asset_partition_key_for_output().split("-")[1])

    async with httpx.AsyncClient() as client:
        try:
            log_w_header(f"starting stream download for month {month_num}", "/")

            # request details
            # $where query https://dev.socrata.com/docs/queries/
            app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
            where = f"tpep_pickup_datetime >= '{start_date}' AND tpep_pickup_datetime < '{end_date}'"
            request_limit = 500000
            offset = 0

            # for logging
            total_rows_loaded = 0
            start_req_time = 0.0
            end_req_time = 0.0
            stream_total_time = 0.0

            # start the request loop
            # Limit the records request to N rows where N is request_limit and
            #  use offset to get the next set of rows, if there will be any
            should_stream_data = True
            while should_stream_data:
                log_w_header(f"requesting with {request_limit=} and {offset=}", "*")
                start_req_time = time.time()
                async with client.stream(
                    "GET",
                    f"{constants.YELLOW_TAXI_TRIPS_2022_URL}.{DATA_TYPE_TO_STREAM}?"
                    + f"$$app_token={app_token}"
                    + f"&$where={where}"
                    + f"&$limit={request_limit}"
                    + f"&$offset={offset}",
                    timeout=httpx.Timeout(90.0),
                ) as response:
                    # Check for exceptions
                    response.raise_for_status()

                    # log request time
                    end_req_time = time.time()
                    log_w_header(
                        f"Request took {(end_req_time - start_req_time):.2f} seconds",
                        ".",
                    )

                    # save every X lines to csv where X = line_limit, more on this below
                    lines = []
                    line_limit = 100000

                    # log time for streaming data
                    start_stream_time = time.time()
                    async for line in response.aiter_lines():
                        # end the request loop when there are no more rows to fetch
                        # this works when `request_limit` % `line_limit` != 0 or
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
                        # convert the line to a dictionary and add to the lines list
                        lines.append(json.loads(line))

                        # save the collected lines
                        # Use the offset variable to determine if the list of rows will be written as new or will be appended
                        # offset can be also used to prevent writing the csv header for appended rows
                        if len(lines) == line_limit:
                            df = pd.DataFrame.from_records(lines)
                            df.to_csv(
                                f"{SAVE_DIR}/csv/2022-{month_num}.csv",
                                mode="w" if offset == 0 else "a",
                                header=True if offset == 0 else False,
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
                            # clear lines[] for the next set of records
                            lines.clear()
                            start_stream_time = time.time()
                    # end of for loop

                    # add the remaining lines if there are any
                    # this also means that there are no more stream data for the current request
                    #  since the streamed data did not reach the line_limit
                    # NOTE: Ending the request loop will only work correctly when request_limit % line_limit == 0
                    #  This means if the request_limit is not divisible by line_limit, the request loop
                    #  will terminate after the first request even if there is still data for the API request of the sql-like query
                    if len(lines) > 0:
                        df = pd.DataFrame.from_records(lines)
                        df.to_csv(
                            f"{SAVE_DIR}/csv/2022-{month_num}.csv",
                            mode="w" if offset == 0 else "a",
                            header=True if offset == 0 else False,
                            index=False,
                        )
                        # update the counter and clear the lines list
                        total_rows_loaded += len(lines)
                        end_stream_time = time.time()
                        stream_total_time += end_stream_time - start_stream_time

                        print(
                            f"2022-{month_num}: Saved {len(lines)} rows from clean up (took {(end_stream_time - start_stream_time):.2f} seconds to save {line_limit} records)",
                            ".",
                        )

                        log_w_header("No more stream data", "x")

                        if request_limit % line_limit == 0:
                            should_stream_data = False

                        lines.clear()

                    log_w_header(f"2022-{month_num}: Offset {offset} done!", "*")
                    print(f"Current total rows: {total_rows_loaded}")
                    print(
                        f"Took {timedelta(seconds=stream_total_time)} to stream and save the {request_limit} records"
                    )
                    stream_total_time = 0.0
                    # continue requesting for streaming data if you are still reaching the line limit
                    if should_stream_data:
                        offset += request_limit
                    # print(f"fetched within {(end_time - start_time):.2f} seconds")
                    # print(lines)
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
