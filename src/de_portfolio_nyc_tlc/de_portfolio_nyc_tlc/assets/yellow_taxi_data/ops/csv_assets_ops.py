import json
import os
from dagster import (
    In,
    OpExecutionContext,
    op,
    get_dagster_logger,
)
from httpx import AsyncClient, Response
from numpy import append
import pandas as pd


@op
def async_stream_download(client: AsyncClient):

    pass


@op
def get_monthly_partition_key(context: OpExecutionContext) -> str:
    return context.asset_partition_key_for_output()


@op(
    ins={
        "response": In(Response),
        "line_limit": In(int),
        "file_name": In(str),
        "append_flag": In(bool),
    }
)
async def handle_stream_data_response(
    response: Response, line_limit: int, file_name: str, append_flag: bool
):
    # Check for exceptions
    response.raise_for_status()

    # save every N lines, where N = line_limit
    line_accumulator = []

    async for line in response.aiter_lines():
        # check for empty response
        # end the request loop when there are no more rows to fetch
        # this works when `request_limit` % `line_limit` != 0 or
        #  when the count of record of the previous request
        #  is divisible by line_limit
        if line == "[]":
            break

        # stream data received, clean the data
        # the data received here is a string representation
        # of the json data
        line = line.strip(",[]")
        # convert the line to a dictionary and add it to line_accumulator
        line_accumulator.append(json.loads(line))

        # check if we have received enough lines
        # NOTE: if `len(line_accumulator)` < `line_limit` by the end of the for loop, that means we have exhausted the response
        #  See the corresponding conditional statement below for this case.
        if len(line_accumulator) == line_limit:
            # save the acculated lines
            save_streamed_lines(line_accumulator, file_name, append_flag)
            # clear the contents of line_accumulator for the next set of responses
            line_accumulator.clear()
    # end of for loop

    # add the remaining lines if there are any
    # this also means that there are no more stream data for the current request
    #  since the streamed data did not reach the line_limit
    # NOTE: Ending the request loop will only work correctly when request_limit % line_limit == 0
    #  This means if the request_limit is not divisible by line_limit, the request loop
    #  will terminate after the first request even if there is still data for our API request
    if len(line_accumulator) > 0:
        save_streamed_lines(line_accumulator, file_name, append_flag)
        line_accumulator.clear()

        # end the request loop
        return False

    # continue the request loop while we are still reaching the line limit
    return True


@op
def save_streamed_lines(lines: list[int], file_path: str, append_flag: bool) -> None:
    # convert line_accumulator to a dataframe
    df = pd.DataFrame.from_records(lines)
    # save the dataframe as csv
    # `append_flag` uses the offset variable from our request to determine the
    #  values for writing mode and header
    df.to_csv(
        file_path,
        mode="w" if append_flag else "a",
        header=append_flag,
        index=False,
    )


@op
def get_list_of_csv() -> list[str]:
    log = get_dagster_logger()
    CSV_FOLDER = os.path.join(os.path.dirname(__file__), "data/csv")
    log.info(CSV_FOLDER)
    csv_files = [
        file.path
        for file in os.scandir(CSV_FOLDER)
        if file.is_file() and file.name.endswith(".csv")
    ]

    log.info(csv_files)
    for csv in csv_files:
        log.info(f"File name: {csv}")

    return csv_files


@op
def verify_row_count(csv_files: list[str], v_count: int):
    log = get_dagster_logger()
    total_count = 0
    VERIFIED_COUNT = v_count
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        total_count += df.shape[0]
    log.info(f"Total rows of downloaded csv: {total_count}")
    assert total_count == VERIFIED_COUNT
