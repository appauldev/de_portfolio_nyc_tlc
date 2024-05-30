import json
from pathlib import Path

from httpx import Response
import pandas as pd

from ....utils.log_utils import log_w_header
from ....partitions import monthly_partition


def get_monthly_range(start_date: str) -> tuple[str, str]:
    end_date: str = (
        monthly_partition.get_next_partition_key(start_date)
        if start_date.split("-")[1] != "12"
        else "2023-01-01"
    )

    return (start_date, end_date)


def create_file_save_path(start_date: str, csv_data_dir: Path) -> str:
    if not csv_data_dir.exists():
        csv_data_dir.mkdir(parents=True, exist_ok=True)

    month_num = start_date.split("-")[1]
    CSV_FILE_PATH = Path(f"{csv_data_dir}/2022-{month_num}.csv")

    print(f"file save path: {CSV_FILE_PATH}")

    return str(CSV_FILE_PATH)


async def handle_stream_data_response(
    response: Response,
    response_limit: int,
    accumulator_limit: int,
    file_name: str,
    total_records_saved: int,
):
    """
    Handles the response stream of the request. Returns a tuple that contains
    `(1) the boolean value to determine if the main request loop should continue`, and
    `(2) the number of saved rows from the handled request loop`
    """
    # Check for exceptions
    response.raise_for_status()

    # save every N lines, where N = accumulator_limit
    line_accumulator = []
    # for logging
    partition_name = file_name.split("/")[-1]
    async for line in response.aiter_lines():
        # check for empty response, and end the request loop when there are no more rows to fetch
        if line == "[]":
            return False, total_records_saved

        # stream data received, clean the data
        # the data received here is a string representation of the json data
        line = line.strip(",[]")
        # convert the line to a dictionary and add it to line_accumulator
        line_accumulator.append(json.loads(line))

        # save the accumulated lines
        if len(line_accumulator) == accumulator_limit:
            # add csv header and append to the end of file when there are existing records already
            append_flag = True if total_records_saved != 0 else False
            save_streamed_lines(line_accumulator, file_name, append_flag)
            # clear the contents of line_accumulator for the next set of responses
            line_accumulator.clear()
            # log progress
            total_records_saved += accumulator_limit
            log_w_header(f"{partition_name}: Saved {total_records_saved} rows", ".")
    # end of for loop

    # add the remaining lines if there are any
    # this also means that there are no more stream data for the current request
    #  since the streamed data did not reach the accumulator_limit
    # NOTE: Ending the request loop will only work correctly when request_limit % accumulator_limit == 0
    #  This means if the request_limit is not divisible by accumulator_limit, the request loop
    #  will terminate after the first request even if there is still data for our API request
    get_line_accumulator_length = len(line_accumulator)
    if get_line_accumulator_length > 0:
        append_flag = True if total_records_saved != 0 else False
        save_streamed_lines(line_accumulator, file_name, append_flag)
        line_accumulator.clear()
        # log
        total_records_saved += get_line_accumulator_length
        log_w_header(
            f"{partition_name}: Saved {total_records_saved} rows after last set of records",
            ".",
        )

        # end the request loop
        if response_limit % accumulator_limit == 0:
            return False, total_records_saved

    # continue the request loop while we are still reaching the line limit
    return True, total_records_saved


def save_streamed_lines(lines: list[int], file_path: str, append_flag: bool) -> None:
    # convert line_accumulator to a dataframe
    df = pd.DataFrame.from_records(lines)
    # save the dataframe as csv. `append_flag` uses the total_saved_records to determine the values for writing mode and header
    df.to_csv(
        file_path,
        mode="a" if append_flag else "w",
        header=not append_flag,
        index=False,
    )
