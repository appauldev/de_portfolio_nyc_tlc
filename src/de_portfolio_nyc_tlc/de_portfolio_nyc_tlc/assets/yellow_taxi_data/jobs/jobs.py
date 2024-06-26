from dagster import job
from ..ops.csv_assets_ops import get_list_of_csv, verify_row_count


@job
def check_row_count():
    csv_list = get_list_of_csv()
    verify_row_count(csv_list)
