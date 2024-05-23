from dagster import MonthlyPartitionsDefinition

MONTHLY_START = "2022-01-01"
MONTHLY_END = "2023-01-01"

monthly_partition = MonthlyPartitionsDefinition(
    start_date=MONTHLY_START, end_date=MONTHLY_END
)
