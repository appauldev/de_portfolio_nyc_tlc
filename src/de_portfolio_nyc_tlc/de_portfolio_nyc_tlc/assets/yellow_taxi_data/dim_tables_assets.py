from dagster import MaterializeResult, asset


@asset
def dim_date() -> MaterializeResult:

    return MaterializeResult(metadata={})
