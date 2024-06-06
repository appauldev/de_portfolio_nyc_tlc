from dagster import MetadataValue
from duckdb import DuckDBPyConnection


def get_table_metadata(conn: DuckDBPyConnection, table_name: str, limit=20):
    conn.sql(
        f"""--sql
                SELECT * FROM {table_name} LIMIT {limit}
            """
    ).show()
    result = conn.sql(
        f"""--sql
            SELECT * FROM {table_name} LIMIT {limit}
        """
    ).to_df()
    print(result.dtypes)

    # Metadata
    count_total_records = (
        conn.sql(
            f"""--sql
        SELECT COUNT(*) AS total_count FROM {table_name}
        """
        )
        .to_df()
        .at[0, "total_count"]
    )

    return {"Count of total records": MetadataValue.text(f"{count_total_records:,}")}
