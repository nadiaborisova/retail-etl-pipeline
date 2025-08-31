import pandas as pd

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def load_data_to_snowflake(df: pd.DataFrame, database: str, schema: str, table: str) -> None:
    """
    Load data to snowflake tables
    """
    if df.empty:
        raise ValueError("Empty Dataframe")

    snowflake_hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
    engine = snowflake_hook.get_sqlalchemy_engine()

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        index=False,
        if_exists="replace",
        method="multi",
    )