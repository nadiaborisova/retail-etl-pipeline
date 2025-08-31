import logging
import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaError

from pandera.pandas import Column, Check

logger = logging.getLogger(__name__)

output_sales_trends_schema = pa.DataFrameSchema({
    "region": Column(str, required=True, nullable=False),
    "category": Column(str, required=True, nullable=False),
    "peak_hour": Column(int, Check.in_range(0, 23), required=True, nullable=False),
    "max_sales": Column(float, Check.greater_than_or_equal_to(0), required=True, nullable=False),
})

def validate_output_sales_trends_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the schema of the hourly sales trends DataFrame.
    """
    try:
        return output_sales_trends_schema.validate(df)
    except SchemaError as err:
        logger.error("Enriched data schema validation failed.")
        logger.error(err.failure_cases)
        return df