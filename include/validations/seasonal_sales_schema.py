import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)


output_seasonal_sales_schema = pa.DataFrameSchema({
    "quarter": Column(str, required=True, nullable=False),
    "category": Column(str, required=True, nullable=False),
    "total_sales": Column(float, required=True, nullable=False),
    "order_count": Column(int, required=True, nullable=False),
})


def validate_output_seasonal_sales_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the seasonal sales pattern DataFrame against the expected schema.
    """
    try:
        return output_seasonal_sales_schema.validate(df)
    except SchemaError as err:
        logger.error("Schema validation failed for seasonal sales patterns.")
        logger.error(err.failure_cases)
        return df
