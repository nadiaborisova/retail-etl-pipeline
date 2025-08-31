import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError
from pandera.dtypes import Timestamp

logger = logging.getLogger(__name__)

output_status_over_time_schema = pa.DataFrameSchema({
    "week": Column(pa.Timestamp, required=True, nullable=False),
    "Pending": Column(int, required=True),
    "Shipped": Column(int, required=True),
    "Returned": Column(int, required=True),
})


def validate_output_order_status_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the order status breakdown schema.
    """
    try:
        return output_status_over_time_schema.validate(df)
    except SchemaError as err:
        logger.error("Order status schema validation failed.")
        logger.error(err.failure_cases)
        return df