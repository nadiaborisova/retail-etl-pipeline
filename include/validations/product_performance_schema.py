import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)

output_product_performance_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "category": Column(str),
    "brand": Column(str),
    "total_revenue": Column(float, Check.ge(0)),
    "total_units_sold": Column(int, Check.ge(0)),
    "average_rating": Column(float, Check.in_range(0, 5)),
    "performance_tier": Column(pa.Category, checks=Check.isin(["Low Performer", "Average", "Bestseller"]))
})


def validate_output_product_performance_schema(performance_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the product performance DataFrame against the expected output schema.
    """
    try:
        return output_product_performance_schema.validate(performance_df)
    except SchemaError as err:
        logger.error("Product performance schema validation failed.")
        logger.error(err.failure_cases)
        return performance_df