import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)

input_products_schema = pa.DataFrameSchema({
    "product_id": Column(int, nullable=True),
    "category": Column(str, nullable=True),
    "brand": Column(str, nullable=True),
    "rating": Column(float, nullable=True),
    "in_stock": Column(bool, nullable=True),
    "launch_date": Column(pa.DateTime, nullable=True)
})

output_products_schema = pa.DataFrameSchema({
    "product_id": Column(int, Check.greater_than(0), required=True, nullable=False),
    "category": Column(str, Check(lambda s: s.str.islower()), required=True, nullable=False),
    "brand": Column(str, Check(lambda s: s.str.isupper().all()), required=True, nullable=False),
    "rating": Column(float, Check.in_range(0.0, 5.0), required=True, nullable=False),
    "in_stock": Column(bool, required=True, nullable=False),
    "launch_date": Column(pa.DateTime, Check.less_than_or_equal_to(pd.Timestamp.today()), required=True, nullable=False),
})

def validate_input_products_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate input product DataFrame against the input schema.
    Allows nullable values for some fields and is used for early validation.
    """
    try:
        validated_df = input_products_schema.validate(df)
        logger.info("Input products schema validation succeeded.")
        return validated_df
    except SchemaError as err:
        logger.error("Input schema validation failed.", exc_info=True)
        return df

def validate_output_products_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate processed product DataFrame against the output schema.
    Enforces stricter checks like positive IDs, valid rating range, and launch dates.
    """
    try:
        validated_df = output_products_schema.validate(df)
        logger.info("Output products schema validation succeeded.")
        return validated_df
    except SchemaError as err:
        logger.error("Output schema validation failed.")
        logger.error(err.failure_cases)
        logger.error(err.schema)
        logger.error(err.data.head())
        return df