import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)


input_sales_schema = pa.DataFrameSchema({
    "sales_id": Column(int, nullable=True),
    "product_id": Column(int, nullable=True),
    "region": Column(str, nullable=True),
    "quantity": Column(int, nullable=True),
    "price": Column(float, nullable=True),
    "timestamp": Column(pa.DateTime, nullable=True),
    "discount": Column(float, nullable=True),
    "order_status": Column(str, nullable=True)
})


output_sales_schema = pa.DataFrameSchema({
    "sales_id": Column(int, Check.greater_than(0), required=True, nullable=False),
    "product_id": Column(int, Check.greater_than(0), required=True, nullable=False),
    "region": Column(str, required=True, nullable=False),
    "quantity": Column(int, Check.greater_than_or_equal_to(0), required=True, nullable=False),
    "price": Column(float, Check.greater_than_or_equal_to(0), required=True, nullable=False),
    "timestamp": Column(pa.DateTime, Check.less_than_or_equal_to(pd.Timestamp.now()), required=True, nullable=False),
    "discount": Column(float, Check.in_range(0.0, 1.0), required=True, nullable=False),
    "order_status": Column(str, Check.isin(["completed", "cancelled", "pending", "returned", "shipped"]), required=True, nullable=False),
})


def validate_input_sales_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate raw input sales data against a flexible schema.
    """
    try:
        return input_sales_schema.validate(df)
    except SchemaError as err:
        logger.error("Input sales schema validation failed.")
        logger.error(err.failure_cases)
        return df


def validate_output_sales_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate cleaned or transformed sales data against a strict schema.
    """
    try:
        return output_sales_schema.validate(df)
    except SchemaError as err:
        logger.error("Output sales schema validation failed.")
        logger.error(err.failure_cases)
        logger.error(err.schema)
        logger.error(err.data.head())
        return df