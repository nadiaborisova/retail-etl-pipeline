import logging
import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaError

from pandera.pandas import Column, Check

logger = logging.getLogger(__name__)

output_merged_schema = pa.DataFrameSchema({
    "sales_id": Column(int, required=True, nullable=False),
    "product_id": Column(int, required=True, nullable=False),
    "region": Column(str, required=True, nullable=False),
    "quantity": Column(int, required=True, nullable=False),
    "price": Column(float, required=True, nullable=False),
    "timestamp": Column(pa.DateTime, required=True, nullable=False),
    "discount": Column(float, required=True, nullable=False),
    "order_status": Column(str, required=True, nullable=False),
    "category": Column(str, Check(lambda s: s.str.islower()), required=True, nullable=False),
    "brand": Column(str, Check(lambda s: s.str.isupper()), required=True, nullable=False),
    "rating": Column(float, required=True, nullable=False),
    "in_stock": Column(bool, required=True, nullable=False),
    "launch_date": Column(pa.DateTime, required=True, nullable=False)
})


def validate_output_merge_schema(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the schema of the merged sales and products DataFrame using Pandera.
    """
    try:
        validated_df = output_merged_schema.validate(merged_df, lazy=True)
        logger.info("Merged data schema validation succeeded")
        return validated_df
    except SchemaError as err:
        logger.error("Merged data schema validation failed")
        logger.error(f"Failure cases:\n{err.failure_cases}")
        return merged_df