import logging
import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaError

from pandera.pandas import Column, Check

logger = logging.getLogger(__name__)

output_enrich_schema = pa.DataFrameSchema({
    "sales_id": Column("int64", required=True, nullable=False),
    "product_id": Column("int64", required=True, nullable=False),
    "region": Column(str, required=True, nullable=False),
    "quantity": Column("int64", required=True, nullable=False),
    "price": Column(float, required=True, nullable=False),
    "timestamp": Column(pa.DateTime, required=True, nullable=False),
    "discount": Column(float, required=True, nullable=True),
    "order_status": Column(str, required=True, nullable=False),
    "category": Column(str, Check(lambda s: s.str.islower()), required=True, nullable=False),
    "brand": Column(str, Check(lambda s: s.str.isupper()), required=True, nullable=False),
    "rating": Column(float, required=True, nullable=False),
    "in_stock": Column(bool, required=True, nullable=False),
    "launch_date": Column(pa.DateTime, required=True, nullable=False),
    "month": Column(str, required=True, nullable=False),
    "weekday": Column(str, required=True, nullable=False),
    "hour": Column("int64", required=True, nullable=False),
    "sales_bucket": Column(str, required=True, nullable=False),
})


def validate_output_enrich_schema(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the output schema of the enriched DataFrame using Pandera.
    """
    try:
        return output_enrich_schema.validate(merged_df)
    except SchemaError as err:
        logger.error("Enriched data schema validation failed.")
        logger.error(f"Schema errors: {err.failure_cases}")
        logger.error(f"DataFrame dtypes:\n{merged_df.dtypes}")
        return merged_df