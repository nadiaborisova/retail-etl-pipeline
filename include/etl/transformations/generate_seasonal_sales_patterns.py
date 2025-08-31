import pandas as pd
import logging

from include.validations.seasonal_sales_schema import validate_output_seasonal_sales_schema
from include.etl.transformations.helpers import validate_required_columns

logger = logging.getLogger(__name__)


def generate_seasonal_sales_patterns(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generating seasonal sales trends by quarter and product category.
    """
    logger.info("Generating seasonal sales patterns by quarter and category.")

    required_cols = ["timestamp", "category", "total_sales"]
    validate_required_columns(enriched_df, required_cols, "seasonal sales patterns")

    seasonal_df = (
        enriched_df
        .assign(quarter=enriched_df["timestamp"].dt.to_period("Q").astype(str))
        .groupby(["quarter", "category"], as_index=False)
        .agg(
            total_sales=("total_sales", "sum"),
            order_count=("total_sales", "size")  # counts rows = orders
        )
    )

    logger.info("Seasonal sales pattern generation complete.")
    return validate_output_seasonal_sales_schema(seasonal_df)