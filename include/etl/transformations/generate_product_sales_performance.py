import pandas as pd
import logging

from include.validations.product_performance_schema import validate_output_product_performance_schema
from include.etl.transformations.helpers import validate_required_columns

logger = logging.getLogger(__name__)


def generate_product_sales_performance(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generating product sales ranking and performance categorization based on revenue and sales volume.
    """
    logger.info("Generating product sales performance data.")

    required_columns = ["product_id", "quantity", "total_sales", "category", "brand", "rating"]
    validate_required_columns(merged_df, required_columns, "product sales performance")

    performance_df = (
        merged_df
        .groupby(["product_id", "category", "brand"], as_index=False)
        .agg(
            total_revenue=("total_sales", "sum"),
            total_units_sold=("quantity", "sum"),
            average_rating=("rating", "mean")
        )
    )

    performance_df["performance_tier"] = pd.cut(
        performance_df["total_revenue"],
        bins=[0, 20000, 50000, float("inf")],
        labels=["Low Performer", "Average", "Bestseller"]
    )

    logger.info(f"Generated performance data for {len(performance_df)} products.")
    return validate_output_product_performance_schema(performance_df)