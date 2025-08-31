import pandas as pd
import logging

from include.validations.revenue_concentration_schema import validate_output_revenue_concentration_schema
from include.etl.transformations.helpers import validate_required_columns, calculate_percentage_share

logger = logging.getLogger(__name__)


def analyze_revenue_concentration_by_region(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzing revenue concentration and inequality by region.
    """
    logger.info("Analyzing revenue concentration by region.")

    required_cols = ["region", "total_sales"]
    validate_required_columns(enriched_df, required_cols, "revenue concentration analysis")

    region_df = (
        enriched_df
        .groupby("region", as_index=False)["total_sales"]
        .sum()
        .sort_values(by="total_sales", ascending=False)
    )

    region_df = calculate_percentage_share(
        region_df,
        value_col="total_sales",
        share_col="revenue_share",
        cumulative_col="cumulative_share"
    )

    logger.info("Revenue concentration analysis complete.")
    return validate_output_revenue_concentration_schema(region_df)