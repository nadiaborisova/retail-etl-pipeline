import pandas as pd
import logging

from include.validations.sales_trends_schema import validate_output_sales_trends_schema
from include.etl.transformations.helpers import validate_required_columns

logger = logging.getLogger(__name__)


def generate_hourly_sales_trends(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzing hourly sales trends to identify the peak sales hour for each region and category.
    """
    logger.info("Generating hourly sales trends by region and category.")

    required_columns = ["region", "category", "hour", "total_sales"]
    validate_required_columns(merged_df, required_columns, "hourly sales trends")

    grouped = (
        merged_df
        .groupby(["region", "category", "hour"], as_index=False)["total_sales"]
        .sum()
    )

    peak_trends_df = (
        grouped.sort_values(["region", "category", "total_sales"], ascending=[True, True, False])
        .groupby(["region", "category"], as_index=False)
        .first()
        .rename(columns={
            "hour": "peak_hour",
            "total_sales": "max_sales"
        })
    )

    logger.info(f"Generated {len(peak_trends_df)} peak trend rows.")
    return validate_output_sales_trends_schema(peak_trends_df)