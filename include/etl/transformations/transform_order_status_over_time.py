import pandas as pd
import logging

from include.validations.order_status_schema import validate_output_order_status_schema
from include.etl.transformations.helpers import validate_required_columns

logger = logging.getLogger(__name__)


def transform_order_status_over_time(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Track how orders move through statuses (“Pending,” “Shipped,” “Returned”) by week.
    """
    logger.info("Starting order status breakdown by week...")

    required_cols = ["timestamp", "order_status"]
    validate_required_columns(enriched_df, required_cols, "order status over time")

    enriched_df["week"] = enriched_df["timestamp"].dt.to_period("W").dt.start_time

    grouped = (
        enriched_df.groupby(["week", "order_status"])
        .size()
        .reset_index(name="order_count")
    )

    pivoted_df = (
        grouped.pivot(index="week", columns="order_status", values="order_count")
        .fillna(0)
        .astype(int)
        .reset_index()
        .rename_axis(None, axis=1)
    )

    pivoted_df.columns = [col.title() if col != "week" else col for col in pivoted_df.columns]

    for status in ["Pending", "Shipped", "Returned"]:
        if status not in pivoted_df.columns:
            pivoted_df[status] = 0

    pivoted_df = pivoted_df[["week", "Pending", "Shipped", "Returned"]]

    logger.info("Weekly order status breakdown transformation complete.")
    return validate_output_order_status_schema(pivoted_df)