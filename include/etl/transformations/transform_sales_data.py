import pandas as pd
import logging

from include.validations.sales_schema import validate_input_sales_schema, validate_output_sales_schema
from include.etl.transformations.helpers import (
    standardize_column_names,
    safe_datetime_conversion,
    validate_required_columns,
    clean_string_columns
)

logger = logging.getLogger(__name__)


def transform_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw sales data.
    """
    logger.info("Starting sales data cleaning and transformation...")

    rename_map = {
        "qty": "quantity",
        "Time stamp": "timestamp"
    }
    sales_df = standardize_column_names(sales_df, rename_map)

    required_cols = ["sales_id", "product_id", "region", "quantity", "price", "timestamp", "order_status"]
    validate_required_columns(sales_df, required_cols, operation_name="sales data transformation")

    sales_df = sales_df[(sales_df["price"] > 0) & (sales_df["quantity"] > 0)].copy()

    sales_df = safe_datetime_conversion(sales_df, column="timestamp", datetime_format="%d-%m-%y %H:%M", errors="coerce")
    sales_df = sales_df[sales_df["timestamp"] <= pd.Timestamp.now()].copy()

    sales_df = validate_input_sales_schema(sales_df)

    sales_df = sales_df.dropna(subset=required_cols).copy()

    sales_df = clean_string_columns(sales_df, ["region", "order_status"], case="lower")

    valid_statuses = ["completed", "cancelled", "pending", "returned", "shipped"]
    sales_df = sales_df[sales_df["order_status"].isin(valid_statuses)].copy()
    logger.info(f"Unique statuses: {sales_df['order_status'].unique()}")

    if "discount" in sales_df.columns:
        sales_df.loc[:, "discount"] = sales_df["discount"].fillna(0.0)
    else:
        sales_df["discount"] = 0.0

    sales_df["total_sales"] = sales_df["quantity"] * sales_df["price"] * (1 - sales_df["discount"])

    sales_df = sales_df.drop_duplicates().copy()

    logger.info(f"Cleaning and transformation complete. Final row count: {len(sales_df)}")
    return validate_output_sales_schema(sales_df)
