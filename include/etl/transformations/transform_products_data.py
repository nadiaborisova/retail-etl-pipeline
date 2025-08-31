import pandas as pd
import logging

from include.validations.products_schema import validate_output_products_schema, validate_input_products_schema
from include.etl.transformations.helpers import (
    standardize_column_names,
    safe_datetime_conversion,
    validate_required_columns,
    clean_string_columns
)

logger = logging.getLogger(__name__)

def transform_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Applying transformations to the input products DataFrame.
    """
    logger.info("Starting transformation of products data.")

    products_df = standardize_column_names(products_df)
    products_df = safe_datetime_conversion(products_df, column="launch_date", errors="coerce")

    products_df = validate_input_products_schema(products_df)

    required_cols = ["product_id", "category", "brand", "rating", "in_stock", "launch_date"]
    validate_required_columns(products_df, required_cols, operation_name="products data transformation")
    products_df = products_df.dropna(subset=required_cols).copy()

    products_df = clean_string_columns(products_df, {"category": "lower", "brand": "upper"})

    products_df = products_df.drop_duplicates()

    logger.info(f"Products transformation complete. Final row count: {len(products_df)}")
    return validate_output_products_schema(products_df)