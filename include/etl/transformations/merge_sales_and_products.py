import pandas as pd
import logging

from include.validations.merged_schema import validate_output_merge_schema
from include.etl.transformations.helpers import (
    validate_dataframes_for_merge,
    check_merge_compatibility,
    analyze_merge_quality
)

logger = logging.getLogger(__name__)


def merge_sales_and_products(sales_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge sales and products DataFrames on the 'product_id' column using an inner join.
    """
    logger.info("Starting merge of sales and products data")

    validate_dataframes_for_merge(sales_df, products_df, "product_id", "sales", "products")
    check_merge_compatibility(sales_df, products_df, "product_id", "products")

    logger.info(f"Sales DataFrame: {len(sales_df)} rows, {sales_df['product_id'].nunique()} unique products")
    logger.info(f"Products DataFrame: {len(products_df)} rows, {products_df['product_id'].nunique()} unique products")

    merged_df = sales_df.merge(products_df, on="product_id", how="inner", validate="m:1")

    quality_analysis = analyze_merge_quality(sales_df, products_df, merged_df, "product_id", "sales_id")
    logger.info(f"Merge completed: {quality_analysis['output_merged_count']} rows "
                f"({quality_analysis['merge_ratio']:.1f}% of original sales)")

    if quality_analysis['merge_ratio'] < 90:
        logger.warning(f"Low merge ratio ({quality_analysis['merge_ratio']:.1f}%). Check for mismatched product_ids")

    return validate_output_merge_schema(merged_df)