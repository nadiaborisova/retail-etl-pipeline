import pandas as pd
import logging

from include.validations.enriched_schema import validate_output_enrich_schema
from include.etl.transformations.helpers import (
    add_temporal_features,
    create_sales_buckets,
    log_dataframe_info,
    validate_required_columns
)

logger = logging.getLogger(__name__)


def enrich_merged_data(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriching merged data by adding temporal and categorical features.
    """
    logger.info("Starting data enrichment process")

    required_columns = ['timestamp', 'total_sales']
    validate_required_columns(merged_df, required_columns)

    enriched_df = merged_df.copy()
    enriched_df = add_temporal_features(enriched_df, "timestamp")
    enriched_df = create_sales_buckets(enriched_df, "total_sales")

    log_dataframe_info(enriched_df, "Data enrichment completed")

    return validate_output_enrich_schema(enriched_df)