"""
Helper functions for ETL operations.
"""
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Union

logger = logging.getLogger(__name__)


def standardize_column_names(df: pd.DataFrame, rename_map: Dict[str, str] = None) -> pd.DataFrame:
    """
    Standardize DataFrame column names by applying optional rename map and cleaning.
    Ensures lowercase, no leading/trailing spaces, spaces replaced by underscores.
    """
    if rename_map:
        df = df.rename(columns=rename_map)

    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r'\s+', '_', regex=True)
    )
    return df


def clean_string_columns(df: pd.DataFrame, columns: Union[List[str], Dict[str, str]],
                         case: str = "lower") -> pd.DataFrame:
    """
    Clean string columns by stripping whitespace and normalizing case.
    """
    df_copy = df.copy()

    if isinstance(columns, dict):
        for col, col_case in columns.items():
            if col in df_copy.columns:
                series = df_copy[col].astype(str).str.strip()
                if col_case == "lower":
                    series = series.str.lower()
                elif col_case == "upper":
                    series = series.str.upper()
                df_copy.loc[:, col] = series
    else:
        for col in columns:
            if col in df_copy.columns:
                series = df_copy[col].astype(str).str.strip()
                if case == "lower":
                    series = series.str.lower()
                elif case == "upper":
                    series = series.str.upper()
                df_copy.loc[:, col] = series

    return df_copy


def validate_required_columns(df: pd.DataFrame, required_cols: List[str],
                              operation_name: str = "operation") -> None:
    """
    Validate that all required columns are present in the DataFrame.
    """
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logger.error(f"Missing required columns for {operation_name}: {missing}")
        raise ValueError(f"Missing required columns for {operation_name}: {missing}")


def filter_positive_values(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Filter DataFrame to keep only rows with positive values in specified columns.
    Uses .loc and returns a copy to avoid SettingWithCopyWarning.
    """
    condition = pd.Series(True, index=df.index)
    for col in columns:
        if col in df.columns:
            condition &= df[col] > 0
        else:
            logger.warning(f"Column {col} not found for positive value filtering")

    filtered_df = df.loc[condition].copy()
    logger.info(f"Filtered to {len(filtered_df)} rows with positive values in {columns}")
    return filtered_df


def safe_datetime_conversion(df: pd.DataFrame, column: str, datetime_format: str = None,
                             errors: str = 'coerce') -> pd.DataFrame:
    """
    Safely convert a column to datetime.
    """
    df_copy = df.copy()

    if column in df_copy.columns:
        try:
            if datetime_format:
                df_copy[column] = pd.to_datetime(df_copy[column], format=datetime_format, errors=errors)
            else:
                df_copy[column] = pd.to_datetime(df_copy[column], errors=errors)

            logger.info(f"Successfully converted {column} to datetime")
        except Exception as e:
            logger.error(f"Failed to convert {column} to datetime: {e}")
            return df
    else:
        logger.warning(f"Column '{column}' not found for datetime conversion")

    return df_copy


def validate_dataframes_for_merge(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        join_key: str,
        left_name: str = "left",
        right_name: str = "right"
) -> None:
    """
    Generic validation for DataFrame merge operations.
    """
    if left_df.empty:
        raise ValueError(f"{left_name.capitalize()} DataFrame is empty")
    if right_df.empty:
        raise ValueError(f"{right_name.capitalize()} DataFrame is empty")

    if join_key not in left_df.columns:
        raise ValueError(f"Missing '{join_key}' column in {left_name} DataFrame")
    if join_key not in right_df.columns:
        raise ValueError(f"Missing '{join_key}' column in {right_name} DataFrame")

    left_null_count = left_df[join_key].isnull().sum()
    right_null_count = right_df[join_key].isnull().sum()

    if left_null_count > 0:
        logger.warning(f"Found {left_null_count} null {join_key} values in {left_name} data")
    if right_null_count > 0:
        logger.warning(f"Found {right_null_count} null {join_key} values in {right_name} data")


def check_merge_compatibility(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        join_key: str,
        right_name: str = "right"
) -> None:
    """
    Check for potential issues with merge operation.
    """
    duplicate_count = right_df[join_key].duplicated().sum()
    if duplicate_count > 0:
        logger.error(f"Found {duplicate_count} duplicate {join_key} values in {right_name} DataFrame")
        raise ValueError(f"{right_name.capitalize()} DataFrame contains duplicate {join_key} values")

    left_keys = set(left_df[join_key].dropna())
    right_keys = set(right_df[join_key].dropna())

    missing_keys = left_keys - right_keys
    if missing_keys:
        logger.warning(f"Found {len(missing_keys)} {join_key} values in left that don't exist in {right_name}")
        sample_missing = list(missing_keys)[:5]
        logger.warning(f"Sample missing {join_key} values: {sample_missing}")


def analyze_merge_quality(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        merged_df: pd.DataFrame,
        join_key: str,
        left_id_col: Optional[str] = None
) -> dict:
    """
    Analyze the quality and completeness of merge operation.
    """
    analysis = {
        "input_left_count": len(left_df),
        "input_right_count": len(right_df),
        "output_merged_count": len(merged_df),
        "merge_ratio": len(merged_df) / len(left_df) * 100 if len(left_df) > 0 else 0,
        "unique_keys_in_left": left_df[join_key].nunique(),
        "unique_keys_in_right": right_df[join_key].nunique(),
        "keys_successfully_merged": merged_df[join_key].nunique()
    }

    if left_id_col and left_id_col in left_df.columns and left_id_col in merged_df.columns:
        merged_ids = set(merged_df[left_id_col])
        original_ids = set(left_df[left_id_col])
        analysis["lost_records_count"] = len(original_ids - merged_ids)

    return analysis

def filter_valid_categorical_values(df: pd.DataFrame, column: str,
                                    valid_values: List[str]) -> pd.DataFrame:
    """
    Filter DataFrame to keep only rows with valid categorical values.
    Returns a filtered DataFrame (not in-place, since it's a row filter).
    """
    if column not in df.columns:
        logger.warning(f"Column {column} not found for categorical filtering")
        return df

    filtered_df = df[df[column].isin(valid_values)]
    unique_values = df[column].unique()

    logger.info(f"Filtered {column} to valid values: {valid_values}")
    logger.info(f"Unique values found: {unique_values}")

    return filtered_df


def add_temporal_features(df: pd.DataFrame, timestamp_col: str) -> pd.DataFrame:
    """
    Add common temporal features from a timestamp column.
    """
    if timestamp_col not in df.columns:
        logger.error(f"Timestamp column {timestamp_col} not found")
        raise ValueError(f"Timestamp column {timestamp_col} not found")

    df_copy = df.copy()

    df_copy["month"] = df_copy[timestamp_col].dt.to_period('M').astype(str)
    df_copy["weekday"] = df_copy[timestamp_col].dt.day_name()
    df_copy["hour"] = df_copy[timestamp_col].dt.hour.astype('int64')

    logger.info(f"Added temporal features from {timestamp_col}")

    return df_copy


def create_sales_buckets(df: pd.DataFrame, sales_col: str,
                           bins: List[float] = None,
                           labels: List[str] = None) -> pd.DataFrame:
    """
    Create revenue/sales buckets from a continuous revenue column.
    """
    if sales_col not in df.columns:
        logger.error(f"Sales column '{sales_col}' not found")
        raise ValueError(f"Sales column '{sales_col}' not found")

    # Default bins and labels
    if bins is None:
        bins = [0, 100, 500, float("inf")]

    if labels is None:
        labels = ["Low", "Medium", "High"]

    if len(labels) != len(bins) - 1:
        raise ValueError(f"Number of labels ({len(labels)}) must be one less than number of bins ({len(bins)})")

    df_copy = df.copy()

    df_copy["sales_bucket"] = pd.cut(
        df_copy[sales_col],
        bins=bins,
        labels=labels,
        include_lowest=True
    ).astype(str)

    logger.info(f"Created sales_bucket from '{sales_col}' with {len(labels)} categories")

    return df_copy


def log_dataframe_info(df: pd.DataFrame, operation_name: str,
                       show_preview: bool = True, preview_rows: int = 3) -> None:
    """
    Log comprehensive DataFrame information.
    """
    logger.info(f"{operation_name} - Shape: {df.shape}")

    if show_preview and len(df) > 0:
        logger.info(f"{operation_name} - Preview:\n{df.head(preview_rows).to_string(index=False)}")

    logger.info(f"{operation_name} - Data types:\n{df.dtypes}")


def calculate_percentage_share(
        df: pd.DataFrame,
        value_col: str,
        group_cols: List[str] = None,
        share_col: str = "share",
        cumulative_col: str = "cumulative_share"
) -> pd.DataFrame:
    """
    Calculate percentage share and cumulative share for a value column.
    """
    if value_col not in df.columns:
        logger.error(f"Value column {value_col} not found")
        raise ValueError(f"Value column {value_col} not found")

    df_copy = df.copy()

    if group_cols:
        total = df_copy.groupby(group_cols)[value_col].transform('sum')
    else:
        total = df_copy[value_col].sum()

    df_copy[share_col] = df_copy[value_col] / total

    if group_cols:
        df_copy[cumulative_col] = (
            df_copy.groupby(group_cols)[share_col].cumsum()
        )
    else:
        df_copy = df_copy.sort_values(by=value_col, ascending=False)
        df_copy[cumulative_col] = df_copy[share_col].cumsum()

    logger.info(f"Added percentage shares for {value_col} "
                f"(columns: {share_col}, {cumulative_col})")

    return df_copy
