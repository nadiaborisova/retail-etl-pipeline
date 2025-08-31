import pytest
import pandas as pd
from include.etl.transformations.transform_products_data import transform_products_data

def sample_products_df():
    """Sample DataFrame for testing product transformations."""
    return pd.DataFrame({
        "product_id": [101, 102, 103, 104],
        "category": ["Electronics ", "furniture", "Toys", None],
        "brand": ["apple", "IKEA", "LEGO", "nike"],
        "rating": [4.5, 4.0, 3.5, 5.0],
        "in_stock": [10, 0, 5, 3],
        "launch_date": ["2025-01-01", "2025-02-01", "2025-03-01", "2025-04-01"]
    })

def test_required_columns_present():
    """Check that all required columns exist in the transformed products DataFrame."""
    df = sample_products_df()
    transformed = transform_products_data(df)
    expected_cols = ["product_id", "category", "brand", "rating", "in_stock", "launch_date"]
    assert set(expected_cols).issubset(transformed.columns)


def test_clean_string_columns():
    """Ensure 'category' is lowercase and 'brand' is uppercase after transformation."""
    df = sample_products_df()
    transformed = transform_products_data(df)
    assert all(transformed["category"] == transformed["category"].str.lower())
    assert all(transformed["brand"] == transformed["brand"].str.upper())


def test_drop_na_rows():
    """Check that rows with missing required columns are dropped."""
    df = sample_products_df()
    transformed = transform_products_data(df)
    assert transformed["category"].isna().sum() == 0


def test_drop_duplicates():
    """Verify that duplicate rows are removed in the transformed DataFrame."""
    df = sample_products_df()
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    transformed = transform_products_data(df)
    assert len(transformed) == len(df.dropna(subset=["product_id", "category", "brand", "rating", "in_stock", "launch_date"]).drop_duplicates())


def test_missing_required_columns():
    """Check that a ValueError is raised if any required column is missing."""
    df = pd.DataFrame({
        "product_id": [101],
        # "category" missing
        "brand": ["Apple"],
        "rating": [4.5],
        "in_stock": [10],
        "launch_date": ["2025-01-01"]
    })
    with pytest.raises(ValueError, match="Missing required columns"):
        transform_products_data(df)

