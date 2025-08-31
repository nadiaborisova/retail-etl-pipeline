import pytest
import pandas as pd

from include.etl.transformations.merge_sales_and_products import merge_sales_and_products
from include.etl.transformations.transform_products_data import transform_products_data
from include.etl.transformations.transform_sales_data import transform_sales_data


def sample_sales_df():
    """Create a sample sales DataFrame matching the input schema."""
    return pd.DataFrame({
        "sales_id": [1, 2, 3],
        "product_id": [101, 102, 103],
        "region": ["north", "south", "east"],
        "quantity": [2, 1, 5],
        "price": [50.0, 100.0, 20.0],
        "timestamp": pd.to_datetime(["2025-01-01 12:00", "2025-01-02 13:00", "2025-01-03 14:00"]),
        "discount": [0.0, 0.1, 0.0],
        "order_status": ["completed", "cancelled", "pending"]
    })

def sample_products_df():
    """Create a sample products DataFrame matching the input schema."""
    return pd.DataFrame({
        "product_id": [101, 102, 103],
        "category": ["electronics", "clothing", "books"],
        "brand": ["Sony", "Nike", "Penguin"],
        "rating": [4.5, 4.0, 5.0],
        "in_stock": [True, True, False],
        "launch_date": pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01"])
    })


def test_merge_sales_and_products_success():
    """Test that merge_sales_and_products returns correct merged DataFrame."""
    sales_df = transform_sales_data(sample_sales_df())
    products_df = transform_products_data(sample_products_df())
    merged = merge_sales_and_products(sales_df, products_df)
    assert "product_id" in merged.columns
    assert "sales_id" in merged.columns
    assert len(merged) <= len(sales_df)

def test_merge_sales_and_products_inner_join():
    """Test that only matching product_ids are merged."""
    sales_df = transform_sales_data(sample_sales_df())
    products_df = transform_products_data(sample_products_df())
    products_df = products_df[products_df["product_id"] != 102]
    merged = merge_sales_and_products(sales_df, products_df)
    assert 102 not in merged["product_id"].values

