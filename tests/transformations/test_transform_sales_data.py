import pytest
import pandas as pd

from include.etl.transformations.transform_sales_data import transform_sales_data

def sample_sales_df():
    """Creating sample DataFrame for testing sales transformations."""
    return pd.DataFrame({
        "sales_id": [1, 2, 3, 4],
        "product_id": [101, 102, 103, 104],
        "region": ["North ", "south", "EAST", "west"],
        "quantity": [2, 0, 3, 1],
        "price": [50, 100, 0, 20],
        "timestamp": ["01-01-25 12:00", "02-01-25 13:00", "03-01-25 14:00", "04-01-25 15:00"],
        "order_status": ["Completed", "Cancelled", "pending", "Returned"],
        # discount
    })

def test_required_columns_present():
    """Check that all required columns exist in the transformed sales DataFrame."""
    df = sample_sales_df()
    transformed = transform_sales_data(df)
    expected_cols = ["sales_id", "product_id", "region", "quantity", "price",
                     "timestamp", "order_status", "discount", "total_sales"]
    assert set(expected_cols).issubset(transformed.columns)


def test_lowercase_region_order_status():
    """Ensure 'region' and 'order_status' columns are converted to lowercase."""
    df = sample_sales_df()
    transformed = transform_sales_data(df)
    assert all(transformed["region"] == transformed["region"].str.lower())
    assert all(transformed["order_status"] == transformed["order_status"].str.lower())


def test_filter_zero_quantity_or_price():
    """Check that rows with zero 'quantity' or 'price' are removed."""
    df = sample_sales_df()
    transformed = transform_sales_data(df)
    assert (transformed["quantity"] > 0).all()
    assert (transformed["price"] > 0).all()


def test_discount_column_added():
    """Verify that the 'discount' column is added and filled with 0.0 if missing."""
    df = sample_sales_df()
    transformed = transform_sales_data(df)
    assert (transformed["discount"] == 0.0).all()


def test_total_sales_calculation():
    """Validate that 'total_sales' is correctly calculated as quantity * price * (1 - discount)."""
    df = sample_sales_df()
    transformed = transform_sales_data(df)
    for idx, row in transformed.iterrows():
        expected_total = row["quantity"] * row["price"] * (1 - row["discount"])
        assert row["total_sales"] == expected_total


def test_missing_required_columns():
    """Check that a ValueError is raised if any required column is missing."""
    df = pd.DataFrame({
        "sales_id": [1],
        "product_id": [101],
        # "region" column missing
        "quantity": [2],
        "price": [50],
        "timestamp": ["01-01-25 12:00"],
        "order_status": ["completed"]
    })

    with pytest.raises(ValueError, match="Missing required columns"):
        transform_sales_data(df)