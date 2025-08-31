import pandas as pd
import pytest
from include.etl.transformations.generate_product_sales_performance import generate_product_sales_performance

@pytest.fixture
def sample_merged_df():
    return pd.DataFrame({
        "product_id": [101, 101, 102, 103],
        "category": ["electronics", "electronics", "furniture", "furniture"],
        "brand": ["APPLE", "APPLE", "IKEA", "IKEA"],
        "quantity": [5, 3, 10, 2],
        "total_sales": [5000.0, 3000.0, 20000.0, 8000.0],
        "rating": [4.5, 4.0, 3.5, 4.2]
    })

def test_generate_product_sales_performance(sample_merged_df):
    df_perf = generate_product_sales_performance(sample_merged_df)

    for col in ["total_revenue", "total_units_sold", "average_rating", "performance_tier"]:
        assert col in df_perf.columns, f"{col} should be present in the performance DataFrame"

    assert df_perf["product_id"].nunique() == 3

    row_101 = df_perf[df_perf["product_id"] == 101].iloc[0]
    assert row_101["total_revenue"] == 8000.0
    assert row_101["total_units_sold"] == 8
    assert abs(row_101["average_rating"] - 4.25) < 1e-6

    allowed_tiers = ["Low Performer", "Average", "Bestseller"]
    assert all(tier in allowed_tiers for tier in df_perf["performance_tier"])
