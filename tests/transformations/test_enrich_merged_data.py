import pytest
import pandas as pd

from include.etl.transformations.enrich_merged_data import enrich_merged_data

@pytest.fixture
def valid_sample_data():
    return pd.DataFrame({
        "sales_id": [1, 2],
        "product_id": [101, 102],
        "region": ["US", "EU"],
        "quantity": [5, 3],
        "price": [100.0, 150.0],
        "timestamp": pd.to_datetime(["2025-08-01 12:00:00", "2025-08-02 15:30:00"]),
        "discount": [0.0, 0.0],
        "order_status": ["shipped", "pending"],
        "category": ["electronics", "furniture"],
        "brand": ["BRAND1", "BRAND2"],
        "rating": [4.5, 3.0],
        "in_stock": [True, False],
        "launch_date": pd.to_datetime(["2025-01-01", "2025-02-15"]),
        "total_sales": [50.0, 1000.0]
    })

def test_enrich_merged_data_transformation(valid_sample_data):
    df_enriched = enrich_merged_data(valid_sample_data)

    for col in ["month", "weekday", "hour", "sales_bucket"]:
        assert col in df_enriched.columns, f"{col} should be present in enriched DataFrame"

    assert df_enriched["hour"].tolist() == [12, 15]
    assert df_enriched["month"].tolist() == ["2025-08", "2025-08"]

    allowed_buckets = ["Low", "Medium", "High"]
    assert all(bucket in allowed_buckets for bucket in df_enriched["sales_bucket"]), \
        "All sales_bucket values should be one of Low, Medium, High"

    assert df_enriched.shape[0] == valid_sample_data.shape[0]

