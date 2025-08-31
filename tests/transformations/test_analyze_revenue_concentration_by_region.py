import pandas as pd
import pytest

from include.etl.transformations.analyze_revenue_concentration_by_region import analyze_revenue_concentration_by_region

@pytest.fixture
def sample_enriched_df():
    return pd.DataFrame({
        "region": ["North", "North", "South", "East"],
        "total_sales": [1000.0, 500.0, 2000.0, 500.0]
    })

def test_analyze_revenue_concentration_by_region(sample_enriched_df):
    """Test revenue concentration analysis by region with correct share calculations."""
    df_region = analyze_revenue_concentration_by_region(sample_enriched_df)

    for col in ["region", "total_sales", "revenue_share", "cumulative_share"]:
        assert col in df_region.columns, f"{col} should exist in result DataFrame"

    assert df_region["region"].nunique() == 3

    north_sales = df_region[df_region["region"] == "North"]["total_sales"].iloc[0]
    assert north_sales == 1500.0

    total_sales_sum = df_region["total_sales"].sum()
    computed_shares = (df_region["total_sales"] / total_sales_sum).round(4).tolist()
    result_shares = df_region["revenue_share"].round(4).tolist()
    assert computed_shares == result_shares

    cumulative = df_region["cumulative_share"].tolist()
    assert cumulative == sorted(cumulative), "cumulative_share should be increasing"