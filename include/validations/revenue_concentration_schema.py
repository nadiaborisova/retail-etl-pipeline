import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)


output_revenue_concentration_schema = pa.DataFrameSchema({
    "region": Column(str, required=True, nullable=False),
    "total_sales": Column(float, required=True, nullable=False),
    "revenue_share": Column(float, checks=pa.Check.ge(0.0)),
    "cumulative_share": Column(float, checks=[pa.Check.ge(0.0), pa.Check.le(1.0)], required=True, nullable=False)
})


def validate_output_revenue_concentration_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the schema of the revenue concentration analysis DataFrame.
    """
    try:
        return output_revenue_concentration_schema.validate(df)
    except SchemaError as err:
        logger.error("Revenue concentration schema validation failed.")
        logger.error(err.failure_cases)
        return df