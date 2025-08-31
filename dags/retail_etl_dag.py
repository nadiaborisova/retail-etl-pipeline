import os
import yaml
import logging
import pandas as pd

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from pendulum import datetime

from include.etl.extract.extract_from_local import load_folder_to_dict
from include.etl.extract.extract_from_s3 import load_s3_files_to_dict
from include.etl.load.load_data import load_data_to_snowflake

from include.etl.transformations.transform_sales_data import transform_sales_data
from include.etl.transformations.transform_products_data import transform_products_data
from include.etl.transformations.enrich_merged_data import enrich_merged_data
from include.etl.transformations.merge_sales_and_products import merge_sales_and_products
from include.etl.transformations.generate_hourly_sales_trends import generate_hourly_sales_trends
from include.etl.transformations.generate_product_sales_performance import generate_product_sales_performance
from include.etl.transformations.generate_seasonal_sales_patterns import generate_seasonal_sales_patterns
from include.etl.transformations.analyze_revenue_concentration_by_region import analyze_revenue_concentration_by_region
from include.etl.transformations.transform_order_status_over_time import transform_order_status_over_time


logger = logging.getLogger(__name__)

current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", ".."))
config_path = os.path.join(project_root, "include", "config.yaml")

with open(config_path, "r") as file:
    config = yaml.safe_load(file)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["extract", "transform", "etl"]
)
def retail_etl_dag():

    @task()
    def extract_data(config: dict) -> dict:
        """
        Extracts raw data files from a configured data source.

        By default, the function uses the 'local' source if the 'source' key is not provided in the config.

        Supported sources:
        - 'local': Loads files from a local directory specified in config['local']['folder'].
        - 's3': Loads files from an S3 bucket and folder using configuration under config['s3'].
        """

        source = config.get("source", "local")

        if source == "local":
            data_folder = os.path.join(project_root, config["local"]["folder"])
            if not os.path.exists(data_folder):
                logger.error(f"Local folder not found: {data_folder}")
                raise AirflowException(f"Local folder not found: {data_folder}")
            return load_folder_to_dict(data_folder, extensions=("csv", "json"))

        elif source == "S3":
            try:
                return load_s3_files_to_dict(config["s3"], extensions=("csv", "json"))
            except Exception as e:
                logger.error(f"Failed to load files from S3: {e}")
                raise AirflowException(f"Failed to load files from S3: {e}")

        else:
            logger.error(f"Unsupported data source: {source}")
            raise AirflowException(f"Unsupported data source: {source}")

    @task()
    def transform_sales(files: dict) -> pd.DataFrame:
        """
        Transforms the raw sales data after extraction.
        """
        sales_raw = files.get("sales_data")
        if sales_raw is None:
            logger.error("Sales data not found in extracted files.")
            raise AirflowException("Sales data not found in extracted files.")
        sales_df = pd.read_csv(sales_raw) if isinstance(sales_raw, str) else sales_raw
        transformed_df = transform_sales_data(sales_df)
        logger.info(f"Columns after transformation: {list(transformed_df.columns)}")
        return transformed_df

    @task()
    def transform_products(files: dict) -> pd.DataFrame:
        """
        Transform the raw product data from input files into a cleaned DataFrame.
        """
        products_raw = files.get("product_data")
        if products_raw is None:
            logger.error("Products data not found in extracted files.")
            raise AirflowException("Products data not found in extracted files.")
        products_df = pd.read_csv(products_raw) if isinstance(products_raw, str) else products_raw
        return transform_products_data(products_df)

    @task()
    def merge_data(sales_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge sales data with product data into a single DataFrame.
        """
        logger.info("Completed merge_data task.")
        return merge_sales_and_products(sales_df, products_df)

    @task()
    def enrich_data(merged_df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich the merged DataFrame with additional computed or lookup information.
        """
        logger.info("Completed enrich_data task.")
        return enrich_merged_data(merged_df)

    @task()
    def hourly_sales_trends(enriched_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate hourly sales trend metrics from the enriched sales data.
        """
        logger.info("Completed hourly_sales_trends task.")
        return generate_hourly_sales_trends(enriched_df)

    @task()
    def product_performance_task(merged_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate product sales performance metrics from merged sales and product data.
        """
        logger.info("Completed product_performance_task.")
        return generate_product_sales_performance(merged_df)

    @task()
    def seasonal_sales_task(enriched_df: pd.DataFrame) -> pd.DataFrame:
        """
        Task to analyze seasonal sales patterns by quarter and category.
        """
        logger.info("Completed seasonal_sales_task.")
        return generate_seasonal_sales_patterns(enriched_df)

    @task()
    def revenue_concentration_task(enriched_df: pd.DataFrame) -> pd.DataFrame:
        """
        Task to compute revenue concentration and inequality metrics by region.
        """
        logger.info("Completed revenue_concentration_task.")
        return analyze_revenue_concentration_by_region(enriched_df)

    @task()
    def order_status_analysis_task(enriched_df: pd.DataFrame) -> pd.DataFrame:
        """
        Airflow task to analyze weekly order status breakdown.
        """
        try:
            return transform_order_status_over_time(enriched_df)
        except Exception as e:
            logger.error(f"Failed to compute order status breakdown: {e}")
            raise AirflowException(f"Order status breakdown failed: {e}")

    @task()
    def load_to_snowflake_task(df: pd.DataFrame, database: str, schema_name: str, table_name: str) -> None:
        logger.info(f"Loading DataFrame to {database}.{schema_name}.{table_name}")
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"First 5 rows:\n{df.head().to_string()}")
        load_data_to_snowflake(df=df, database=database, schema=schema_name, table=table_name)

    with TaskGroup("extraction") as extraction:
        extracted_files = extract_data(config)

    with TaskGroup("transformation") as transformation:
        sales_df = transform_sales(extracted_files)
        products_df = transform_products(extracted_files)
        merged_df = merge_data(sales_df, products_df)
        enriched_df = enrich_data(merged_df)

    with TaskGroup("analysis") as analysis:
        sales_trends_df = hourly_sales_trends(enriched_df)
        product_perf_df = product_performance_task(enriched_df)
        seasonal_trends_df = seasonal_sales_task(enriched_df)
        revenue_concentration_df = revenue_concentration_task(enriched_df)
        order_status_df = order_status_analysis_task(enriched_df)

    with TaskGroup("loading") as loading:
        tables_config = {
            "load_sales": ("sales_df", "sales"),
            "load_product": ("products_df", "product"),
            "load_merged_sales_products": ("merged_df", "merged_sales_products"),
            "load_enriched_sales": ("enriched_df", "enriched_sales_products"),
            "load_hourly_sales_trends": ("sales_trends_df", "sales_hourly_trends"),
            "load_product_performance": ("product_perf_df", "products_sales_performance"),
            "load_seasonal_sales_patterns": ("seasonal_trends_df", "seasonal_sales_patterns"),
            "load_revenue_concentration_analysis": ("revenue_concentration_df", "revenue_concentration_analysis"),
            "load_order_status_over_time": ("order_status_df", "order_status_over_time"),
        }

        for task_id, (df_name, target_key) in tables_config.items():
            df = locals()[df_name]
            target_conf = config["snowflake"]["targets"][target_key]

            load_to_snowflake_task.override(task_id=task_id)(
                df=df,
                database=config["snowflake"]["database"],
                schema_name=target_conf["schema"],
                table_name=target_conf["table"]
            )


retail_etl_dag()