from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import StringIO
import logging
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

def load_s3_files_to_dict(config: dict, extensions=("csv", "json")) -> dict:
    """
    Load CSV and JSON files from S3 using Airflow's S3Hook.
    """

    aws_conn_id = config.get("aws_conn_id", "aws_conn_id")
    bucket = config.get("bucket")
    prefix = config.get("prefix", "")

    if not bucket:
        raise AirflowException("S3 bucket not specified in config")

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
    if not keys:
        logger.warning(f"No files found in bucket '{bucket}' with prefix '{prefix}'")
        return {}

    data_dict = {}

    for key in keys:
        if not key.lower().endswith(extensions):
            continue

        file_name = key.split("/")[-1]
        file_key, ext = file_name.rsplit(".", 1)
        ext = ext.lower()

        try:
            file_obj = s3_hook.get_key(key, bucket_name=bucket)
            content = file_obj.get()['Body'].read().decode("utf-8")

            if ext == "csv":
                df = pd.read_csv(StringIO(content))
            elif ext == "json":
                df = pd.read_json(StringIO(content), orient="records")
            else:
                logger.warning(f"Unsupported file extension '{ext}' for file '{file_name}', skipping.")
                continue

            data_dict[file_key] = df
            logger.info(f"Loaded '{file_name}' from S3 (rows: {len(df)})")

        except Exception as e:
            logger.error(f"Failed to load '{file_name}' from S3: {e}")
            raise AirflowException(f"Failed to load '{file_name}' from S3: {e}")

    if not data_dict:
        logger.warning(f"No valid files with extensions {extensions} found in S3 bucket '{bucket}' with prefix '{prefix}'")

    return data_dict