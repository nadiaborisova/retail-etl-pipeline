import os
import pandas as pd
import yaml
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open("include/config.yaml", "r") as file:
    config = yaml.safe_load(file)


def load_folder_to_dict(folder_path: str, extensions=("csv", "json")) -> dict:
    """
    Loads CSV and/or JSON files from a folder into a dictionary of Pandas DataFrames.
    Supported extensions: .csv, .json
    """
    logger.info(f"Attempting to load files from: {folder_path}")
    if not os.path.exists(folder_path):
        logger.error(f"Directory not found: {folder_path}")
        raise FileNotFoundError(f"Directory does not exist: {folder_path}")

    data_dict = {}

    current_file = os.path.abspath(__file__)
    project_root = os.path.abspath(os.path.join(current_file, "..", "..", "..", ".."))
    data_folder = os.path.join(project_root, config['local']['folder'])

    for filename in os.listdir(folder_path):
        ext = filename.split(".")[-1].lower()
        if ext not in extensions:
            continue

        key = os.path.splitext(filename)[0]
        if not key:
            logger.warning(f"Skipping file with empty key: {filename}")
            continue

        file_path = os.path.join(data_folder, filename)

        try:
            if ext == "csv":
                df = pd.read_csv(file_path)
            elif ext == "json":
                df = pd.read_json(file_path, orient="records")

            data_dict[key] = df
            logger.info(f"Loaded {ext.upper()} file: {filename} (rows: {len(df)})")

        except Exception as e:
            logger.error(f"Error reading {filename}: {e}")

    return data_dict