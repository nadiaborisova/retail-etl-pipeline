# RETAIL ETL PIPELINE

## Project Description

This project is an ETL pipeline for extracting, transforming, validating, and loading retail sales and product metadata. It uses Airflow (Astro CLI) for orchestration, Pandas for data transformation, Pandera for schema validation, and Snowflake as the destination data warehouse. The data source can be configured to read from S3 or local storage, with local set as the default.


## Prerequisites

- Python 3.8+
- Astro CLI (and Docker)
- Access to Snowflake instance
- AWS S3 access (if using S3 as data source)

> **Note:** Astro CLI requires Docker to function properly.

Astro CLI is a developer-friendly tool for running Apache Airflow locally, but it relies on Docker under the hood. When you run `astro dev start`, Astro CLI automatically builds Docker containers for Airflow components (webserver, scheduler, worker) and mounts your project inside them.


## Project Structure

```
retail_etl_project/
├── dags/
│   └── retail_etl_dag.py
├── include/
│   ├── config.yaml
│   ├── etl/
│   │   ├── extract/
│   │   ├── transformations/
│   │   ├── load/
│   └── validations/
│   └── tests/
│       └── transformations/
├── data/
│   └── RetailSourceFiles
├── requirements.txt
└── README.md
```


## Quick Start

### Phase 1: Project Setup and Virtual Environment

Before running the project, make sure you are in the project's root directory (retail_etl_project). Next, create and activate a virtual environment where all the required dependencies will be installed from requirements.txt. Using a virtual environment ensures that the project uses the exact package versions needed, isolated from your system-wide Python environment.

```bash
cd retail_etl_project
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### Phase 2: Configuration

The project supports extracting data either from local files (default) or from AWS S3. Specify the source in `config.yaml` as "local" or "S3":

```yaml
aws_conn_id: aws_conn_id

s3:
  bucket: <your-bucket-name>  # e.g., engineering-course
  folder: <your-bucket-folder>  # e.g., RetailSourceFiles/
  
local:
  folder: data/RetailSourceFiles/
  
source: local  # Change to "S3" when using S3

snowflake:
  conn_id: my_snowflake_conn
  account: <your-snowflake-account-name>
  database: <your-snowflake-database>
```

> **Note:** Update AWS credentials and Snowflake settings accordingly.

## Airflow Connections

This project requires Airflow connections for AWS (S3 access, if not using the local extraction) and Snowflake. You can set these connections via the Airflow UI as follows:

### 1. AWS Connection (for S3 access)
- **Connection ID:** `aws_conn_id` (matches config.yaml)
- **Connection Type:** Amazon Web Services
- **Login:** `<YOUR_ACCESS_KEY>`
- **Password:** `<YOUR_SECRET_KEY>`

### 2. Snowflake Connection
- **Connection ID:** `my_snowflake_conn` (matches config.yaml)
- **Connection Type:** Snowflake
- **Login:** `<YOUR_USERNAME>`
- **Password:** `<YOUR_PASSWORD>`
- **Extra Fields JSON:**
```json
{
  "account": "<YOUR_SNOWFLAKE_ACCOUNT>",
  "warehouse": "<YOUR_WAREHOUSE>",
  "database": "<YOUR_DATABASE>"
}
```

### Phase 3: Snowflake Setup

To prepare your Snowflake environment for the Retail ETL pipeline, run the following SQL commands in your Snowflake Worksheet. These commands will create the warehouse, database, role, schemas, tables and will grant appropriate privileges.
```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS RETAIL_DB;

CREATE ROLE etl_role;

-- Replace <your_snowflake_username> with your Snowflake username
GRANT ROLE etl_role TO USER <your_snowflake_username>;

USE ROLE etl_role;

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.CLEANSED_LAYER;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.BUSINESS_LAYER;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.PRESENTATION_LAYER;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE etl_role;
GRANT USAGE ON DATABASE RETAIL_DB TO ROLE etl_role;
GRANT USAGE ON SCHEMA RETAIL_DB.CLEANSED_LAYER TO ROLE etl_role;
GRANT USAGE ON SCHEMA RETAIL_DB.BUSINESS_LAYER TO ROLE etl_role;
GRANT USAGE ON SCHEMA RETAIL_DB.PRESENTATION_LAYER TO ROLE etl_role;

GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA RETAIL_DB.CLEANSED_LAYER TO ROLE etl_role;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA RETAIL_DB.BUSINESS_LAYER TO ROLE etl_role;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA RETAIL_DB.PRESENTATION_LAYER TO ROLE etl_role;

CREATE TABLE RETAIL_DB.CLEANSED_LAYER.SALES_DATA (
    sales_id    INT,
    product_id  INT,
    region      VARCHAR,
    quantity    INT,
    price       DOUBLE,
    timestamp   DATETIME,
    discount    DOUBLE,
    order_status VARCHAR
);

CREATE TABLE RETAIL_DB.CLEANSED_LAYER.PRODUCT_DATA (
    product_id  INT,
    category    VARCHAR,
    brand       VARCHAR,
    rating      DOUBLE,
    in_stock    BOOLEAN,
    launch_date DATETIME
);

CREATE TABLE RETAIL_DB.BUSINESS_LAYER.MERGED_SALES_PRODUCTS (
    sales_id       INT,
    product_id     INT,
    region         STRING,
    quantity       INT,
    price          FLOAT,
    timestamp      DATETIME,
    discount       FLOAT,
    order_status   STRING,
    category       STRING,
    brand          STRING,
    rating         FLOAT,
    in_stock       BOOLEAN,
    launch_date    DATETIME
);

CREATE TABLE RETAIL_DB.BUSINESS_LAYER.ENRICHED_SALES_PRODUCTS (
    sales_id       INT,
    product_id     INT,
    region         STRING,
    quantity       INT,
    price          FLOAT,
    timestamp      DATETIME,
    discount       FLOAT,
    order_status   STRING,
    category       STRING,
    brand          STRING,
    rating         FLOAT,
    in_stock       BOOLEAN,
    launch_date    DATETIME,
    month          STRING,
    weekday        STRING,
    hour           INT,
    sales_bucket   STRING
);

CREATE TABLE RETAIL_DB.PRESENTATION_LAYER.SALES_HOURLY_TRENDS (
    region      STRING,
    category    STRING,
    peak_hour   INT,
    max_sales   FLOAT
);

CREATE TABLE RETAIL_DB.PRESENTATION_LAYER.PRODUCTS_SALES_PERFORMANCE (
    product_id         INT,
    category           STRING,
    brand              STRING,
    total_revenue      FLOAT,
    total_units_sold   INT,
    average_rating     FLOAT,
    performance_tier   STRING
);

CREATE TABLE RETAIL_DB.PRESENTATION_LAYER.SEASONAL_SALES_PATTERNS (
    quarter      STRING,
    category     STRING,
    total_sales  FLOAT,
    order_count  INT
);

CREATE TABLE RETAIL_DB.PRESENTATION_LAYER.REVENUE_CONCENTRATION_ANALYSIS (
    region            STRING,
    total_sales       FLOAT,
    revenue_share     FLOAT,
    cumulative_share  FLOAT
);

CREATE TABLE RETAIL_DB.PRESENTATION_LAYER.ORDER_STATUS_OVER_TIME (
    week     DATE,
    pending  INT,
    shipped  INT,
    returned INT
);
```


## Usage

1. Start Airflow:
   ```bash
   astro dev start
   ```

2. Access Airflow UI at `http://localhost:8080`

3. Configure connections as described above

4. Trigger the ETL pipeline from the Airflow UI


## Testing
The project includes comprehensive unit tests to ensure data transformation reliability and code quality. The test suite covers all transformation functions.

Make sure your virtual environment is activated and you're in the project root (retail_etl_project). Then run:

```bash
pytest /tests/transformations/ -v
```
    
>**Note:** If pytest is not installed, you can install it via:

```bash
pip install pytest
```



