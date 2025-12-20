## Conda Environments

 - Install conda environments required for project &  Run the following command:

`conda env create --file prj-environment.yml`

`conda activate prj`

# Extract
Prerequisites
1. Kaggle API Key: Go to Kaggle Settings -> Create New Token. Save kaggle.json.

2. GCP Service Account: Go to GCP Console -> IAM -> Service Accounts -> Create Key (JSON). Save as gcp_key json.

3. GCS Bucket: Create a bucket (e.g., my-olist-raw-data) in Google Cloud Storage.

### Creat a bucket in google cloud storage
- bucketname:  ecommerce-olist

  Grant acces  storage storage object admin using IAM

### Saving kaggle key

 `mkdir -p ~/.kaggle`

 `cp kaggle/kaggle.json ~/.kaggle/kaggle.json`

### Execute Extract & load.
- Update the configuration according to your credentials in ingest_olist.py
```Config
   # --- CONFIGURATION ---
   KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
   LOCAL_DOWNLOAD_PATH = "./temp_data"
   GCS_BUCKET_NAME = "ecommerce-olist"    # Your GCS Bucket Name
   GCS_DESTINATION_FOLDER = "raw/"      # Folder inside the bucket
   GCP_KEY_PATH = "/Users/IvanHan/DSAI-Group2-project/olist-group-2-8f8d5aa4ae8b.json"              # Path to your Service Account Key
```


- Run below command in terminal.

  `python ingest_olist.py`
- Check the Google Storage whether the files are uploaded. 

![alt text](image-3.png)


# Load
### Load data from GCS to BigQuery using Meltano dbt
- Run below commands in terminal.

  `meltano init`
( Enter the project name as Melt). It will create a folder \Melt

  `cd Melt`

  `meltano invoke dbt-bigquery:initialize`

- It creates transform/dbt_project.yml.

- It creates transform/profiles/bigquery/profiles.yml.

- It sets up the basic folder structure (models, seeds, etc.).

- Right-click transform/ -> New File -> Name it packages.yml. (If package.yml is not available in \Melt\transform)

Paste this content into transform/packages.yml:
```packages
packages:
  - package: dbt-labs/dbt_external_tables
    version: 0.12.0
```
Paste below content into transform/dbt_project.yml:
```
name: my_meltano_project
version: '1.0'
profile: meltano
config-version: 2
require-dbt-version: [">=1.0.0", "<2.0.0"]
flags:
  send_anonymous_usage_stats: False
  use_colors: True
  partial_parse: true
model-paths:
- models
analysis-paths:
- analysis
test-paths:
- tests
seed-paths:
- data
macro-paths:
- macros
snapshot-paths:
- snapshots
target-path: target
log-path: logs
packages-install-path: dbt_packages
clean-targets:
- target
- dbt_packages
- logs
models:
  my_meltano_project: null

```

Create transform/profiles/bigquery/profiles.yml( if not exists)

Paste below content into transform/profiles/bigquery/profiles.yml:
- Replace 'project: olist-group-2' With your bigquery project id
- Replace "keyfile: /Users/IvanHan/DSAI-Group2-project/olist-group-2-8f8d5aa4ae8b.json"  With path of your google cloud key json file
```
# transform/profiles/bigquery/profiles.yml
meltano:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      # These env_vars are standard for Meltano dbt-bigquery
      project: olist-group-2
      dataset: dbt_dev
      keyfile: /Users/IvanHan/DSAI-Group2-project/olist-group-2-8f8d5aa4ae8b.json
      threads: 1
      timeout_seconds: 300
      location: asia-southeast1
      priority: interactive
```
#### Verification
Once you have done either Solution 1 or 2, verify the fix by running:

`meltano invoke dbt-bigquery:debug`

If you see "All checks passed!", you are ready to proceed next step

#### Install the dependencies: Run this command in your terminal to download that package

  `meltano invoke dbt-bigquery:deps`

Success Indicator: You should see a message saying: Installing dbt-labs/dbt_external_tables Installed from version 0.12.0

Update only below block in /Melt/meltano.yml

Replace keyfile: olist-group-2-8f8d5aa4ae8b.json with Google cloud keyfile 

Replace project: olist-group-2 with  your bigquery project

```
....
plugins:
  utilities:
  - name: dbt-bigquery
    variant: dbt-labs
    pip_url: dbt-core dbt-bigquery meltano-dbt-ext~=0.3.0
    config:
      keyfile: olist-group-2-8f8d5aa4ae8b.json  ##<- Replace your google cloud json
      project: olist-group-2  ## <- Replace your bigquery project
      dataset: dbt_dev
    commands:
      run-operation:
      args: run-operation
      description: Run a specific dbt operation macro  
```
#### Define your Sources 
create Melt/transform/models/staging/src_olist.yml (if not exist)
(make sure to replace the projectid)


```
version: 2

sources:
  - name: olist_raw
    database: olist-group-2 # <--- Replace with your GCP Project ID
    schema: ecommerce_data          # The dataset in BigQuery
    loader: gcs_external
    
    tables:
      # 1. Orders
      - name: orders
        description: "Raw orders data from GCS"
        external:
          location: "gs://olist-group-2/ecommerce_data/raw-orders.csv"
          options:
            format: csv
            skip_leading_rows: 1
            allow_jagged_rows: true
            quote: '"'

      # 2. Customers (Repeat for other files in your screenshot)
      - name: customers
        external:
          location: "gs://olist-group-2/ecommerce_data/raw-customers.csv"
          options:
            format: csv
            skip_leading_rows: 1
      
      # 3. Geolocation (Repeat for other files in your screenshot)
      - name: geolocation
        external:
          location: "gs://olist-group-2/ecommerce_data/raw-geolocation.csv"
          options:
            format: csv
            skip_leading_rows: 1

      # 4. Order_items (Repeat for other files in your screenshot)
      - name: order_items
        external:
          location: "gs://olist-group-2/ecommerce_data/raw-order_items.csv"
          options:
            format: csv
            skip_leading_rows: 1

      # 5. Payments (Repeat for other files in your screenshot)
      - name: payments
        external:
          location: "gs://olist-group-2/ecommerce_data/raw-payments.csv"
          options:
            format: csv
            skip_leading_rows: 1

      # 6. Reviews (Repeat for other files in your screenshot)
      - name: reviews
        external:
          location: "gs://olist-group-2/ecommerce_data/raw-reviews.csv"
          options:
            format: csv
            skip_leading_rows: 1

      # 7. Products (Repeat for other files in your screenshot)
      - name: products
        external:
          location: "gs://olist-group-2/ecommerce_data/raw-products.csv"
          options:
            format: csv
            skip_leading_rows: 1

      # 8. Sellers (Repeat for other files in your screenshot)
      - name: sellers
        external:
          location: "gs://olist-group-2/ecommerce_data/raw-sellers.csv"
          options:
            format: csv
            skip_leading_rows: 1

      # 9. Product Category Name Translation (Repeat for other files in your screenshot)
      - name: category_translation
        external:
          location: "gs://olist-group-2/ecommerce_data/raw-category_translation.csv"
          options:
            format: csv
            skip_leading_rows: 1
```

#### Run the load
Finally Run this command.  this will load the data to bigQuery

`meltano invoke dbt-bigquery:run-operation stage_external_sources --args "{select: olist_raw}"`


# DBT Setup
### Step 1: Initialize the Project
  Navigate to your preferred directory and create a new project.
    Enter the following commend in the terminal. ( ensure the you are in parent drectory)

  `md dbt`

  `cd dbt`

  `dbt init olist_analytics`
  
      - Database: Choose bigquery.

      - Auth: Choose oauth (recommended for local dev) or service_account.

      - Project ID: Enter your GCP Project ID.

      - Dataset: Enter the name of the target dataset where dbt will build your models (e.g., analytics or dbt_prod).

Response
----------
Setting up your profile.

Which database would you like to use?
[1] bigquery

(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)

Enter a number: 1

/Users/IvanHan/miniconda3/envs/prj/lib/python3.11/site-packages/google/cloud/aiplatform/models.py:52: FutureWarning: Support for google-cloud-storage < 3.0.0 will be removed in a future version of google-cloud-aiplatform. Please upgrade to google-cloud-storage >= 3.0.0.

  from google.cloud.aiplatform.utils import gcs_utils

[1] oauth

[2] service_account

Desired authentication method option (enter a number): 1

project (GCP project id): olist-group-2

dataset (the name of your dbt dataset): olist_raw

threads (1 or more): 1

job_execution_timeout_seconds [300]: 

[1] US

[2] EU

Desired location option (enter a number): 1

14:06:18  Profile olist_analytics written to /Users/IvanHan/.dbt/profiles.yml using target's profile_template.yml and 
your supplied values. Run 'dbt debug' to validate the connection.

-----------

### Step 2: Define Your Sources (Staging Layer)
We need to tell dbt where your raw Meltano data lives.

1. Create a file models/staging/sources.yml.

2. Define your raw tables (adjust database and schema to match your BigQuery setup).

Copy-paste the below content in to source.yml

```
version: 2

sources:
  - name: olist_raw
    database: olist-group-2 # <--- Replace with your GCP Project ID
    schema: olist_raw # Where Meltano loaded the data
    tables:
      - name: orders
      - name: order_items
      - name: products
      - name: customers
      - name: sellers
      - name: geolocation
      - name: payments
      - name: reviews
      - name: category_translation
```
### Step 3: Create Staging Models
Copy-paste the Sql below files in  .model\staging\ folder 

![alt text](image.png)

Once you have created these 9 files, run the following command in your terminal to verify they all work:

`dbt run --select staging`

The results should be as below 

----
13:26:46  1 of 4 START sql view model dbt_dev.stg_customers .............................. [RUN]
13:26:47  1 of 4 OK created sql view model dbt_dev.stg_customers ......................... [CREATE VIEW (0 processed) in 1.08s]
13:26:47  2 of 4 START sql view model dbt_dev.stg_order_payments ......................... [RUN]
13:26:48  2 of 4 OK created sql view model dbt_dev.stg_order_payments .................... [CREATE VIEW (0 processed) in 1.09s]
13:26:48  3 of 4 START sql view model dbt_dev.stg_product_category_name_translation ...... [RUN]
13:26:50  3 of 4 OK created sql view model dbt_dev.stg_product_category_name_translation . [CREATE VIEW (0 processed) in 1.78s]
13:26:50  4 of 4 START sql view model dbt_dev.stg_products ............................... [RUN]
13:26:52  4 of 4 OK created sql view model dbt_dev.stg_products .......................... [CREATE VIEW (0 processed) in 1.85s]
13:26:52
13:26:52  Finished running 4 view models in 0 hours 0 minutes and 6.63 seconds (6.63s).
13:26:52
13:26:52  Completed successfully
13:26:52
13:26:52  Done. PASS=4 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=4

-----

### Step 4: Create Dimension and fact Models (Marts Layer)
Copy-paste the Sql below files in  .model\marts\ folder 

Step 1: Create the marts Folder

Open your Terminal (where your prj conda environment is active).
Navigate to your dbt project's models directory:
bash
'''
cd Melt/transform/models
Create the marts folder:
bash
'''
mkdir marts
Verify: You can use ls to check:
bash
'''
ls

Step 2: Place Your Dimension and Fact Model Files

Now that you have the marts folder, you'll place the SQL files for your dimension and fact models inside it.
Navigate into the new marts folder:
bash
'''
cd marts
Create the SQL files for each of your dimension and fact models:


![alt text](image-1.png)

### Step 5: Update dbt_project
- Open dbt_project.yml
- Find the models: section at the bottom of the file.

```

# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: 'olist_analytics'
version: '1.0.0'

# This setting configures which "profile" dbt uses for this project.
profile: 'olist_analytics'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/
# directory as views. These settings can be overridden in the individual model
# files using the `{{ config(...) }}` macro.
models:
  olist_analytics:
    # Config indicated by + and applies to all files under models/example/
    #example:
    #  +materialized: view
    staging:
      # Staging models are lightweight cleaning steps.
      # We keep them as 'views' so they always reflect fresh raw data 
      # and don't take up extra storage.
      +materialized: view
      +schema: staging
    marts:
      # Marts (Facts & Dims) are queried frequently by BI tools.
      # We materialize them as 'tables' to improve query performance
      # and reduce BigQuery compute costs for end-users.
      +materialized: table
      +schema: analytics
```
### Step 6: Rundbt
`dbt run`

-----
16:13:20  1 of 16 START sql table model olist_raw_analytics.dim_date ..................... [RUN]
16:13:23  1 of 16 OK created sql table model olist_raw_analytics.dim_date ................ [CREATE TABLE (1.5k rows, 0 processed) in 2.81s]
16:13:23  2 of 16 START sql view model olist_raw_staging.stg_customers ................... [RUN]
16:13:24  2 of 16 OK created sql view model olist_raw_staging.stg_customers .............. [CREATE VIEW (0 processed) in 1.31s]
16:13:24  3 of 16 START sql view model olist_raw_staging.stg_geolocation ................. [RUN]
16:13:26  3 of 16 OK created sql view model olist_raw_staging.stg_geolocation ............ [CREATE VIEW (0 processed) in 1.39s]
16:13:26  4 of 16 START sql view model olist_raw_staging.stg_order_items ................. [RUN]
16:13:27  4 of 16 OK created sql view model olist_raw_staging.stg_order_items ............ [CREATE VIEW (0 processed) in 1.44s]
16:13:27  5 of 16 START sql view model olist_raw_staging.stg_order_payments .............. [RUN]
16:13:29  5 of 16 OK created sql view model olist_raw_staging.stg_order_payments ......... [CREATE VIEW (0 processed) in 1.62s]
16:13:29  6 of 16 START sql view model olist_raw_staging.stg_order_reviews ............... [RUN]
16:13:30  6 of 16 OK created sql view model olist_raw_staging.stg_order_reviews .......... [CREATE VIEW (0 processed) in 1.43s]
16:13:30  7 of 16 START sql view model olist_raw_staging.stg_orders ...................... [RUN]
16:13:32  7 of 16 OK created sql view model olist_raw_staging.stg_orders ................. [CREATE VIEW (0 processed) in 1.69s]
16:13:32  8 of 16 START sql view model olist_raw_staging.stg_product_category_name_translation  [RUN]
16:13:33  8 of 16 OK created sql view model olist_raw_staging.stg_product_category_name_translation  [CREATE VIEW (0 processed) in 1.18s]
16:13:33  9 of 16 START sql view model olist_raw_staging.stg_products .................... [RUN]
16:13:34  9 of 16 OK created sql view model olist_raw_staging.stg_products ............... [CREATE VIEW (0 processed) in 1.52s]
16:13:34  10 of 16 START sql view model olist_raw_staging.stg_sellers .................... [RUN]
16:13:36  10 of 16 OK created sql view model olist_raw_staging.stg_sellers ............... [CREATE VIEW (0 processed) in 1.23s]
16:13:36  11 of 16 START sql table model olist_raw_analytics.dim_customers ............... [RUN]
16:13:40  11 of 16 OK created sql table model olist_raw_analytics.dim_customers .......... [CREATE TABLE (96.1k rows, 8.6 MiB processed) in 4.40s]
16:13:40  12 of 16 START sql table model olist_raw_analytics.dim_geolocation ............. [RUN]
16:13:48  12 of 16 OK created sql table model olist_raw_analytics.dim_geolocation ........ [CREATE TABLE (19.0k rows, 58.4 MiB processed) in 7.78s]
16:13:48  13 of 16 START sql table model olist_raw_analytics.fct_order_items ............. [RUN]
16:13:55  13 of 16 OK created sql table model olist_raw_analytics.fct_order_items ........ [CREATE TABLE (112.7k rows, 31.6 MiB processed) in 6.90s]
16:13:55  14 of 16 START sql table model olist_raw_analytics.fct_orders .................. [RUN]
16:14:00  14 of 16 OK created sql table model olist_raw_analytics.fct_orders ............. [CREATE TABLE (99.4k rows, 16.8 MiB processed) in 4.77s]
16:14:00  15 of 16 START sql table model olist_raw_analytics.dim_products ................ [RUN]
16:14:05  15 of 16 OK created sql table model olist_raw_analytics.dim_products ........... [CREATE TABLE (33.0k rows, 2.3 MiB processed) in 4.91s]
16:14:05  16 of 16 START sql table model olist_raw_analytics.dim_sellers ................. [RUN]
16:14:08  16 of 16 OK created sql table model olist_raw_analytics.dim_sellers ............ [CREATE TABLE (3.1k rows, 170.5 KiB processed) in 3.27s]
16:14:08  
16:14:08  Finished running 7 table models, 9 view models in 0 hours 0 minutes and 51.38 seconds (51.38s).
16:14:08  
16:14:08  Completed successfully
16:14:08  
16:14:08  Done. PASS=16 WARN=0 ERROR=0 SKIP=0 TOTAL=16
------
creates dim and fact table in bigQuery
![alt text](image-2.png)

## Great Expectations
import great_expectations as gx
import os

# 1. SETUP CONTEXT
# ----------------
# GX 1.0+ handles context creation similarly, but the internals are different.
context = gx.get_context()

# 2. DEFINE CONNECTION VARIABLES
# ------------------------------
project_id = "olist-group-2"
dataset_name = "olist_raw_analytics"
table_name = "fct_order_items"
# Ensure the key file is in your current directory
key_path = "olist-group-2-8f8d5aa4ae8b.json" 

# Connection string (Same as before)
connection_string = f"bigquery://{project_id}/{dataset_name}?credentials_path={key_path}"

# 3. CONNECT TO BIGQUERY (Updated for GX 1.0+)
# --------------------------------------------
datasource_name = "my_bigquery_source"

# Try to get it; if it doesn't exist, create it.
try:
    datasource = context.data_sources.get(datasource_name)
    print(f"Datasource '{datasource_name}' found.")
except:
    print(f"Datasource '{datasource_name}' not found. Creating it...")
    datasource = context.data_sources.add_sql(
        name=datasource_name, 
        connection_string=connection_string
    )

# 4. DEFINE ASSET & BATCH DEFINITION (New API)
# --------------------------------------------
# In GX 1.0, you must explicitly define *how* you want to batch the data 
# (e.g., "Whole Table" or "By Year"). Here we define a "Whole Table" batch.
asset_name = f"{table_name}_asset"
batch_definition_name = "full_table_batch"

# Add Asset
try:
    asset = datasource.get_asset(asset_name)
except LookupError:
    asset = datasource.add_table_asset(
        name=asset_name, 
        table_name=table_name
    )

# Add Batch Definition (The new critical step)
try:
    batch_definition = asset.get_batch_definition(batch_definition_name)
except LookupError:
    # This defines a batch that simply reads the whole table
    batch_definition = asset.add_batch_definition(name=batch_definition_name)

# 5. CREATE EXPECTATION SUITE (New API)
# -------------------------------------
suite_name = "order_items_quality_suite"
try:
    suite = context.suites.get(suite_name)
except:
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

# 6. GET VALIDATOR
# ----------------
# FIX: context.get_validator requires a 'batch_request', not a 'batch_definition'.
# We use .build_batch_request() to convert it.

validator = context.get_validator(
    batch_request=batch_definition.build_batch_request(),
    expectation_suite_name=suite_name
)

print("\n--- Starting Validation Checks (GX 1.4+) ---")

# 7. ADD CHECKS (Standard Logic)
# ------------------------------
# These commands haven't changed much, but they now attach to the Suite automatically.

print(f"Checking {table_name}: order_id cannot be null...")
validator.expect_column_values_to_not_be_null(column="order_id")

print(f"Checking {table_name}: price must be > 0...")
validator.expect_column_values_to_be_between(
    column="price", 
    min_value=0, 
    strict_min=True  # <--- The correct parameter name
)

print(f"Checking {table_name}: freight_value must be >= 0...")
validator.expect_column_values_to_be_between(
    column="freight_value", 
    min_value=0
)

# Save the suite logic
context.suites.add_or_update(validator.expectation_suite)

# 8. RUN CHECKPOINT
# -----------------
# We create a simple Checkpoint definition to capture the results
checkpoint_name = "my_bq_checkpoint"

# In GX 1.0, Checkpoints validate a specific Batch Definition against a Suite
checkpoint = context.checkpoints.add_or_update(
    gx.Checkpoint(
        name=checkpoint_name,
        validation_definitions=[
            gx.ValidationDefinition(
                name="my_validation_def",
                data=batch_definition,
                suite=suite
            )
        ]
    )
)

print("Running Checkpoint...")
checkpoint_result = checkpoint.run()

# 9. BUILD AND OPEN RESULTS
# -------------------------
# FIX: Manually build the HTML docs before opening them
print("Building Data Docs...")
context.build_data_docs()

context.open_data_docs()

## Data Analytics and visualization
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account

# --- 1. PAGE CONFIGURATION ---
st.set_page_config(page_title="Executive Command Center", layout="wide", initial_sidebar_state="collapsed")

# --- CUSTOM CSS FOR "PRESENTATION MODE" (LARGE FONTS) ---
st.markdown("""
    <style>
    /* 1. Global Text Size */
    html, body, [class*="css"]  {
        font-family: 'Sans Serif';
    }
    
    /* 2. TAB TITLES (The clickable buttons) */
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 24px !important;
        font-weight: bold !important;
    }
    
    /* 3. METRIC CARDS (Top Row) */
    [data-testid="stMetricValue"] {
        font-size: 40px !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 18px !important;
        font-weight: bold !important;
    }

    /* 4. HEADERS */
    h1 { font-size: 48px !important; }
    h2 { font-size: 36px !important; }
    h3 { font-size: 28px !important; }
    
    /* 5. NORMAL TEXT */
    .stMarkdown p {
        font-size: 18px !important;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🇧🇷 Brazilian E-Commerce Strategic Dashboard")
st.markdown("### 🚀 Module 2 Project: High-Level Business Intelligence")

# --- 2. AUTHENTICATION ---
KEY_FILE = 'stellar-verve-478012-n6-5c79fd657d1a.json'
PROJECT_ID = 'stellar-verve-478012-n6'
DATASET = 'olist_raw_analytics'

@st.cache_resource
def get_client():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            KEY_FILE, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except Exception as e:
        st.error(f"🚨 Connection Error: {e}")
        return None

@st.cache_data
def run_query(query):
    client = get_client()
    if client:
        return client.query(query).to_dataframe()
    return pd.DataFrame()

# --- 3. DATA INGESTION ---
with st.spinner('Crunching BigQuery Data...'):
    
    # KPI Query
    sql_kpi = f"""
        SELECT 
            (SELECT COUNT(DISTINCT order_id) FROM `{PROJECT_ID}.{DATASET}.fct_orders`) as total_orders,
            (SELECT SUM(payment_value) FROM `{PROJECT_ID}.{DATASET}.dim_payments`) as total_revenue,
            (SELECT AVG(review_score) FROM `{PROJECT_ID}.{DATASET}.dim_reviews`) as avg_csat,
            (SELECT COUNT(DISTINCT seller_id) FROM `{PROJECT_ID}.{DATASET}.dim_sellers`) as total_sellers
    """
    df_kpi = run_query(sql_kpi)

    # 1. Monthly Revenue Trend
    sql_trend = f"""
        SELECT 
            DATE_TRUNC(DATE(o.purchase_at), MONTH) as month,
            SUM(i.price) as revenue
        FROM `{PROJECT_ID}.{DATASET}.fct_orders` o
        JOIN `{PROJECT_ID}.{DATASET}.fct_order_items` i ON o.order_id = i.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY 1 ORDER BY 1
    """
    df_trend = run_query(sql_trend)

    # 2. Top Categories
    sql_cat = f"""
        SELECT 
            p.category_name, 
            SUM(i.price) as revenue,
            COUNT(i.order_id) as volume
        FROM `{PROJECT_ID}.{DATASET}.fct_order_items` i
        JOIN `{PROJECT_ID}.{DATASET}.dim_products` p ON i.product_id = p.product_id
        WHERE p.category_name IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """
    df_cat = run_query(sql_cat)

    # 3. Delivery Speed vs Satisfaction
    sql_qual = f"""
        SELECT 
            r.review_score,
            AVG(o.time_to_delivery_hours) / 24 as avg_days
        FROM `{PROJECT_ID}.{DATASET}.dim_reviews` r
        JOIN `{PROJECT_ID}.{DATASET}.fct_orders` o ON r.order_id = o.order_id
        WHERE o.order_status = 'delivered' AND o.time_to_delivery_hours IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """
    df_qual = run_query(sql_qual)

    # 4. Payment Methods
    sql_pay = f"""
        SELECT payment_type, COUNT(*) as count 
        FROM `{PROJECT_ID}.{DATASET}.dim_payments` 
        GROUP BY 1 ORDER BY 2 DESC
    """
    df_pay = run_query(sql_pay)

    # 5. Order Distribution by Day of Week
    sql_dow = f"""
        SELECT 
            d.day_name,
            d.day_of_week, 
            COUNT(o.order_id) as orders
        FROM `{PROJECT_ID}.{DATASET}.fct_orders` o
        JOIN `{PROJECT_ID}.{DATASET}.dim_date` d ON DATE(o.purchase_at) = d.date_day
        GROUP BY 1, 2 ORDER BY 2
    """
    df_dow = run_query(sql_dow)

    # 6. Seller State Performance
    sql_seller = f"""
        SELECT s.state, SUM(i.price) as revenue
        FROM `{PROJECT_ID}.{DATASET}.dim_sellers` s
        JOIN `{PROJECT_ID}.{DATASET}.fct_order_items` i ON s.seller_id = i.seller_id
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """
    df_seller = run_query(sql_seller)

# --- 4. VISUALIZATION HELPERS (LARGE FONTS) ---
def style_plot(fig, title):
    """Applies a consistent large-font style to all Plotly charts."""
    fig.update_layout(
        title=dict(text=title, font=dict(size=24)), # Big Title
        font=dict(size=18),                         # Big General Text (Legend, Axis)
        hovermode="x unified",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    # Make axis labels specifically larger and bold
    fig.update_xaxes(title_font=dict(size=20, family='Arial', color='black'), tickfont=dict(size=16))
    fig.update_yaxes(title_font=dict(size=20, family='Arial', color='black'), tickfont=dict(size=16))
    return fig

# --- 5. DASHBOARD LAYOUT ---

# A. KPI ROW
if not df_kpi.empty:
    st.markdown("## 🎯 Key Performance Indicators")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("💰 Total Revenue", f"R$ {df_kpi['total_revenue'][0]:,.0f}")
    k2.metric("📦 Total Orders", f"{df_kpi['total_orders'][0]:,.0f}")
    k3.metric("⭐ CSAT Score", f"{df_kpi['avg_csat'][0]:.2f} / 5.0")
    k4.metric("🏪 Active Sellers", f"{df_kpi['total_sellers'][0]:,.0f}")
    st.divider()

# B. INTERACTIVE TABS
# Note: CSS above forces these tab titles to be 24px
tab1, tab2, tab3, tab4 = st.tabs(["📈 Sales Analysis", "🚚 Logistics & QA", "💳 Financials", "📅 Behavioral"])

# --- TAB 1: SALES (SIDE BY SIDE LAYOUT) ---
with tab1:
    st.markdown("### Revenue Trends & Top Products")
    
    # Create two equal columns for Side-by-Side layout
    col_sales_1, col_sales_2 = st.columns([1, 1])
    
    with col_sales_1:
        # Chart 1: Trend
        if not df_trend.empty:
            fig_trend = px.line(df_trend, x='month', y='revenue', markers=True, 
                                line_shape='spline', color_discrete_sequence=['#00CC96'])
            st.plotly_chart(style_plot(fig_trend, "Monthly Revenue Growth"), use_container_width=True)

    with col_sales_2:
        # Chart 2: Categories
        if not df_cat.empty:
            fig_cat = px.bar(df_cat, x='revenue', y='category_name', orientation='h', 
                             color='revenue', color_continuous_scale='Viridis',
                             text_auto='.2s')
            fig_cat.update_layout(yaxis={'categoryorder':'total ascending'})
            # Hiding Y-axis title because the labels are obvious
            fig_cat.update_yaxes(title=None) 
            st.plotly_chart(style_plot(fig_cat, "Top 10 Categories"), use_container_width=True)

# --- TAB 2: LOGISTICS ---
with tab2:
    col_q1, col_q2 = st.columns(2)
    
    with col_q1:
        st.markdown("### 🐢 Speed vs. Satisfaction")
        if not df_qual.empty:
            fig_qual = px.bar(df_qual, x='review_score', y='avg_days', 
                              color='avg_days', color_continuous_scale='RdYlGn_r')
            st.plotly_chart(style_plot(fig_qual, "Avg Delivery Days by Review Score"), use_container_width=True)
            st.info("💡 Note: Lower delivery time = Higher score.")

    with col_q2:
        st.markdown("### 🗺️ Seller Distribution")
        if not df_seller.empty:
            fig_seller = px.bar(df_seller, x='state', y='revenue', 
                                color='revenue', color_continuous_scale='Blues')
            st.plotly_chart(style_plot(fig_seller, "Revenue by Seller State"), use_container_width=True)

# --- TAB 3: FINANCIALS ---
with tab3:
    st.markdown("### Payment Preferences")
    if not df_pay.empty:
        c1, c2 = st.columns([2, 1])
        with c1:
            fig_pie = px.pie(df_pay, names='payment_type', values='count', hole=0.4,
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_pie.update_traces(textposition='inside', textinfo='percent+label', textfont_size=20)
            st.plotly_chart(style_plot(fig_pie, "Order Count by Payment Method"), use_container_width=True)
        with c2:
            st.markdown("#### 💡 Strategy Note")
            st.write("Credit Card dominance suggests a need for strong fraud detection. High Boleto usage implies a need for automated payment reminders.")

# --- TAB 4: BEHAVIORAL ---
with tab4:
    st.markdown("### When do customers buy?")
    if not df_dow.empty:
        fig_dow = px.bar(df_dow, x='day_name', y='orders', 
                         color='orders', color_continuous_scale='Magma')
        st.plotly_chart(style_plot(fig_dow, "Total Orders by Day of Week"), use_container_width=True)

st.markdown("---")
st.caption("Powered by BigQuery & Streamlit | Project: olist-group-2")

#list of final commands the we have run after all the setup done 

'''
conda env update -f prj-environment.yml --prune
conda activate prj
python ingest_olist.py
cd Melt
meltano invoke dbt-bigquery:run-operation stage_external_sources --args "{select: olist_raw}"
cd ..
cd d ./dbt/olist_analytics/
dbt run
ccd ../..
python bq_checks.py
dashboard_rev2.py
'''

