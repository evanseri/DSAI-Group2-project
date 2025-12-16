# DSAI-Group2-project
DSAI-Group2 Project

# Project setup
## Conda Environments

Install conda environments required for project
Run the following command:

`conda env create --file prj-environment.yml`

`conda activate prj`

## Extract and Load
Prerequisites
1. Kaggle API Key: Go to Kaggle Settings -> Create New Token. Save kaggle.json.

2. GCP Service Account: Go to GCP Console -> IAM -> Service Accounts -> Create Key (JSON). Save as gcp_key json.

3. GCS Bucket: Create a bucket (e.g., my-olist-raw-data) in Google Cloud Storage.

## Creat a bucket in google cloud storage
- bucketname:  dsai2-olist-raw-data
- Grant acces  storage storage object admin isung iam 

## Saving kaggle key
mkdir -p ~/.kaggle
cp kaggle/kaggle.json ~/.kaggle/kaggle.json

## Execute Extract & load.
- Run below command in terminal. 
- python ingest_olist.py 

# Transform
## transfer date from GCS to BigQuery using Meltano dbt
- Run below commands in terminal. 
- meltano init
( Enter the project name as Melt). It will create a folder \Melt
- cd Melt
- meltano add utility dbt-bigquery
(Installs dbt-bigquery.
Creates a transform/ folder in your project.)
- meltano invoke dbt-bigquery:initialize
- meltano invoke dbt-bigquery:deps


Right-click transform/ -> New File -> Name it packages.yml.

Paste this content into it:
packages:
  - package: dbt-labs/dbt_external_tables
    version: 0.12.0

- meltano invoke dbt-bigquery:deps

'- meltano config dbt-bigquery set keyfile gcp_key.json
- meltano config dbt-bigquery set keyfile stellar-verve-478012-n6-5c79fd657d1a.json
(Replace with your gcp jason key file) 

'- meltano config dbt-bigquery set project your-gcp-project-id
- meltano config dbt-bigquery set project stellar-verve-478012-n6
(Replace with your gcp project Id) 
- meltano config dbt-bigquery set dataset dbt_dev

Go to transform/models/.

Create a folder named staging (optional, but good practice).

Create the file transform/models/staging/src_olist.yml.
fill the content of src_olist.yml
( make sure to replace the projectid)



