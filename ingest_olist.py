import os
import zipfile
import glob
from concurrent.futures import ThreadPoolExecutor
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage

# --- CONFIGURATION ---
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
LOCAL_DOWNLOAD_PATH = "./temp_data"
GCS_BUCKET_NAME = "dsai2-olist-raw-data"    # Your GCS Bucket Name
GCS_DESTINATION_FOLDER = "raw/olist/"      # Folder inside the bucket
GCP_KEY_PATH = "stellar-verve-478012-n6-5c79fd657d1a.json"              # Path to your Service Account Key

def download_from_kaggle():
    """Authenticates and downloads dataset from Kaggle."""
    print("--- Step 1: Authenticating with Kaggle ---")
    # Ensure kaggle.json is in ~/.kaggle/ or env vars are set
    api = KaggleApi()
    api.authenticate()

    print(f"--- Step 2: Downloading {KAGGLE_DATASET} ---")
    if not os.path.exists(LOCAL_DOWNLOAD_PATH):
        os.makedirs(LOCAL_DOWNLOAD_PATH)
    
    # Download and unzip automatically
    api.dataset_download_files(
        KAGGLE_DATASET, 
        path=LOCAL_DOWNLOAD_PATH, 
        unzip=True
    )
    print("Download and extraction complete.")

def upload_single_file(local_file, bucket):
    """Uploads a single file to GCS."""
    filename = os.path.basename(local_file)
    blob_path = f"{GCS_DESTINATION_FOLDER}{filename}"
    
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_file)
    print(f"Uploaded: {filename} -> gs://{bucket.name}/{blob_path}")

def upload_to_gcs_parallel():
    """Uploads all CSVs in the folder to GCS using parallel threads."""
    print("--- Step 3: Uploading to Google Cloud Storage ---")
    
    # Authenticate with GCS
    client = storage.Client.from_service_account_json(GCP_KEY_PATH)
    bucket = client.bucket(GCS_BUCKET_NAME)

    # Get list of CSV files
    csv_files = glob.glob(f"{LOCAL_DOWNLOAD_PATH}/*.csv")
    
    for file in csv_files:
        upload_single_file(file, bucket)
        print(file)

    # Use ThreadPool to upload files in parallel (Faster for multiple files)
    #with ThreadPoolExecutor(max_workers=5) as executor:
    #    for file in csv_files:
    #        executor.submit(upload_single_file, file, bucket)
    #        print(file)

def cleanup():
    """Removes temporary files."""
    print("--- Step 4: Cleanup ---")
    files = glob.glob(f"{LOCAL_DOWNLOAD_PATH}/*")
    for f in files:
        os.remove(f)
    os.rmdir(LOCAL_DOWNLOAD_PATH)
    print("Local temp files removed.")

if __name__ == "__main__":
    download_from_kaggle()
    upload_to_gcs_parallel()
    cleanup()
    print("✅ Ingestion Complete.")
