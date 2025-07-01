import os
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
import boto3
import zipfile
from functions import download_from_kaggle, unzip_file, bucket_upload, get_s3_client

#Kaggle download part
api = KaggleApi()
api.authenticate()

KAGGLE_SLUG = "sraddhanjalibarik/hotel-booking-data-set"
DATASET_NAME = "hotel_bookings.csv"
DEST_DIR = os.path.join("sources")

try:
    print(f"Downloading {DATASET_NAME}")
    download_from_kaggle(api, KAGGLE_SLUG, DEST_DIR)
    print("Download completed")
    print(os.listdir(DEST_DIR))
except Exception as e:
    print(f"🔴 Failed to download from Kaggle: {e}")

#unzip_file(DEST_DIR, DATASET_NAME)

#S3 upload part
load_dotenv()

AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION = os.getenv("AWS_REGION")
BUCKET = os.getenv("AWS_BUCKET_NAME")

local_path = os.path.join(DEST_DIR, DATASET_NAME)
S3_KEY = f"raw/{DATASET_NAME}"

if not os.path.exists(local_path):
    raise FileNotFoundError(f"🔴 Local file not found: {local_path}")

s3 = get_s3_client()

upload_success = bucket_upload(s3, local_path, BUCKET, S3_KEY)
if upload_success:
    print("🟢 Proceeding with the pipeline...")
else:
    print("🔴 Stopping due to failed upload.")






