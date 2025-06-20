import os
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
import boto3


def download_from_kaggle(api, slug, path):
    api.dataset_download_files(slug, path=path, unzip=True)

def unzip_file(path, file_name):
    zip_path = os.path.join(path, file_name)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(path)

def bucket_upload(client, local_path, bucket, s3_key):
    try:
        print(f"Uploading to S3 : s3://{bucket}/{s3_key}...")
        client.upload_file(local_path, bucket, s3_key)
        print("🟢 Upload successful")
        return True
    except client.exceptions.NoSuchBucket:
        print(f"🔴 Error: {bucket} does not exists")
    except Exception as e:
        print(f"🔴 Upload failed: {e}")
    return False

def get_s3_client():
    load_dotenv()
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )
    return session.client("s3")

 