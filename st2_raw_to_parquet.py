import os
from dotenv import load_dotenv
from functions import get_s3_client, bucket_upload
import boto3
import pandas as pd
from io import BytesIO

load_dotenv()

AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION = os.getenv("AWS_REGION")
BUCKET = os.getenv("AWS_BUCKET_NAME")

S3_KEY = "raw/hotel_bookings.csv"

#S3 download part
s3 = get_s3_client()

print("Downloading CSV from S3..")
obj = s3.get_object(Bucket=BUCKET, Key=S3_KEY)
df = pd.read_csv(BytesIO(obj["Body"].read()), compression='infer')

#pandas clean part
df = df.drop_duplicates()
print("🟢 Duplicates droped")

df = df[(df["adr"] >= 0) & (df["adr"] < 1000)]
print("🟢 Negative and extremely high adr dropped")

df["reservation_status_date"] = pd.to_datetime(df["reservation_status_date"])
df["arrival_date"] = pd.to_datetime(
    df["arrival_date_year"].astype(str) + "-" +
    df["arrival_date_month"] + "-" +
    df["arrival_date_day_of_month"].astype(str),
    errors="coerce"
)
df.drop(["arrival_date_year", "arrival_date_month", "arrival_date_day_of_month"], axis=1, inplace=True)
before = len(df)
df = df[df["arrival_date"].notna()]
after = len(df)
print(f"🟢 Removed {before - after} rows with invalid arrival dates (NaT)")

df["is_repeated_guest"] = df["is_repeated_guest"] == 1
df["is_canceled"] = df["is_canceled"] == 1
df["underage_guests"] = (df['children'] > 0) | (df['babies'] > 0)
print("🟢 Boolean fixed")

df.drop(["agent", "company", "reserved_room_type", 'assigned_room_type', 'days_in_waiting_list'], axis=1, inplace=True)

#to parquet
LOCAL_PROCESSED_DIR = os.path.join("processed")
os.makedirs(LOCAL_PROCESSED_DIR, exist_ok=True)

processed_path = os.path.join(LOCAL_PROCESSED_DIR, "hotel_bookings.parquet")
df.to_parquet(processed_path, index=False)
print(f"🟢 Saved local parquet: {processed_path}")

s3_key_processed = "processed/hotel_bookings.parquet"

processed_upload_success = bucket_upload(s3, processed_path, BUCKET, s3_key_processed)
if processed_upload_success:
    print("🟢 Parquet uploaded to S3 successfully.")
else:
    print(f"🔴 Upload failed")