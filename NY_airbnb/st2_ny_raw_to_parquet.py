import os
from dotenv import load_dotenv
from functions import get_s3_client, bucket_upload
from generate_athena_schema_func import generate_athena_schema
import boto3
import pandas as pd
from io import BytesIO

load_dotenv()

AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION = os.getenv("AWS_REGION")
BUCKET = os.getenv("AWS_BUCKET_NAME")

S3_KEY = "raw/AB_NYC_2019.csv"

#S3 download part
s3 = get_s3_client()

print("Downloading CSV from S3..")
obj = s3.get_object(Bucket=BUCKET, Key=S3_KEY)
df = pd.read_csv(BytesIO(obj["Body"].read()), compression='infer')

#pandas clean part

df = df.drop_duplicates()
df = df.dropna(subset=['name', 'host_name'])
df = df[df['price'] <= 800]
df['last_review'] = df['last_review'].fillna('No reviews')
df['reviews_per_month'] = df['reviews_per_month'].fillna(0)

#to parquet / upload to S3
s3_key_processed = "processed/AB_NYC_2019.parquet"

buffer = BytesIO()
df.to_parquet(buffer, index=False)
buffer.seek(0)

try:
    s3.upload_fileobj(buffer, BUCKET, s3_key_processed)
    print("🟢 Parquet uploaded to S3 successfully.")
except Exception as e:
    print(f"🔴 Upload failed: {e}")

#athena schema generation
table_name = "AB_NYC_2019"
location = 's3://hotel-data-lake44/processed/'
s3_schema_key= "schemas/AB_NYC_2019.sql"
schema = generate_athena_schema(df, table_name, location) 
print(schema)

athena = boto3.client('athena', region_name ='us-east-2')

database = 'default'
output = 's3://hotel-data-lake44/athena-query-results/'

response = athena.start_query_execution(
    QueryString = schema,
    QueryExecutionContext={'Database': database},
    ResultConfiguration={'OutputLocation': output}
)
print('Started Athena query', response['QueryExecutionId'])