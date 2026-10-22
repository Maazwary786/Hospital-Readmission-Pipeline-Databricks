
import boto3
import os

BUCKET_NAME = "mouaz-analytics-2026-8627"
REGION = "ap-south-1"

LOCAL_FOLDER = "data/power_bi_export"
S3_FOLDER = "processed/"

s3 = boto3.client("s3", region_name=REGION)

for filename in os.listdir(LOCAL_FOLDER):
    if filename.endswith(".csv"):
        local_path = os.path.join(LOCAL_FOLDER, filename)
        s3_key = S3_FOLDER + filename

        s3.upload_file(local_path, BUCKET_NAME, s3_key)
        print(f"Uploaded: {filename}")

print("\nAll CSV files uploaded successfully!")
