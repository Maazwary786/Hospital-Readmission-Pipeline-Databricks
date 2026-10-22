
import boto3

BUCKET_NAME = "mouaz-analytics-2026-8627"

s3 = boto3.client("s3", region_name="ap-south-1")

response = s3.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="raw/"
)

print("S3 connection successful!")

for obj in response.get("Contents", []):
    print(obj["Key"])
