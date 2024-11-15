# S3.py
import boto3
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv

def upload_to_s3(df, bucket_name, file_name):
    """
    Uploads a DataFrame to an S3 bucket as a CSV file.

    Parameters:
    - df: DataFrame to upload.
    - bucket_name: Name of the S3 bucket.
    - file_name: Name of the file to be saved in the bucket.
    """

    # Initialize S3 client
    s3_client = boto3.client('s3',aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))

    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload CSV to S3
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=csv_buffer.getvalue()
    )
    print("File uploaded to S3:", response)
