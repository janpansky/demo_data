import datetime
import os
import random
from io import BytesIO

import boto3
import polars as pl

# Base directory for your CSV files.
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")
# Base date for numeric increments.
base_date_incr = datetime.date(2022, 1, 1)


def generate_id(prefix):
    """Generate a unique ID using a random number between 10^7 and 10^10-1."""
    return f"{prefix}-{random.randint(10000000, 9999999999)}"


def read_csv(filename):
    """
    Read a CSV file from local disk or from S3 if USE_S3=true is set.
    """
    if os.getenv("USE_S3", "false").lower() == "true":
        s3 = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN")
        )
        bucket = os.getenv("AWS_S3_BUCKET")
        key = filename

        print(f"📦 Reading {key} from s3://{bucket}/{key}...")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pl.read_csv(BytesIO(obj["Body"].read()))

    # ✅ Fallback to local
    file_path = os.path.join(DATA_DIR, filename)
    print(f"📂 Reading {filename} from local: {file_path}")
    return pl.read_csv(file_path)


def write_csv(df, filename):
    """Write a Polars DataFrame to a CSV file in the data directory."""
    file_path = os.path.join(DATA_DIR, filename)
    df.write_csv(file_path)


def update_dataset(filename, new_data):
    """
    Append new_data (a list of dicts) to the CSV file.
    This function forces the new data to use exactly the original file's columns.
    """
    df_orig = read_csv(filename)
    df_new = pl.DataFrame(new_data)
    # Use the original file's column order.
    ref_columns = df_orig.columns
    # For any missing column in df_new, add it as null.
    for col in ref_columns:
        if col not in df_new.columns:
            dtype = df_orig.schema.get(col, pl.Utf8)
            df_new = df_new.with_columns(pl.lit(None).cast(dtype).alias(col))
    df_new = df_new.select(ref_columns)
    updated_df = pl.concat([df_orig, df_new])
    write_csv(updated_df, filename)
    print(f"✅ Updated {filename} with {len(new_data)} new records.")


def write_deltas_to_s3(df, filename):
    """
    Upload only delta DataFrame to S3 instead of writing full dataset.
    """
    if df.is_empty():
        print(f"⚠️ No delta data to upload for {filename}. Skipping.")
        return

    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN")
    )
    bucket = os.getenv("AWS_S3_BUCKET")
    key = f"deltas/{filename}"

    # Save DataFrame to memory as CSV
    from io import BytesIO
    buffer = BytesIO()
    df.write_csv(buffer)
    buffer.seek(0)

    print(f"📤 Uploading {len(df)} delta rows to s3://{bucket}/{key}")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue(), ContentType="text/csv")
