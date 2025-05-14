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

    file_path = os.path.join(DATA_DIR, filename)
    print(f"📂 Reading {filename} from local: {file_path}")
    return pl.read_csv(file_path)

def write_csv(df, filename):
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

        buffer = BytesIO()
        df.write_csv(buffer)
        buffer.seek(0)

        print(f"📤 Uploading full file to s3://{bucket}/{key}")
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue(), ContentType="text/csv")
    else:
        file_path = os.path.join(DATA_DIR, filename)
        df.write_csv(file_path)

def update_dataset(filename, new_data):
    df_orig = read_csv(filename)
    df_new = pl.DataFrame(new_data)
    ref_columns = df_orig.columns
    for col in ref_columns:
        if col not in df_new.columns:
            dtype = df_orig.schema.get(col, pl.Utf8)
            df_new = df_new.with_columns(pl.lit(None).cast(dtype).alias(col))
    df_new = df_new.select(ref_columns)
    updated_df = pl.concat([df_orig, df_new])
    write_csv(updated_df, filename)
    print(f"✅ Updated {filename} with {len(new_data)} new records.")

def write_deltas_to_s3(df, filename):
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

    buffer = BytesIO()
    df.write_csv(buffer)
    buffer.seek(0)

    print(f"📤 Uploading {len(df)} delta rows to s3://{bucket}/{key}")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue(), ContentType="text/csv")

    # 🆕 Automatically update the meta file if the file is orders.csv
    if filename == "orders.csv":
        update_orders_meta_s3(datetime.date.today())

def get_last_order_date_s3():
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN")
    )
    bucket = os.getenv("AWS_S3_BUCKET")
    key = "orders_last_date.txt"

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        date_str = obj["Body"].read().decode("utf-8").strip()
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception as e:
        print(f"⚠️ Could not load orders_last_date.txt from S3: {e}")
        return datetime.date.today() - datetime.timedelta(days=1)

def update_orders_meta_s3(current_date):
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN")
    )
    bucket = os.getenv("AWS_S3_BUCKET")
    key = "orders_last_date.txt"
    body = current_date.strftime("%Y-%m-%d")

    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"), ContentType="text/plain")
    print(f"📄 Updated orders_last_date.txt to {body} in S3")
