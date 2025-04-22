import os
import random
import datetime
import polars as pl

# Base directory for your CSV files.
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")
# Base date for numeric increments.
base_date_incr = datetime.date(2022, 1, 1)

def generate_id(prefix):
    """Generate a unique ID using a random number between 10^7 and 10^10-1."""
    return f"{prefix}-{random.randint(10000000, 9999999999)}"

def read_csv(filename):
    """Read a CSV file from the data directory."""
    file_path = os.path.join(DATA_DIR, filename)
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
    file_path = os.path.join(DATA_DIR, filename)
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
    print(f"Updated {filename} with {len(new_data)} new records.")