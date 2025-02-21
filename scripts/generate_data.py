import os
import polars as pl
import random
import datetime


# Helper: manually parse a date string given the expected full timestamp format.
def parse_date(s, fmt):
    try:
        return datetime.datetime.strptime(s, fmt).date()
    except Exception:
        return None


# Expected full timestamp format (e.g. "2022-01-20 12:54:47.000")
DATE_FORMAT = "%Y-%m-%d %H:%M:%S.000"


# For non-date columns, randomly sample an existing value.
def sample_value(series):
    vals = series.drop_nulls().to_list()
    return random.choice(vals) if vals else None


# For a date column, generate a new date value using the same format as the sample.
def generate_new_date(sample, current_date):
    # If a sample exists and contains a space, assume it's a full timestamp.
    if sample and " " in sample:
        random_time = datetime.time(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        return datetime.datetime.combine(current_date, random_time).strftime(DATE_FORMAT)
    else:
        # Otherwise, return a simple date string.
        return current_date.strftime("%Y-%m-%d")


# Define dataset configuration; filenames are relative to the data folder.
datasets = [
    {"filename": "customer.csv", "is_dimension": True},
    {"filename": "monthly_inventory.csv", "is_dimension": False},
    {"filename": "order_lines.csv", "is_dimension": False},
    {"filename": "orders.csv", "is_dimension": True},
    {"filename": "product.csv", "is_dimension": True},
    {"filename": "returns.csv", "is_dimension": False}
]

# Define the path to the data folder (assuming this script is in the "scripts" folder)
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")
today = datetime.date.today()

for ds in datasets:
    filename = ds["filename"]
    file_path = os.path.join(DATA_DIR, filename)

    # Skip dimension datasets (if desired)
    if ds["is_dimension"]:
        print(f"Skipping dimension dataset: {filename}")
        continue

    try:
        df_orig = pl.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        continue

    # We assume every CSV has a "date" column.
    if "date" not in df_orig.columns:
        print(f"No 'date' column found in {filename}. Skipping dataset.")
        continue

    # Determine maximum date in the original file by manually parsing the "date" column.
    date_values = df_orig["date"].drop_nulls().to_list()
    max_date = None
    for s in date_values:
        d = parse_date(s, DATE_FORMAT)
        if d is not None and (max_date is None or d > max_date):
            max_date = d

    if max_date is None:
        print(f"No valid date found in {filename}. Using today as start_date: {today}")
        start_date = today
    else:
        start_date = max_date + datetime.timedelta(days=1)

    new_rows = []
    current_date = start_date
    while current_date <= today:
        num_new = random.randint(20, 40)  # up to 20 new rows per day
        for _ in range(num_new):
            new_row = {}
            # For returns.csv and order_lines.csv, sync specific date fields:
            if filename.lower() == "returns.csv":
                # Assume the two date columns are "date" and "return date"
                sync_cols = {"date", "return date"}
                new_date_value = None  # generate once per new row
            elif filename.lower() == "order_lines.csv":
                # Assume the two date columns are "date" and "order_date"
                sync_cols = {"date", "order_date"}
                new_date_value = None
            else:
                sync_cols = set()  # no syncing for other files

            for col in df_orig.columns:
                # For date/time columns (column name contains "date" or "time")
                if "date" in col.lower() or "time" in col.lower():
                    # If this column is to be synced, generate a common value once.
                    if col.lower() in sync_cols:
                        if new_date_value is None:
                            series = df_orig[col].drop_nulls()
                            sample = series.to_list()[0] if series.len() > 0 else None
                            new_date_value = generate_new_date(str(sample) if sample is not None else None,
                                                               current_date)
                        new_row[col] = new_date_value
                    else:
                        # For non-synced date/time columns, generate individually.
                        series = df_orig[col].drop_nulls()
                        sample = series.to_list()[0] if series.len() > 0 else None
                        new_row[col] = generate_new_date(str(sample) if sample is not None else None, current_date)
                else:
                    new_row[col] = sample_value(df_orig[col])
            new_rows.append(new_row)
        current_date += datetime.timedelta(days=1)

    if new_rows:
        new_df = pl.DataFrame(new_rows)
        # Simply concatenate without altering original schema.
        updated_df = pl.concat([df_orig, new_df])
        updated_df.write_csv(file_path)
        print(f"Updated {filename} with {len(new_rows)} new rows (structure unchanged).")
    else:
        print(f"No new rows generated for {filename}.")
