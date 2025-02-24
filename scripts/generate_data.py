import os
import polars as pl
import random
import datetime

# Define a list of first and last names to generate new customer names
FIRST_NAMES = [
    "Emma", "Olivia", "Liam", "Noah", "Ava", "Sophia", "Mason", "Isabella", "Lucas", "Ethan",
    "James", "Harper", "Michael", "Alexander", "Daniel", "Emily", "Benjamin", "Jack", "Amelia"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Davis", "Rodriguez", "Martinez",
    "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson"
]

# Define dataset configuration; filenames are relative to the data folder.
datasets = [
    {"filename": "customer.csv", "is_dimension": True},
    {"filename": "monthly_inventory.csv", "is_dimension": False},
    {"filename": "order_lines.csv", "is_dimension": False},
    {"filename": "orders.csv", "is_dimension": True},
    {"filename": "product.csv", "is_dimension": True},
    {"filename": "returns.csv", "is_dimension": False}
]

# Path to the data folder (assuming this script is in the "scripts" folder)
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")
today = datetime.date.today()


for ds in datasets:
    filename = ds["filename"]
    file_path = os.path.join(DATA_DIR, filename)

    try:
        df_orig = pl.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        continue

    # Handle customer table separately
    if filename == "customer.csv":
        print(f"Appending new customers to {filename}...")

        required_columns = [
            "customer_id", "ls__customer_id__customer_name", "customer_city",
            "geo__customer_city__city_pushpin_longitude", "geo__customer_city__city_pushpin_latitude",
            "customer_country", "customer_email", "customer_state",
            "customer_created_date", "wdf__client_id"
        ]

        # Ensure all required columns exist
        if not all(col in df_orig.columns for col in required_columns):
            print(f"Missing required columns in {filename}. Skipping dataset.")
            continue

        # Extract unique locations from existing customers
        location_pool = df_orig.select(["customer_city", "customer_state", "customer_country",
                                        "geo__customer_city__city_pushpin_longitude",
                                        "geo__customer_city__city_pushpin_latitude"]).unique().to_dicts()

        # Find the max customer_created_date to generate only newer customers
        date_values = df_orig["customer_created_date"].drop_nulls().to_list()
        max_date = max(date_values) if date_values else today
        start_date = datetime.datetime.strptime(max_date, "%Y-%m-%d").date() + datetime.timedelta(days=1)

        new_rows = []
        current_date = start_date
        while current_date <= today:
            num_new = random.randint(1, 5)  # Add 5-15 new customers per day
            for _ in range(num_new):
                # Generate a unique customer ID
                new_customer_id = f"C-{random.randint(1000000, 9999999)}"
                while new_customer_id in df_orig["customer_id"].to_list():
                    new_customer_id = f"C-{random.randint(1000000, 9999999)}"

                # Generate a random name
                first_name = random.choice(FIRST_NAMES)
                last_name = random.choice(LAST_NAMES)
                full_name = f"{first_name} {last_name}"

                # Assign a random location from existing customers
                location = random.choice(location_pool)

                # Generate a realistic email
                email_domain = random.choice(["@gmail.com", "@yahoo.com", "@outlook.com", "@example.com"])
                email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}{email_domain}"

                new_row = {
                    "customer_id": new_customer_id,
                    "ls__customer_id__customer_name": full_name,
                    "customer_city": location["customer_city"],
                    "geo__customer_city__city_pushpin_longitude": location["geo__customer_city__city_pushpin_longitude"],
                    "geo__customer_city__city_pushpin_latitude": location["geo__customer_city__city_pushpin_latitude"],
                    "customer_country": location["customer_country"],
                    "customer_email": email,
                    "customer_state": location["customer_state"],
                    "customer_created_date": current_date.strftime("%Y-%m-%d"),
                    "wdf__client_id": random.choice(df_orig["wdf__client_id"].to_list())  # Keep existing categories
                }
                new_rows.append(new_row)

            current_date += datetime.timedelta(days=1)

        if new_rows:
            new_df = pl.DataFrame(new_rows)
            updated_df = pl.concat([df_orig, new_df])
            updated_df.write_csv(file_path)
            print(f"Updated {filename} with {len(new_rows)} new customers.")
        else:
            print(f"No new customers generated for {filename}.")

        continue  # Skip the rest of the loop for customer.csv

    # Skip dimension datasets
    if ds["is_dimension"]:
        print(f"Skipping dimension dataset: {filename}")
        continue

    # For non-dimension datasets, ensure "date" column exists
    if "date" not in df_orig.columns:
        print(f"No 'date' column found in {filename}. Skipping dataset.")
        continue

    # Determine maximum date in the original file
    date_values = df_orig["date"].drop_nulls().to_list()
    max_date = max(date_values) if date_values else today
    start_date = datetime.datetime.strptime(max_date.split()[0], "%Y-%m-%d").date() + datetime.timedelta(days=1)

    new_rows = []
    current_date = start_date
    while current_date <= today:
        num_new = random.randint(20, 40)
        for _ in range(num_new):
            new_row = {}
            for col in df_orig.columns:
                new_row[col] = random.choice(df_orig[col].drop_nulls().to_list()) if col != "date" else current_date.strftime("%Y-%m-%d")
            new_rows.append(new_row)

        current_date += datetime.timedelta(days=1)

    if new_rows:
        new_df = pl.DataFrame(new_rows)
        updated_df = pl.concat([df_orig, new_df])
        updated_df.write_csv(file_path)
        print(f"Updated {filename} with {len(new_rows)} new rows.")
    else:
        print(f"No new rows generated for {filename}.")