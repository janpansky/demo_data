import os
import polars as pl
import random
import datetime

# Define dataset configurations
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")
today = datetime.date.today()

# Helper function to generate unique IDs
def generate_id(prefix):
    return f"{prefix}-{random.randint(100000, 999999)}"

# Load existing data and determine the latest recorded date per dataset
max_dates = {}

datasets = [
    {"filename": "customer.csv", "date_column": "customer_created_date"},
    {"filename": "orders.csv", "date_column": "order_date"},
    {"filename": "order_lines.csv", "date_column": "date"},
    {"filename": "returns.csv", "date_column": "date"},
]

existing_columns = {}

for ds in datasets:
    file_path = os.path.join(DATA_DIR, ds["filename"])

    try:
        df_orig = pl.read_csv(file_path)
        existing_columns[ds["filename"]] = df_orig.columns  # Store existing columns
        if ds["date_column"] and ds["date_column"] in df_orig.columns:
            max_dates[ds["filename"]] = df_orig[ds["date_column"]].drop_nulls().max()
        else:
            max_dates[ds["filename"]] = None
    except Exception as e:
        print(f"Error reading {ds['filename']}: {e}")
        max_dates[ds["filename"]] = None
        existing_columns[ds["filename"]] = None

# Convert max dates to datetime objects
for key, value in max_dates.items():
    if value is not None:
        try:
            max_dates[key] = datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            max_dates[key] = datetime.datetime.strptime(value.split()[0], "%Y-%m-%d").date()
    else:
        max_dates[key] = datetime.date(2024, 1, 1)

# Merchant types
MERCHANT_TYPES = ["merchant__electronics", "merchant__clothing", "merchant__bigboxretailer"]

# Step 1: Generate Missing Data Day-by-Day
new_customers = []
new_orders = []
new_order_lines = []
new_returns = []

# Ensure customers use existing locations
customer_locations = pl.read_csv(os.path.join(DATA_DIR, "customer.csv")).select([
    "customer_city", "customer_state", "customer_country",
    "geo__customer_city__city_pushpin_longitude",
    "geo__customer_city__city_pushpin_latitude"
]).unique().to_dicts()

current_date = min(max_dates.values()) + datetime.timedelta(days=1)

while current_date <= today:
    day_customers = []
    day_orders = []
    day_order_lines = []
    day_returns = []

    # Generate customers for missing day
    if max_dates["customer.csv"] < current_date:
        for _ in range(random.randint(5, 10)):
            new_customer_id = generate_id("C")
            full_name = f"{random.choice(['Emma', 'Olivia', 'Liam', 'Noah', 'Ava'])} {random.choice(['Smith', 'Johnson', 'Williams'])}"
            location = random.choice(customer_locations)
            merchant_type = random.choice(MERCHANT_TYPES)

            customer = {
                "customer_id": new_customer_id,
                "ls__customer_id__customer_name": full_name,
                "customer_city": location["customer_city"],
                "geo__customer_city__city_pushpin_longitude": location["geo__customer_city__city_pushpin_longitude"],
                "geo__customer_city__city_pushpin_latitude": location["geo__customer_city__city_pushpin_latitude"],
                "customer_country": location["customer_country"],
                "customer_email": f"{full_name.replace(' ', '.').lower()}@example.com",
                "customer_state": location["customer_state"],
                "customer_created_date": current_date.strftime("%Y-%m-%d"),
                "wdf__client_id": merchant_type,
            }
            day_customers.append(customer)
            new_customers.append(customer)

    # Get **existing** customers to use in orders
    existing_customers = pl.read_csv(os.path.join(DATA_DIR, "customer.csv"))["customer_id"].to_list()
    day_customer_ids = [c["customer_id"] for c in day_customers]  # Newly generated customers
    all_valid_customers = existing_customers + day_customer_ids  # Ensure only real customers are used

    # **Check if orders for this date already exist**
    existing_orders = pl.read_csv(os.path.join(DATA_DIR, "orders.csv"))
    if "order_date" in existing_orders.columns:
        existing_order_dates = existing_orders["order_date"].drop_nulls().unique().to_list()
    else:
        existing_order_dates = []

    # Generate orders **only if missing for this day**
    if str(current_date) not in existing_order_dates:
        for _ in range(random.randint(3, 7)):
            new_order_id = generate_id("O")
            customer_id = random.choice(all_valid_customers)  # Only use real customers

            order = {
                "order_id": new_order_id,
                "wdf__client_id": random.choice(MERCHANT_TYPES),
                "order_status": random.choice(["Processed", "Completed", "In Cart", "Canceled"]),
                "order_date": current_date.strftime("%Y-%m-%d"),  # Ensure order is dated correctly
                "customer_id": customer_id  # Ensuring valid customers
            }
            day_orders.append(order)
            new_orders.append(order)

    # Generate order lines for missing day
    if max_dates["order_lines.csv"] < current_date:
        for order in day_orders:
            for _ in range(random.randint(1, 3)):
                order_line = {
                    "order_line_id": generate_id("L"),
                    "order__order_id": order["order_id"],
                    "product__product_id": generate_id("P"),
                    "customer__customer_id": order["customer_id"],  # Ensure the customer exists
                    "order_unit_price": float(round(random.uniform(5, 200), 2)),
                    "order_unit_quantity": float(random.randint(1, 5)),
                    "wdf__client_id": order["wdf__client_id"],
                    "order_unit_discount": float(round(random.uniform(0, 50), 2)),
                    "order_unit_cost": float(round(random.uniform(5, 150), 2)),
                    "date": current_date.strftime("%Y-%m-%d %H:%M:%S.000"),
                    "order_date": current_date.strftime("%Y-%m-%d %H:%M:%S.000"),
                    "customer_age": f"{random.randint(18, 70)}M+",
                }
                day_order_lines.append(order_line)
                new_order_lines.append(order_line)

    # Move to next missing day
    current_date += datetime.timedelta(days=1)

# Save only the datasets that need updating
for dataset, new_data in [
    ("customer.csv", new_customers),
    ("orders.csv", new_orders),
    ("order_lines.csv", new_order_lines),
]:
    if not new_data:
        print(f"No new data for {dataset}. Skipping update.")
        continue

    file_path = os.path.join(DATA_DIR, dataset)

    try:
        df_orig = pl.read_csv(file_path)
        df_new = pl.DataFrame(new_data)

        # Ensure column consistency before appending
        if existing_columns[dataset]:
            df_new = df_new.select(existing_columns[dataset])  # Only keep known columns

        updated_df = pl.concat([df_orig, df_new]).filter(pl.col(df_orig.columns[0]).is_not_null())
        updated_df.write_csv(file_path)
        print(f"Updated {dataset} with {len(new_data)} new records.")

    except Exception as e:
        print(f"Error writing {dataset}: {e}")