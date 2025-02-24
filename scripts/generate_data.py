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
    {"filename": "orders.csv", "date_column": None},  # Orders use customer_created_date indirectly
    {"filename": "order_lines.csv", "date_column": "date"},
    {"filename": "returns.csv", "date_column": "date"},
]

for ds in datasets:
    file_path = os.path.join(DATA_DIR, ds["filename"])

    try:
        df_orig = pl.read_csv(file_path)
        if ds["date_column"] and ds["date_column"] in df_orig.columns:
            max_dates[ds["filename"]] = df_orig[ds["date_column"]].drop_nulls().max()
        else:
            max_dates[ds["filename"]] = None  # No date column, update only if new orders exist
    except Exception as e:
        print(f"Error reading {ds['filename']}: {e}")
        max_dates[ds["filename"]] = None  # Assume missing files should start fresh

# Convert max dates to datetime objects for comparison
for key, value in max_dates.items():
    if value is not None:
        try:
            max_dates[key] = datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            max_dates[key] = datetime.datetime.strptime(value.split()[0], "%Y-%m-%d").date()
    else:
        max_dates[key] = datetime.date(2024, 1, 1)  # Default start date if missing

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

    # Generate orders for missing day
    if max_dates["orders.csv"] < current_date:
        for customer in day_customers:
            new_order_id = generate_id("O")
            order = {
                "order_id": new_order_id,
                "wdf__client_id": customer["wdf__client_id"],
                "order_status": random.choice(["Processed", "Completed", "In Cart", "Canceled"]),
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
                    "customer__customer_id": generate_id("C"),
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

    # Generate returns for missing day
    if max_dates["returns.csv"] < current_date:
        for order in day_orders:
            if random.random() < 0.3:
                return_data = {
                    "return_id": generate_id("R"),
                    "order__order_id": order["order_id"],
                    "product__product_id": generate_id("P"),
                    "customer__customer_id": generate_id("C"),
                    "return_unit_cost": float(round(random.uniform(10, 150), 2)),
                    "return_unit_quantity": float(random.randint(1, 5)),
                    "wdf__client_id": order["wdf__client_id"],
                    "return_unit_paid_amount": float(round(random.uniform(5, 200), 2)),
                    "date": current_date.strftime("%Y-%m-%d %H:%M:%S.000"),
                    "return_date": current_date.strftime("%Y-%m-%d %H:%M:%S.000"),
                }
                day_returns.append(return_data)
                new_returns.append(return_data)

    # Move to next missing day
    current_date += datetime.timedelta(days=1)

# Save only the datasets that need updating
for dataset, new_data in [
    ("customer.csv", new_customers),
    ("orders.csv", new_orders),
    ("order_lines.csv", new_order_lines),
    ("returns.csv", new_returns)
]:
    if not new_data:
        print(f"No new data for {dataset}. Skipping update.")
        continue

    file_path = os.path.join(DATA_DIR, dataset)

    try:
        df_orig = pl.read_csv(file_path)
        df_new = pl.DataFrame(new_data)

        updated_df = pl.concat([df_orig, df_new]).filter(pl.col(df_orig.columns[0]).is_not_null())
        updated_df.write_csv(file_path)
        print(f"Updated {dataset} with {len(new_data)} new records.")

    except Exception as e:
        print(f"Error writing {dataset}: {e}")