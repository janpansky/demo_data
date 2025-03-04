import os
import polars as pl
import random
import datetime

# Define dataset configurations and today's date.
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")
today = datetime.date.today()
# Base date for numeric increment (adjust multiplier as needed)
base_date_incr = datetime.date(2022, 1, 1)

# Helper: generate unique IDs.
def generate_id(prefix):
    return f"{prefix}-{random.randint(100000, 99999999)}"

# Datasets to update.
datasets = [
    {"filename": "customer.csv", "date_column": "customer_created_date"},
    {"filename": "orders.csv", "date_column": "order_date"},       # orders.csv originally has only 3 columns.
    {"filename": "order_lines.csv", "date_column": "date"},
    {"filename": "returns.csv", "date_column": "return_date"},
    {"filename": "monthly_inventory.csv", "date_column": "date"}
]

# Read each file to extract the existing columns and compute the max date.
existing_columns = {}
max_dates = {}
for ds in datasets:
    file_path = os.path.join(DATA_DIR, ds["filename"])
    try:
        df_orig = pl.read_csv(file_path)
        # Preserve the original column order.
        existing_columns[ds["filename"]] = df_orig.columns
        if ds["date_column"] and ds["date_column"] in df_orig.columns:
            max_dates[ds["filename"]] = df_orig[ds["date_column"]].drop_nulls().max()
        else:
            max_dates[ds["filename"]] = None
    except Exception as e:
        print(f"Error reading {ds['filename']}: {e}")
        max_dates[ds["filename"]] = None
        existing_columns[ds["filename"]] = None

# Convert max dates to datetime objects.
for key, value in max_dates.items():
    if value is not None:
        try:
            max_dates[key] = datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            max_dates[key] = datetime.datetime.strptime(value.split()[0], "%Y-%m-%d").date()
    else:
        max_dates[key] = datetime.date(2024, 1, 1)

# Merchant types.
MERCHANT_TYPES = ["merchant__electronics", "merchant__clothing", "merchant__bigboxretailer"]

# Load existing references.
try:
    customer_df = pl.read_csv(os.path.join(DATA_DIR, "customer.csv"))
    existing_customer_ids = customer_df["customer_id"].to_list()
except Exception:
    existing_customer_ids = []

try:
    orders_df = pl.read_csv(os.path.join(DATA_DIR, "orders.csv"))
    existing_order_ids = orders_df["order_id"].to_list()
except Exception:
    existing_order_ids = []

try:
    product_df = pl.read_csv(os.path.join(DATA_DIR, "product.csv"))
    existing_product_ids = product_df["product_id"].to_list()
except Exception:
    existing_product_ids = []

# Load customer locations.
customer_locations = customer_df.select([
    "customer_city", "customer_state", "customer_country",
    "geo__customer_city__city_pushpin_longitude",
    "geo__customer_city__city_pushpin_latitude"
]).unique().to_dicts()

# --------------------------
# Step 1: Generate New Data for Customers, Orders, and Order Lines (Daily Data)
# --------------------------
new_customers = []
new_orders = []      # In memory, we generate extra fields for orders (customer_id, order_date) for referential linking.
new_order_lines = []

# Use the maximum date among customer, orders, and order_lines as the start date.
global_start_date = max(max_dates["customer.csv"], max_dates["orders.csv"], max_dates["order_lines.csv"])
current_date = global_start_date + datetime.timedelta(days=1)

while current_date <= today:
    day_customers = []
    day_orders = []
    day_order_lines = []
    incr = (current_date - base_date_incr).days * 0.1  # Numeric increment.

    # Generate customers if needed.
    if max_dates["customer.csv"] < current_date:
        for _ in range(random.randint(5, 10)):
            new_customer_id = generate_id("C")
            full_name = f"{random.choice(['Emma','Olivia','Liam','Noah','Ava'])} {random.choice(['Smith','Johnson','Williams'])}"
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
        existing_customer_ids.extend([c["customer_id"] for c in day_customers])

    # Generate orders if needed.
    if max_dates["orders.csv"] < current_date:
        for _ in range(random.randint(3, 7)):
            new_order_id = generate_id("O")
            # In memory, we include extra fields for linking.
            order = {
                "order_id": new_order_id,
                "wdf__client_id": random.choice(MERCHANT_TYPES),
                "order_status": random.choice(["Processed", "Completed", "In Cart", "Canceled"]),
                "order_date": current_date.strftime("%Y-%m-%d"),  # In-memory extra field.
                "customer_id": random.choice(existing_customer_ids),  # In-memory extra field.
            }
            day_orders.append(order)
            new_orders.append(order)
        existing_order_ids.extend([o["order_id"] for o in day_orders])

    # Generate order lines if needed.
    if max_dates["order_lines.csv"] < current_date:
        for order in day_orders:
            for _ in range(random.randint(1, 3)):
                base_price = random.uniform(5, 200)
                base_cost = random.uniform(5, 150)
                order_line = {
                    "order_line_id": generate_id("L"),
                    "order__order_id": order["order_id"],
                    "product__product_id": random.choice(existing_product_ids),
                    "customer__customer_id": order["customer_id"],
                    "order_unit_price": float(round(base_price + incr, 2)),
                    "order_unit_quantity": float(random.randint(1, 5)),
                    "wdf__client_id": order["wdf__client_id"],
                    "order_unit_discount": float(round(random.uniform(0, 50), 2)),
                    "order_unit_cost": float(round(base_cost + incr, 2)),
                    "date": current_date.strftime("%Y-%m-%d %H:%M:%S.000"),
                    "order_date": current_date.strftime("%Y-%m-%d %H:%M:%S.000"),
                    "customer_age": f"{random.randint(18, 70)}M+",
                }
                day_order_lines.append(order_line)
                new_order_lines.append(order_line)
    current_date += datetime.timedelta(days=1)

# --------------------------
# Step 2: Generate Monthly Inventory (Monthly Data)
# --------------------------
new_inventory = []
last_inv_date = max_dates["monthly_inventory.csv"]
if last_inv_date is None:
    last_inv_date = datetime.date(2024, 1, 1)
start_year = last_inv_date.year + (last_inv_date.month // 12)
start_month = (last_inv_date.month % 12) + 1
current_month_date = datetime.date(start_year, start_month, 1)
while current_month_date <= today:
    incr = (current_month_date - base_date_incr).days * 0.1
    for product_id in existing_product_ids:
        base_bom = random.randint(300, 2000)
        base_eom = random.randint(300, 2000)
        inventory_data = {
            "monthly_inventory_id": generate_id("M"),
            "product__product_id": product_id,
            "inventory_month": current_month_date.strftime("%Y-%m-01"),
            "monthly_quantity_eom": float(round(base_eom + incr, 2)),
            "wdf__client_id": random.choice(MERCHANT_TYPES),
            "monthly_quantity_bom": float(round(base_bom + incr, 2)),
            "date": current_month_date.strftime("%Y-%m-%d %H:%M:%S.000"),
        }
        new_inventory.append(inventory_data)
    if current_month_date.month == 12:
        current_month_date = datetime.date(current_month_date.year + 1, 1, 1)
    else:
        current_month_date = datetime.date(current_month_date.year, current_month_date.month + 1, 1)

# --------------------------
# Step 3: Generate New Returns Based on New Orders
# We use the in-memory new_orders (which include extra fields) for generating returns.
new_returns = []
returns_start_date = max_dates["returns.csv"] + datetime.timedelta(days=1)
for order in new_orders:
    order_date_obj = datetime.datetime.strptime(order["order_date"], "%Y-%m-%d").date()
    if order_date_obj >= returns_start_date:
        if random.random() < 0.4:  # 40% chance
            incr = (order_date_obj - base_date_incr).days * 0.1
            new_return = {
                "return_id": generate_id("R"),
                "order__order_id": order["order_id"],
                "product__product_id": random.choice(existing_product_ids),
                "customer__customer_id": order["customer_id"],
                "return_unit_cost": float(round(random.uniform(5, 150) + incr, 2)),
                "return_unit_quantity": float(random.randint(1, 3)),
                "wdf__client_id": order["wdf__client_id"],
                "return_unit_paid_amount": float(round(random.uniform(5, 200) + incr, 2)),
                "date": order["order_date"] + " 00:00:00.000",
                "return_date": order["order_date"] + " 00:00:00.000",
            }
            new_returns.append(new_return)

# --------------------------
# Step 4: Save All Updates, preserving original structure exactly.
# For orders.csv, we save only the original 3 columns.
def update_dataset(dataset, new_data):
    if not new_data:
        print(f"No new data for {dataset}. Skipping update.")
        return
    file_path = os.path.join(DATA_DIR, dataset)
    try:
        df_orig = pl.read_csv(file_path)
        df_new = pl.DataFrame(new_data)
        # For orders.csv, use the original file's columns exactly.
        ref_columns = existing_columns[dataset]
        df_new = df_new.select(ref_columns)
        updated_df = pl.concat([df_orig, df_new])
        updated_df.write_csv(file_path)
        print(f"Updated {dataset} with {len(new_data)} new records.")
    except Exception as e:
        print(f"Error writing {dataset}: {e}")

update_dataset("customer.csv", new_customers)
update_dataset("orders.csv", new_orders)
update_dataset("order_lines.csv", new_order_lines)
update_dataset("returns.csv", new_returns)
update_dataset("monthly_inventory.csv", new_inventory)