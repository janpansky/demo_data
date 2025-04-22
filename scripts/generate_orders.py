import datetime, random, os
import polars as pl
from common import generate_id, read_csv, update_dataset, DATA_DIR

# Path for orders metadata file.
ORDERS_META_FILE = os.path.join(DATA_DIR, "orders_last_date.txt")

def get_last_order_date(current_date):
    """Reads the last generated order date from metadata file.
    If the file doesn't exist, assume no orders have been generated and return yesterday."""
    if os.path.exists(ORDERS_META_FILE):
        try:
            with open(ORDERS_META_FILE, "r") as f:
                date_str = f.read().strip()
                return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            pass
    # Default: assume orders generated up to yesterday.
    return current_date - datetime.timedelta(days=1)

def update_orders_meta(current_date):
    """Write the current_date as the last generated order date."""
    with open(ORDERS_META_FILE, "w") as f:
        f.write(current_date.strftime("%Y-%m-%d"))

def generate_orders(current_date, existing_customer_ids, num_orders_range=(80, 120)):
    """
    Generate new orders for every day from the day after the last recorded order date
    (from metadata) up to current_date.
    Extra linking fields (order_date, customer_id) are generated in memory.
    Returns a list of order dicts.
    """
    last_date = get_last_order_date(current_date)
    new_orders = []
    dt = last_date + datetime.timedelta(days=1)
    while dt <= current_date:
        for _ in range(random.randint(*num_orders_range)):
            new_order_id = generate_id("O")
            order = {
                "order_id": new_order_id,
                "wdf__client_id": random.choice(["merchant__electronics", "merchant__clothing", "merchant__bigboxretailer"]),
                "order_status": random.choice(["Processed", "Completed", "In Cart", "Canceled"]),
                # In-memory fields for linking:
                "order_date": dt.strftime("%Y-%m-%d"),
                "customer_id": random.choice(existing_customer_ids)
            }
            new_orders.append(order)
        dt += datetime.timedelta(days=1)
    return new_orders

if __name__ == "__main__":
    today = datetime.date.today()
    customer_df = read_csv("customer.csv")
    existing_customer_ids = customer_df["customer_id"].to_list()
    new_orders = generate_orders(today, existing_customer_ids)
    print(f"Generated {len(new_orders)} new orders.")
    # When saving orders.csv, we preserve only its original 3 columns.
    update_dataset("orders.csv", new_orders)
    # Update metadata so that subsequent runs know orders are generated up to today.
    update_orders_meta(today)