import datetime
import os
import random

from common import generate_id, read_csv, update_dataset, DATA_DIR, get_last_order_date_s3, update_orders_meta_s3

ORDERS_META_FILE = os.path.join(DATA_DIR, "orders_last_date.txt")


def get_last_order_date(current_date):
    if os.getenv("USE_S3", "false").lower() == "true":
        return get_last_order_date_s3()
    if os.path.exists(ORDERS_META_FILE):
        try:
            with open(ORDERS_META_FILE, "r") as f:
                date_str = f.read().strip()
                return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            pass
    return current_date - datetime.timedelta(days=1)


def update_orders_meta(current_date):
    if os.getenv("USE_S3", "false").lower() == "true":
        return update_orders_meta_s3(current_date)
    with open(ORDERS_META_FILE, "w") as f:
        f.write(current_date.strftime("%Y-%m-%d"))


def generate_orders(current_date, existing_customer_ids, num_orders_range=(80, 120)):
    last_date = get_last_order_date(current_date)
    new_orders = []
    dt = last_date + datetime.timedelta(days=1)
    while dt <= current_date:
        for _ in range(random.randint(*num_orders_range)):
            new_order_id = generate_id("O")
            order = {
                "order_id": new_order_id,
                "wdf__client_id": random.choice([
                    "merchant__electronics",
                    "merchant__clothing",
                    "merchant__bigboxretailer"
                ]),
                "order_status": random.choice([
                    "Processed",
                    "Completed",
                    "In Cart",
                    "Canceled"
                ]),
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
    update_dataset("orders.csv", new_orders)
    update_orders_meta(today)
