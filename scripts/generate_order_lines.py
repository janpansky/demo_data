import datetime
import random
import os
import polars as pl
from common import generate_id, read_csv, update_dataset, write_deltas_to_s3


def get_last_order_line_date():
    try:
        df = read_csv("order_lines.csv")
        df = df.with_columns(
            pl.col("order_date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.3f")
        )
        last_date = df["order_date"].max().date()
        return last_date
    except Exception as e:
        print(f"Could not read last order line date: {e}")
        return datetime.date.today() - datetime.timedelta(days=1)


def generate_order_lines(from_date, to_date, orders_df, existing_product_ids, existing_customer_ids, num_order_lines_range=(8, 13)):
    new_order_lines = []
    current_date = from_date + datetime.timedelta(days=1)

    while current_date <= to_date:
        orders_for_day = orders_df.sample(n=int(orders_df.height * 0.01)) if orders_df.height > 0 else pl.DataFrame(schema=orders_df.schema)

        if orders_for_day.height == 0:
            print(f"No orders available for {current_date}. Skipping.")
            current_date += datetime.timedelta(days=1)
            continue

        incr = (current_date - datetime.date(2022, 1, 1)).days * 0.1
        daily_generated_lines = 0

        for order in orders_for_day.iter_rows(named=True):
            for _ in range(random.randint(*num_order_lines_range)):
                base_price = random.uniform(5, 200)
                base_cost = random.uniform(5, 150)
                order_line = {
                    "order_line_id": generate_id("L"),
                    "order__order_id": order["order_id"],
                    "product__product_id": random.choice(existing_product_ids),
                    "customer__customer_id": order["customer_id"],
                    "order_unit_price": round(base_price + incr, 2),
                    "order_unit_quantity": float(random.randint(1, 5)),
                    "wdf__client_id": order["wdf__client_id"],
                    "order_unit_discount": round(random.uniform(0, 50), 2),
                    "order_unit_cost": round(base_cost + incr, 2),
                    "date": current_date.strftime("%Y-%m-%d %H:%M:%S.000"),
                    "order_date": current_date.strftime("%Y-%m-%d %H:%M:%S.000"),
                    "customer_age": f"{random.randint(18, 70)}M+",
                }
                new_order_lines.append(order_line)
                daily_generated_lines += 1

        print(f"Generated {daily_generated_lines} order lines for {current_date}.")
        current_date += datetime.timedelta(days=1)

    return new_order_lines


if __name__ == "__main__":
    today = datetime.date.today()

    base_df = read_csv("orders.csv")

    # Try to merge in delta rows if present
    try:
        delta_df = read_csv("deltas/orders.csv")
        orders_df = pl.concat([base_df, delta_df]).unique(subset=["order_id"])
        print("✅ Merged base and delta orders.csv from S3")
    except Exception:
        orders_df = base_df

    customers_df = read_csv("customer.csv")
    product_df = read_csv("product.csv")

    existing_product_ids = product_df["product_id"].to_list()
    existing_customer_ids = customers_df["customer_id"].to_list()

    if "customer_id" not in orders_df.columns:
        orders_df = orders_df.with_columns(
            pl.Series("customer_id", [random.choice(existing_customer_ids) for _ in range(orders_df.height)])
        )

    last_date = get_last_order_line_date()
    print(f"Last order line date detected: {last_date}")

    new_order_lines = generate_order_lines(last_date, today, orders_df, existing_product_ids, existing_customer_ids)

    if new_order_lines:
        df = pl.DataFrame(new_order_lines)
        if os.getenv("USE_S3", "false").lower() == "true":
            write_deltas_to_s3(df, "order_lines.csv")
        else:
            update_dataset("order_lines.csv", new_order_lines)
        print(f"Updated order_lines.csv with {len(new_order_lines)} new records.")
    else:
        print("No new order lines generated.")
