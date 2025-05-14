import datetime
import random
import os

import polars as pl

from common import generate_id, read_csv, update_dataset


def get_last_return_date():
    try:
        df = read_csv("returns.csv")
        df = df.with_columns(
            pl.col("return_date").str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S%.3f")
        )
        return df["return_date"].max()
    except Exception as e:
        print(f"Could not read last return date: {e}")
        return datetime.date.today() - datetime.timedelta(days=1)


def generate_returns(from_date, to_date, order_lines_df, existing_product_ids, existing_order_ids,
                     existing_customer_ids):
    new_returns = []
    current_date = from_date + datetime.timedelta(days=1)

    while current_date <= to_date:
        orders_for_day = order_lines_df.filter(
            pl.col("order_date_parsed") == current_date
        )

        if orders_for_day.height == 0:
            print(f"No orders found for {current_date}. Skipping.")
            current_date += datetime.timedelta(days=1)
            continue

        incr = (current_date - datetime.date(2022, 1, 1)).days * 0.1
        daily_count = 0

        for order in orders_for_day.iter_rows(named=True):
            if order["order__order_id"] not in existing_order_ids or order["customer__customer_id"] not in existing_customer_ids:
                continue

            if random.random() < 0.4:
                ret = {
                    "return_id": generate_id("R"),
                    "order__order_id": order["order__order_id"],
                    "product__product_id": order["product__product_id"],
                    "customer__customer_id": order["customer__customer_id"],
                    "return_unit_cost": float(round(random.uniform(5, 150) + incr, 2)),
                    "return_unit_quantity": float(random.randint(1, 3)),
                    "wdf__client_id": order["wdf__client_id"],
                    "return_unit_paid_amount": float(round(random.uniform(5, 200) + incr, 2)),
                    "date": current_date.strftime("%Y-%m-%d 00:00:00.000"),
                    "return_date": current_date.strftime("%Y-%m-%d 00:00:00.000"),
                }
                new_returns.append(ret)
                daily_count += 1

        print(f"Generated {daily_count} returns for {current_date}.")
        current_date += datetime.timedelta(days=1)

    return new_returns


if __name__ == "__main__":
    today = datetime.date.today()

    order_lines_df = read_csv("order_lines.csv")
    order_lines_df = order_lines_df.with_columns(
        pl.col("order_date").str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S%.3f").alias("order_date_parsed")
    )

    product_df = read_csv("product.csv")
    existing_product_ids = product_df["product_id"].to_list()
    orders_df = read_csv("orders.csv")
    customers_df = read_csv("customer.csv")
    existing_order_ids = set(orders_df["order_id"].to_list())
    existing_customer_ids = set(customers_df["customer_id"].to_list())

    last_return_date = get_last_return_date()
    print(f"Last return date detected: {last_return_date} — Generating up to: {today}")

    if last_return_date >= today:
        print("✅ Returns already up-to-date. Skipping generation.")
    else:
        new_returns = generate_returns(last_return_date, today, order_lines_df, existing_product_ids, existing_order_ids,
                                       existing_customer_ids)

        if new_returns:
            update_dataset("returns.csv", new_returns)
            print(f"Updated returns.csv with {len(new_returns)} new records.")
        else:
            print("No new returns generated.")
