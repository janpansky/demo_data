import datetime, random
from common import generate_id, read_csv, update_dataset, write_deltas_to_s3
import polars as pl

def generate_monthly_inventory(today, existing_product_ids):
    df = read_csv("monthly_inventory.csv")
    current_month = today.replace(day=1).strftime("%Y-%m-01")
    if current_month in df["inventory_month"].to_list():
        print("Monthly inventory already generated for this month. Skipping inventory generation.")
        return []

    new_inventory = []
    if df.height > 0 and "date" in df.columns:
        last_inv_val = df["date"].drop_nulls().max()
        try:
            last_inv_date = datetime.datetime.strptime(last_inv_val, "%Y-%m-%d").date()
        except ValueError:
            last_inv_date = datetime.datetime.strptime(last_inv_val.split()[0], "%Y-%m-%d").date()
    else:
        last_inv_date = datetime.date(2024, 1, 1)

    start_year = last_inv_date.year + (last_inv_date.month // 12)
    start_month = (last_inv_date.month % 12) + 1
    current_month_date = datetime.date(start_year, start_month, 1)

    while current_month_date <= today:
        incr = (current_month_date - datetime.date(2022, 1, 1)).days * 0.1
        for product_id in existing_product_ids:
            base_bom = random.randint(300, 2000)
            base_eom = random.randint(300, 2000)
            inv = {
                "monthly_inventory_id": generate_id("M"),
                "product__product_id": product_id,
                "inventory_month": current_month_date.strftime("%Y-%m-01"),
                "monthly_quantity_eom": float(round(base_eom + incr, 2)),
                "wdf__client_id": random.choice(
                    ["merchant__electronics", "merchant__clothing", "merchant__bigboxretailer"]),
                "monthly_quantity_bom": float(round(base_bom + incr, 2)),
                "date": current_month_date.strftime("%Y-%m-%d %H:%M:%S.000"),
            }
            new_inventory.append(inv)
        if current_month_date.month == 12:
            current_month_date = datetime.date(current_month_date.year + 1, 1, 1)
        else:
            current_month_date = datetime.date(current_month_date.year, current_month_date.month + 1, 1)
    return new_inventory

if __name__ == "__main__":
    today = datetime.date.today()
    product_df = read_csv("product.csv")
    existing_product_ids = product_df["product_id"].to_list()
    new_inventory = generate_monthly_inventory(today, existing_product_ids)
    print(f"Generated {len(new_inventory)} new monthly inventory records.")

    if new_inventory:
        df = pl.DataFrame(new_inventory)
        if os.getenv("USE_S3", "false").lower() == "true":
            write_deltas_to_s3(df, "monthly_inventory.csv")
        else:
            update_dataset("monthly_inventory.csv", new_inventory)
