import datetime, random
import polars as pl
from common import generate_id, read_csv

def generate_customers(today, customer_locations, merchant_types, existing_customer_ids, num_customers_range=(10, 20)):
    """
    Generate new customer records for every day between the last recorded date in customer.csv and today.
    Returns a list of customer dicts.
    """
    df = read_csv("customer.csv")
    # Determine the last recorded date in the file.
    if df.height > 0 and "customer_created_date" in df.columns:
        max_date_str = df["customer_created_date"].drop_nulls().max()
        try:
            last_date = datetime.datetime.strptime(max_date_str, "%Y-%m-%d").date()
        except Exception:
            last_date = today - datetime.timedelta(days=1)
    else:
        # If file is empty or no date column found, assume yesterday as last date.
        last_date = today - datetime.timedelta(days=1)

    new_customers = []
    # Loop from the day after last_date to today (inclusive)
    dt = last_date + datetime.timedelta(days=1)
    while dt <= today:
        # (Optional) You could add a check here to see if dt is already present in the file.
        for _ in range(random.randint(*num_customers_range)):
            new_customer_id = generate_id("C")
            full_name = f"{random.choice(['Emma','Olivia','Liam','Noah','Ava','James','Mark'])} {random.choice(['Smith','Johnson','Williams'])}"
            location = random.choice(customer_locations)
            merchant_type = random.choice(merchant_types)
            customer = {
                "customer_id": new_customer_id,
                "ls__customer_id__customer_name": full_name,
                "customer_city": location["customer_city"],
                "geo__customer_city__city_pushpin_longitude": location["geo__customer_city__city_pushpin_longitude"],
                "geo__customer_city__city_pushpin_latitude": location["geo__customer_city__city_pushpin_latitude"],
                "customer_country": location["customer_country"],
                "customer_email": f"{full_name.replace(' ', '.').lower()}@example.com",
                "customer_state": location["customer_state"],
                "customer_created_date": dt.strftime("%Y-%m-%d"),
                "wdf__client_id": merchant_type,
            }
            new_customers.append(customer)
            existing_customer_ids.append(new_customer_id)
        dt += datetime.timedelta(days=1)
    return new_customers

if __name__ == "__main__":
    from common import read_csv, update_dataset
    today = datetime.date.today()
    customer_df = read_csv("customer.csv")
    existing_customer_ids = customer_df["customer_id"].to_list()
    customer_locations = customer_df.select([
        "customer_city", "customer_state", "customer_country",
        "geo__customer_city__city_pushpin_longitude",
        "geo__customer_city__city_pushpin_latitude"
    ]).unique().to_dicts()
    merchant_types = ["merchant__electronics", "merchant__clothing", "merchant__bigboxretailer"]

    new_customers = generate_customers(today, customer_locations, merchant_types, existing_customer_ids)
    print(f"Generated {len(new_customers)} new customers since last update up to today.")
    update_dataset("customer.csv", new_customers)