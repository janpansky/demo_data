import datetime
import random
from common import generate_id, read_csv, update_dataset


def generate_customers(today, customer_locations, merchant_types, existing_customer_ids, num_customers_range=(10, 20)):
    df = read_csv("customer.csv")
    if df.height > 0 and "customer_created_date" in df.columns:
        max_date_str = df["customer_created_date"].drop_nulls().max()
        try:
            last_date = datetime.datetime.strptime(max_date_str, "%Y-%m-%d").date()
        except Exception:
            last_date = today - datetime.timedelta(days=1)
    else:
        last_date = today - datetime.timedelta(days=1)

    existing_emails = set(df["customer_email"].to_list()) if "customer_email" in df.columns else set()

    first_names = ['Emma', 'Olivia', 'Liam', 'Noah', 'Ava', 'James', 'Mark']
    last_names = ['Smith', 'Johnson', 'Williams']

    new_customers = []
    dt = last_date + datetime.timedelta(days=1)
    while dt <= today:
        for _ in range(random.randint(*num_customers_range)):
            new_customer_id = generate_id("C")

            # Generate unique email
            attempt = 0
            while True:
                first = random.choice(first_names)
                last = random.choice(last_names)
                email = f"{first.lower()}.{last.lower()}_{random.randint(1, 9999)}@example.com"
                if email not in existing_emails or attempt > 10:
                    existing_emails.add(email)
                    break
                attempt += 1

            full_name = f"{first} {last}"
            location = random.choice(customer_locations)
            merchant_type = random.choice(merchant_types)
            customer = {
                "customer_id": new_customer_id,
                "ls__customer_id__customer_name": full_name,
                "customer_city": location["customer_city"],
                "geo__customer_city__city_pushpin_longitude": location["geo__customer_city__city_pushpin_longitude"],
                "geo__customer_city__city_pushpin_latitude": location["geo__customer_city__city_pushpin_latitude"],
                "customer_country": location["customer_country"],
                "customer_email": email,
                "customer_state": location["customer_state"],
                "customer_created_date": dt.strftime("%Y-%m-%d"),
                "wdf__client_id": merchant_type,
            }
            new_customers.append(customer)
            existing_customer_ids.append(new_customer_id)
        dt += datetime.timedelta(days=1)
    return new_customers


if __name__ == "__main__":
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