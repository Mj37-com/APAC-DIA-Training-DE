import os
import argparse
import pandas as pd
import numpy as np
from faker import Faker

fake = Faker()

def generate_customers(num_rows):
    data = []
    for i in range(num_rows):
        data.append({
            "customer_id": i + 1,
            "natural_key": f"CUST-{fake.bothify(text='########')}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=75),
            "address_line1": fake.street_address(),
            "address_line2": fake.secondary_address(),
            "city": fake.city(),
            "state_region": fake.state(),
            "postcode": fake.postcode(),
            "country_code": fake.country_code(),
            "latitude": fake.latitude(),
            "longitude": fake.longitude()
        })
    return pd.DataFrame(data)

def generate_products(num_rows):
    categories = ["Electronics", "Fashion", "Home", "Sports", "Toys"]
    data = []
    for i in range(num_rows):
        data.append({
            "product_id": i + 1,
            "sku": f"SKU-{fake.bothify(text='???-#####')}",
            "name": fake.word().capitalize(),
            "category": np.random.choice(categories),
            "price": round(np.random.uniform(5, 1500), 2),
            "currency": "USD",
            "updated_at": fake.date_time_this_year()
        })
    return pd.DataFrame(data)

def generate_orders(customers_count, products_count, num_rows):
    data = []
    for i in range(num_rows):
        data.append({
            "order_id": i + 1,
            "customer_id": np.random.randint(1, customers_count + 1),
            "product_id": np.random.randint(1, products_count + 1),
            "quantity": np.random.randint(1, 5),
            "order_date": fake.date_time_this_year(),
            "status": np.random.choice(["Pending", "Shipped", "Delivered", "Cancelled"]),
            "discount": round(np.random.uniform(0, 0.3), 2),
            "updated_at": fake.date_time_this_year()
        })
    return pd.DataFrame(data)

def generate_events(customers_count, num_rows):
    event_types = ["login", "logout", "purchase", "view_product", "add_to_cart"]
    data = []
    for i in range(num_rows):
        data.append({
            "event_id": i + 1,
            "customer_id": np.random.randint(1, customers_count + 1),
            "event_type": np.random.choice(event_types),
            "event_date": fake.date_time_this_year(),
            "amount": round(np.random.uniform(0, 200), 2),
            "updated_at": fake.date_time_this_year()
        })
    return pd.DataFrame(data)

def write_csv(df, output_path, name):
    filepath = os.path.join(output_path, name)
    df.to_csv(filepath, index=False)
    print(f"✅ Created: {filepath} ({len(df)} rows)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, help="Output folder for all generated datasets")
    parser.add_argument("--customers", type=int, default=80000)
    parser.add_argument("--products", type=int, default=5000)
    parser.add_argument("--orders", type=int, default=500000)
    parser.add_argument("--events", type=int, default=2000000)
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print("\n🚀 Generating data...")

    customers_df = generate_customers(args.customers)
    write_csv(customers_df, args.output, "customers.csv")

    products_df = generate_products(args.products)
    write_csv(products_df, args.output, "products.csv")

    orders_df = generate_orders(args.customers, args.products, args.orders)
    write_csv(orders_df, args.output, "orders.csv")

    events_df = generate_events(args.customers, args.events)
    write_csv(events_df, args.output, "events.csv")

    print("\n🎉 Done! All datasets generated successfully.")
