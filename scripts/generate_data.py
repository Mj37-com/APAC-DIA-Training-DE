import os
import json
import numpy as np
import pandas as pd
from pathlib import Path
from faker import Faker

fake = Faker()

# ============================================================
# Utility: Random helpers
# ============================================================
def random_date_range(start, end, n):
    """Generate n random datetimes between start and end."""
    return pd.to_datetime(np.random.randint(int(start.value // 10**9),
                                            int(end.value // 10**9),
                                            n), unit='s')

# ============================================================
# Customers
# ============================================================
def generate_customers(n=80_000):
    print("Generating customers...")
    df = pd.DataFrame({
        "customer_id": np.arange(1, n + 1),
        "natural_key": [f"CUST-{fake.bothify(text='????????').upper()}" for _ in range(n)],
        "first_name": [fake.first_name() for _ in range(n)],
        "last_name": [fake.last_name() for _ in range(n)],
        "email": [fake.email() for _ in range(n)],
        "phone": [fake.phone_number() for _ in range(n)],
        "birth_date": [fake.date_of_birth(minimum_age=18, maximum_age=80) for _ in range(n)],
        "address_line1": [fake.street_address() for _ in range(n)],
        "address_line2": [fake.secondary_address() for _ in range(n)],
        "city": [fake.city() for _ in range(n)],
        "state_region": [fake.state() for _ in range(n)],
        "postcode": [fake.postcode() for _ in range(n)],
        "country_code": [fake.country_code() for _ in range(n)],
        "latitude": [fake.latitude() for _ in range(n)],
        "longitude": [fake.longitude() for _ in range(n)],
    })
    return df

# ============================================================
# Products
# ============================================================
def generate_products(n=2000):
    print("Generating products...")
    df = pd.DataFrame({
        "product_id": np.arange(1, n + 1),
        "natural_key": [f"PROD-{fake.bothify(text='????????').upper()}" for _ in range(n)],
        "name": [fake.word().capitalize() for _ in range(n)],
        "category": np.random.choice(["Electronics", "Clothing", "Toys", "Home", "Sports"], n),
        "price": np.random.uniform(5, 1000, n).round(2),
        "currency": "USD",
    })
    return df

# ============================================================
# Stores
# ============================================================
def generate_stores(n=200):
    print("Generating stores...")
    df = pd.DataFrame({
        "store_id": np.arange(1, n + 1),
        "natural_key": [f"STORE-{fake.bothify(text='??????').upper()}" for _ in range(n)],
        "name": [fake.company() for _ in range(n)],
        "city": [fake.city() for _ in range(n)],
        "country_code": [fake.country_code() for _ in range(n)],
        "latitude": [fake.latitude() for _ in range(n)],
        "longitude": [fake.longitude() for _ in range(n)],
    })
    return df

# ============================================================
# Suppliers
# ============================================================
def generate_suppliers(n=500):
    print("Generating suppliers...")
    df = pd.DataFrame({
        "supplier_id": np.arange(1, n + 1),
        "natural_key": [f"SUP-{fake.bothify(text='??????').upper()}" for _ in range(n)],
        "name": [fake.company() for _ in range(n)],
        "contact_name": [fake.name() for _ in range(n)],
        "phone": [fake.phone_number() for _ in range(n)],
        "email": [fake.company_email() for _ in range(n)],
        "country_code": [fake.country_code() for _ in range(n)],
    })
    return df

# ============================================================
# Orders
# ============================================================
def generate_orders(n_orders=500_000):
    print("Generating orders (headers + lines) ... this may take time for large counts")
    customers = np.arange(1, 80_001)
    stores = np.arange(1, 201)
    products = np.arange(1, 2001)

    order_dates = pd.date_range("2024-01-01", periods=365, freq="d")
    orders = pd.DataFrame({
        "order_id": np.arange(1, n_orders + 1),
        "customer_id": np.random.choice(customers, n_orders),
        "store_id": np.random.choice(stores, n_orders),
        "order_date": np.random.choice(order_dates, n_orders),
        "total_amount": np.random.uniform(10, 5000, n_orders).round(2),
    })

    n_lines = n_orders * 2
    lines = pd.DataFrame({
        "order_line_id": np.arange(1, n_lines + 1),
        "order_id": np.random.choice(orders["order_id"], n_lines),
        "product_id": np.random.choice(products, n_lines),
        "quantity": np.random.randint(1, 10, n_lines),
        "unit_price": np.random.uniform(5, 1000, n_lines).round(2),
    })
    return orders, lines

# ============================================================
# Shipments
# ============================================================
def generate_shipments(n=200_000):
    print("Generating shipments...")
    df = pd.DataFrame({
        "shipment_id": np.arange(1, n + 1),
        "order_id": np.random.randint(1, 500_001, n),
        "shipped_date": pd.date_range("2024-01-01", periods=n, freq="h"),
        "delivery_date": pd.date_range("2024-01-02", periods=n, freq="h"),
        "carrier": np.random.choice(["FedEx", "UPS", "DHL"], n),
    })
    return df

# ============================================================
# Returns
# ============================================================
def generate_returns(n=50_000):
    print("Generating returns (v1/v2 + upsert/delete)...")
    df = pd.DataFrame({
        "return_id": np.arange(1, n + 1),
        "order_id": np.random.randint(1, 500_001, n),
        "product_id": np.random.randint(1, 2001, n),
        "return_reason": np.random.choice(["Damaged", "Wrong Item", "Not Needed", "Late Delivery"], n),
        "return_ts": pd.date_range("2024-07-01", periods=n, freq="h"),
    })
    return df

# ============================================================
# Exchange Rates (Excel)
# ============================================================
def generate_exchange_rates(out_dir):
    print("Generating exchange rates (XLSX)...")
    currencies = ["USD", "EUR", "GBP", "JPY", "AUD"]
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=365, freq="d"),
    })
    for cur in currencies:
        df[cur] = np.random.uniform(0.5, 1.5, len(df)).round(4)
    df.to_excel(out_dir / "exchange_rates.xlsx", index=False, engine="openpyxl")
    return df

# ============================================================
# Events (JSONL)
# ============================================================
def generate_events(out_dir, total_events=2_000_000, seed=42):
    np.random.seed(seed)
    out_dir = Path(out_dir) / "events"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Generating events (JSONL partitions)...")

    months = pd.date_range("2024-01-01", periods=12, freq="MS")
    event_types = ["click", "view", "purchase", "return", "add_to_cart"]
    customer_ids = np.arange(1, 80_001)
    product_ids = np.arange(1, 2001)

    for month_dt in months:
        month_str = month_dt.strftime("%Y-%m")
        file_path = out_dir / f"events_{month_str}.jsonl"
        times = pd.date_range(month_dt, periods=200, freq="h")

        n_events = total_events // len(months)
        events = []
        for _ in range(n_events):
            obj = {
                "event_id": int(np.random.randint(1e9)),
                "event_type": np.random.choice(event_types),
                "customer_id": int(np.random.choice(customer_ids)),
                "product_id": int(np.random.choice(product_ids)),
                "event_ts": str(np.random.choice(times)),
                "metadata": {
                    "device": np.random.choice(["mobile", "desktop", "tablet"]),
                    "channel": np.random.choice(["email", "social", "organic", "paid"]),
                },
            }
            events.append(obj)

        with open(file_path, "w", encoding="utf-8") as f:
            for obj in events:
                s = json.dumps(obj)
                f.write(s + "\n")

    print("✅ Event JSONL files generated successfully.")

# ============================================================
# Main
# ============================================================
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="scripts/data_raw")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    np.random.seed(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    customers = generate_customers()
    products = generate_products()
    stores = generate_stores()
    suppliers = generate_suppliers()
    orders, lines = generate_orders()
    shipments = generate_shipments()
    returns = generate_returns()

    customers.to_csv(out / "customers.csv", index=False)
    products.to_csv(out / "products.csv", index=False)
    stores.to_csv(out / "stores.csv", index=False)
    suppliers.to_csv(out / "suppliers.csv", index=False)
    orders.to_csv(out / "orders.csv", index=False)
    lines.to_csv(out / "order_lines.csv", index=False)
    shipments.to_csv(out / "shipments.csv", index=False)
    returns.to_csv(out / "returns.csv", index=False)

    generate_exchange_rates(out_dir=out)
    generate_events(out_dir=out, total_events=2_000_000, seed=args.seed)

    print("✅ All data generated successfully!")

if __name__ == "__main__":
    main()
