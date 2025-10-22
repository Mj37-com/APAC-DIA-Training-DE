import os
import argparse
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pathlib import Path

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

# -------------------------------------------------------------------
# DATA GENERATORS (aligned with schemas.py)
# -------------------------------------------------------------------

def generate_customers(n=1000, seed=42):
    np.random.seed(seed)
    first_names = ["John", "Jane", "Alex", "Maria", "Carlos", "Lara", "Michael", "Grace"]
    last_names = ["Smith", "Garcia", "Tan", "Reyes", "Lee", "Cheng", "Lim", "Jones"]

    customers = pd.DataFrame({
        "customer_id": range(1, n + 1),
        "natural_key": [f"CUST_{i:05d}" for i in range(1, n + 1)],
        "first_name": np.random.choice(first_names, n),
        "last_name": np.random.choice(last_names, n),
        "email": [f"user{i}@example.com" for i in range(1, n + 1)],
        "phone": [f"+63{np.random.randint(9000000000, 9999999999)}" for _ in range(n)],
        "address_line1": np.random.choice(["123 Main St", "45 Market Ave", "67 Elm Rd"], n),
        "address_line2": np.random.choice(["Brgy. Central", "Brgy. West", "Brgy. East"], n),
        "city": np.random.choice(["Manila", "Cebu", "Davao", "Baguio"], n),
        "state_region": np.random.choice(["NCR", "Visayas", "Mindanao"], n),
        "postcode": np.random.randint(1000, 9999, n).astype(str),
        "country_code": "PH",
        "latitude": np.random.uniform(5, 20, n),
        "longitude": np.random.uniform(120, 126, n),
        "birth_date": pd.to_datetime("1980-01-01") + pd.to_timedelta(np.random.randint(7000, 16000, n), "D"),
        "join_ts": pd.Timestamp.now() - pd.to_timedelta(np.random.randint(1, 1000, n), "D"),
        "is_vip": np.random.choice([True, False], n, p=[0.2, 0.8]),
        "gdpr_consent": np.random.choice([True, False], n, p=[0.9, 0.1])
    })
    return customers

def generate_products(n=325, seed=42):
    np.random.seed(seed)
    categories = ["Electronics", "Clothing", "Food", "Home"]
    subcategories = {
        "Electronics": ["Phones", "Laptops", "TVs"],
        "Clothing": ["Men", "Women"],
        "Food": ["Snacks", "Beverages"],
        "Home": ["Furniture", "Decor"]
    }

    cat_choices = np.random.choice(categories, n)
    subcat_choices = [np.random.choice(subcategories[c]) for c in cat_choices]

    return pd.DataFrame({
        "product_id": range(1, n + 1),
        "sku": [f"SKU_{i:05d}" for i in range(1, n + 1)],
        "name": [f"Product_{i}" for i in range(1, n + 1)],
        "category": cat_choices,
        "subcategory": subcat_choices,
        "current_price": np.random.uniform(10, 5000, n).round(2),
        "currency": "PHP",
        "is_discontinued": np.random.choice([True, False], n, p=[0.1, 0.9]),
        "introduced_dt": pd.to_datetime("2021-01-01") + pd.to_timedelta(np.random.randint(1, 1000, n), "D"),
        "discontinued_dt": pd.NaT
    })

def generate_stores(n=10):
    np.random.seed(42)
    return pd.DataFrame({
        "store_id": range(1, n + 1),
        "store_code": [f"STORE_{i:03d}" for i in range(1, n + 1)],
        "name": [f"Store_{i}" for i in range(1, n + 1)],
        "channel": np.random.choice(["Online", "Retail"], n),
        "region": np.random.choice(["North", "South", "East", "West"], n),
        "state": np.random.choice(["NCR", "Region VII", "Region XI"], n),
        "latitude": np.random.uniform(5, 20, n),
        "longitude": np.random.uniform(120, 126, n),
        "open_dt": pd.to_datetime("2015-01-01") + pd.to_timedelta(np.random.randint(1, 2000, n), "D"),
        "close_dt": pd.NaT
    })

def generate_suppliers(n=10):
    np.random.seed(42)
    return pd.DataFrame({
        "supplier_id": range(1, n + 1),
        "supplier_code": [f"SUP_{i:04d}" for i in range(1, n + 1)],
        "name": [f"Supplier_{i}" for i in range(1, n + 1)],
        "country_code": np.random.choice(["PH", "SG", "JP", "US"], n),
        "lead_time_days": np.random.randint(3, 15, n),
        "preferred": np.random.choice([True, False], n, p=[0.6, 0.4])
    })

def generate_orders(customers, stores, seed=42):
    np.random.seed(seed)
    n = 200
    order_ids = range(1, n + 1)
    order_ts = pd.date_range("2024-01-01", periods=n, freq="D")
    header = pd.DataFrame({
        "order_id": order_ids,
        "order_ts": order_ts,
        "order_dt_local": order_ts.date,
        "customer_id": np.random.choice(customers["customer_id"], n),
        "store_id": np.random.choice(stores["store_id"], n),
        "channel": np.random.choice(["Online", "Retail"], n),
        "payment_method": np.random.choice(["Credit Card", "GCash", "COD"], n),
        "coupon_code": np.random.choice(["DISC10", "NONE", "FREESHIP"], n),
        "shipping_fee": np.random.uniform(50, 500, n).round(2),
        "currency": "PHP"
    })

    lines = pd.DataFrame({
        "order_id": np.repeat(order_ids, 2),
        "line_number": [1, 2] * n,
        "product_id": np.random.randint(1, 325, n * 2),
        "qty": np.random.randint(1, 10, n * 2),
        "unit_price": np.random.uniform(100, 1000, n * 2).round(2),
        "line_discount_pct": np.random.uniform(0, 0.3, n * 2).round(4),
        "tax_pct": np.random.uniform(0.05, 0.12, n * 2).round(4)
    })

    return header, lines

def generate_shipments(orders):
    np.random.seed(42)
    n = len(orders)
    ship_dates = pd.to_datetime(orders["order_dt_local"]) + pd.to_timedelta(np.random.randint(1, 5, n), "D")
    return pd.DataFrame({
        "shipment_id": range(1, n + 1),
        "order_id": orders["order_id"],
        "carrier": np.random.choice(["LBC", "J&T", "NinjaVan"], n),
        "shipped_at": ship_dates,
        "delivered_at": ship_dates + pd.to_timedelta(np.random.randint(1, 3, n), "D"),
        "ship_cost": np.random.uniform(50, 500, n).round(2)
    })

def generate_returns(orders):
    np.random.seed(42)
    m = len(orders) // 4
    return pd.DataFrame({
        "return_id": range(1, m + 1),
        "order_id": np.random.choice(orders["order_id"], m),
        "product_id": np.random.randint(1, 325, m),
        "return_ts": pd.Timestamp("2024-07-01") + pd.to_timedelta(np.random.randint(1, 10, m), "D"),
        "qty": np.random.randint(1, 3, m),
        "reason": np.random.choice(["Defective", "Wrong Item", "Late Delivery"], m)
    })

def generate_exchange_rates():
    np.random.seed(42)
    currencies = ["USD", "PHP", "SGD", "JPY"]
    dates = pd.date_range("2024-01-01", periods=365)
    data = []
    for cur in currencies:
        for date in dates:
            rate = np.random.uniform(0.5, 60)
            data.append((date, cur, rate))
    return pd.DataFrame(data, columns=["date", "currency", "rate_to_aud"])

def generate_sensors(n=100):
    np.random.seed(42)
    return pd.DataFrame({
        "sensor_ts": pd.date_range("2024-01-01", periods=n, freq="H"),
        "store_id": np.random.randint(1, 11, n),
        "shelf_id": [f"SHELF_{i:03d}" for i in range(1, n + 1)],
        "temperature_c": np.random.uniform(20, 35, n).round(2),
        "humidity_pct": np.random.uniform(40, 80, n).round(2),
        "battery_mv": np.random.randint(3500, 4200, n)
    })

def generate_events():
    np.random.seed(42)
    event_types = ["click", "view", "purchase", "add_to_cart"]
    events = []
    for i in range(100):
        event = {
            "json": json.dumps({
                "event_id": i + 1,
                "timestamp": (datetime.now() - timedelta(minutes=np.random.randint(0, 10000))).isoformat(),
                "event_type": np.random.choice(event_types),
                "customer_id": np.random.randint(1, 1000)
            })
        }
        events.append(event)
    return pd.DataFrame(events)

# -------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="scripts/data_raw")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    out_dir = Path(args.out)
    ensure_dir(out_dir)

    customers = generate_customers(seed=args.seed)
    products = generate_products(seed=args.seed)
    stores = generate_stores()
    suppliers = generate_suppliers()
    orders_header, orders_lines = generate_orders(customers, stores, seed=args.seed)
    shipments = generate_shipments(orders_header)
    returns = generate_returns(orders_header)
    exchange_rates = generate_exchange_rates()
    sensors = generate_sensors()
    events = generate_events()

    # --- Write outputs ---
    customers.to_csv(out_dir / "customers.csv", index=False)
    products.to_csv(out_dir / "products.csv", index=False)
    stores.to_csv(out_dir / "stores.csv", index=False)
    suppliers.to_csv(out_dir / "suppliers.csv", index=False)
    orders_header.to_csv(out_dir / "orders_header.csv", index=False)
    orders_lines.to_csv(out_dir / "orders_lines.csv", index=False)
    shipments.to_parquet(out_dir / "shipments.parquet", index=False)
    returns.to_parquet(out_dir / "returns_day1.parquet", index=False)
    exchange_rates.to_parquet(out_dir / "exchange_rates.parquet", index=False)
    sensors.to_parquet(out_dir / "sensors.parquet", index=False)
    events.to_json(out_dir / "events.json", orient="records", indent=2)

    print(f"✅ Raw data generated under: {out_dir}")
    for f in sorted(os.listdir(out_dir)):
        print(f" - {f}")

if __name__ == "__main__":
    main()
