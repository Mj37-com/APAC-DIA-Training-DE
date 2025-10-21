import os
import argparse
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pathlib import Path

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def generate_customers(n=50, seed=42):
    np.random.seed(seed)
    return pd.DataFrame({
        "customer_id": range(1, n + 1),
        "customer_name": [f"Customer_{i}" for i in range(1, n + 1)],
        "region": np.random.choice(["North", "South", "East", "West"], n),
        "join_date": pd.date_range("2023-01-01", periods=n).strftime("%Y-%m-%d")
    })

def generate_products(n=20, seed=42):
    np.random.seed(seed)
    return pd.DataFrame({
        "product_id": range(1, n + 1),
        "product_name": [f"Product_{i}" for i in range(1, n + 1)],
        "category": np.random.choice(["Electronics", "Clothing", "Food", "Home"], n),
        "price": np.random.randint(100, 1000, n)
    })

def generate_stores(n=10):
    return pd.DataFrame({
        "store_id": range(1, n + 1),
        "store_name": [f"Store_{i}" for i in range(1, n + 1)],
        "city": np.random.choice(["Manila", "Cebu", "Davao", "Baguio"], n)
    })

def generate_suppliers(n=10):
    return pd.DataFrame({
        "supplier_id": range(1, n + 1),
        "supplier_name": [f"Supplier_{i}" for i in range(1, n + 1)],
        "country": np.random.choice(["PH", "SG", "US", "JP"], n)
    })

def generate_orders(customers, stores, seed=42):
    np.random.seed(seed)
    n = 200
    order_ids = range(1, n + 1)
    header = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": np.random.choice(customers["customer_id"], n),
        "store_id": np.random.choice(stores["store_id"], n),
        "order_date": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
        "total_amount": np.random.uniform(500, 10000, n).round(2)
    })
    lines = pd.DataFrame({
        "order_id": np.repeat(order_ids, 2),
        "product_id": np.random.randint(1, 21, n * 2),
        "quantity": np.random.randint(1, 10, n * 2),
        "unit_price": np.random.randint(100, 1000, n * 2)
    })
    return header, lines

def generate_shipments(orders):
    n = len(orders)
    return pd.DataFrame({
        "shipment_id": range(1, n + 1),
        "order_id": orders["order_id"],
        "ship_date": pd.to_datetime(orders["order_date"]) + pd.to_timedelta(np.random.randint(1, 7, n), "D"),
        "status": np.random.choice(["In Transit", "Delivered", "Returned"], n)
    })

def generate_returns(orders):
    return pd.DataFrame({
        "return_id": range(1, len(orders)//4 + 1),
        "order_id": np.random.choice(orders["order_id"], len(orders)//4),
        "return_date": pd.to_datetime("2024-07-01") + pd.to_timedelta(np.random.randint(1, 20, len(orders)//4), "D"),
        "reason": np.random.choice(["Defective", "Wrong Item", "Late Delivery"], len(orders)//4)
    })

def generate_exchange_rates():
    dates = pd.date_range("2024-01-01", periods=365)
    return pd.DataFrame({
        "date": dates,
        "USD_PHP": 55 + np.random.normal(0, 0.5, len(dates)),
        "USD_SGD": 1.34 + np.random.normal(0, 0.01, len(dates))
    })

def generate_sensors():
    times = pd.date_range("2024-01-01", periods=100, freq="H")
    return pd.DataFrame({
        "sensor_id": np.random.randint(1000, 1100, len(times)),
        "timestamp": times,
        "temperature": np.random.uniform(20, 35, len(times)),
        "humidity": np.random.uniform(40, 80, len(times))
    })

def generate_events():
    events = []
    event_types = ["click", "view", "purchase", "add_to_cart"]
    for i in range(100):
        events.append({
            "event_id": i + 1,
            "timestamp": (datetime.now() - timedelta(minutes=np.random.randint(0, 10000))).isoformat(),
            "event_type": np.random.choice(event_types),
            "customer_id": np.random.randint(1, 51)
        })
    return events

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="scripts/data_raw")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    out_dir = Path(args.out)
    ensure_dir(out_dir)

    # --- Generate all datasets ---
    customers = generate_customers(seed=args.seed)
    products = generate_products(seed=args.seed)
    stores = generate_stores()
    suppliers = generate_suppliers()
    orders_header, orders_lines = generate_orders(customers, stores, seed=args.seed)
    shipments = generate_shipments(orders_header)
    returns = generate_returns(orders_header)
    rates = generate_exchange_rates()
    sensors = generate_sensors()
    events = generate_events()

    # --- Write to disk ---
    customers.to_csv(out_dir / "customers.csv", index=False)
    products.to_csv(out_dir / "products.csv", index=False)
    stores.to_csv(out_dir / "stores.csv", index=False)
    suppliers.to_csv(out_dir / "suppliers.csv", index=False)
    orders_header.to_csv(out_dir / "orders_header.csv", index=False)
    orders_lines.to_csv(out_dir / "orders_lines.csv", index=False)
    shipments.to_parquet(out_dir / "shipments.parquet", index=False)
    returns.to_parquet(out_dir / "returns_day1.parquet", index=False)
    rates.to_parquet(out_dir / "exchange_rates.parquet", index=False)
    sensors.to_parquet(out_dir / "sensors.parquet", index=False)

    with open(out_dir / "events.json", "w") as f:
        json.dump(events, f, indent=2)

    print(f"✅ Sample raw data written to {out_dir}")
    print("Files written:")
    for f in sorted(os.listdir(out_dir)):
        print(f" - {f}")

if __name__ == "__main__":
    main()
