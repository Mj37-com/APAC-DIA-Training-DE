# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw

import argparse, os, pathlib, random
from datetime import datetime, timedelta, date
import numpy as np
from faker import Faker
from mimesis import Person, Address
import rstr
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from decimal import Decimal

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--out', type=str, default='data_raw')
    return ap.parse_args()

def ensure_dir(p): pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def main():
    args = parse_args()
    random.seed(args.seed); np.random.seed(args.seed)
    out = pathlib.Path(args.out); ensure_dir(out)

    fake = Faker('en_AU')

    # -------------------------------
    # CUSTOMERS
    # -------------------------------
    customers_path = out/'customers.csv'
    with customers_path.open('w', encoding='utf-8') as f:
        f.write('customer_id,natural_key,first_name,last_name,email,phone,address_line1,address_line2,city,state_region,postcode,country_code,latitude,longitude,birth_date,join_ts,is_vip,gdpr_consent\n')
        for i in range(1, 1001):
            nk = 'CUST-' + rstr.rstr('A-Z0-9', 8)
            email = fake.email() if random.random()>0.1 else 'bad_email'
            lat = -44 + random.random()*10; lon = 112 + random.random()*40
            birth = date(1960,1,1) + timedelta(days=random.randint(0, 20000))
            join_ts = datetime(2024,1,1) + timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86399))
            f.write(f"{i},{nk},{fake.first_name()},{fake.last_name()},{email},{fake.phone_number().replace(',',' ')},{fake.street_address().replace(',',' ')},,{fake.city().replace(',',' ')},{fake.state_abbr()},{fake.postcode()},AU,{lat:.6f},{lon:.6f},{birth.isoformat()},{join_ts.isoformat()},{str(random.random()<0.15)},{str(random.random()>0.05)}\n")

    # -------------------------------
    # PRODUCTS
    # -------------------------------
    products_path = out/'products.csv'
    categories = {
        "Electronics": ["Smartphones", "Laptops", "Headphones", "Tablets"],
        "Home Appliances": ["Refrigerators", "Microwaves", "Air Conditioners"],
        "Furniture": ["Chairs", "Tables", "Beds"],
        "Sports": ["Fitness", "Cycling", "Running"]
    }

    with products_path.open('w', encoding='utf-8') as f:
        f.write('product_id,sku,name,category,subcategory,current_price,currency,is_discontinued,introduced_dt,discontinued_dt\n')
        pid = 1
        for category, subcats in categories.items():
            for subcat in subcats:
                for _ in range(25):  # ~400 total products
                    sku = 'SKU-' + rstr.rstr('A-Z0-9', 6)
                    name = f"{fake.word().capitalize()} {subcat[:-1] if subcat.endswith('s') else subcat}"
                    price = round(random.uniform(50, 5000), 2)
                    is_disc = random.random() < 0.1
                    introduced = date(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
                    discontinued = introduced + timedelta(days=random.randint(300, 1000)) if is_disc else ""
                    f.write(f"{pid},{sku},{name},{category},{subcat},{price:.4f},AUD,{is_disc},{introduced},{discontinued}\n")
                    pid += 1

    # Convert products.csv to products.parquet
    products_tbl = pv.read_csv(products_path)
    pq.write_table(products_tbl, out/'products.parquet', compression='snappy')

    # -------------------------------
    # SHIPMENTS
    # -------------------------------
    tbl = pa.table({
        'shipment_id': pa.array(range(1, 10001), type=pa.int64()),
        'order_id': pa.array(range(1, 10001), type=pa.int64()),
        'carrier': pa.array(['AUSPOST']*10000, type=pa.string()),
        'shipped_at': pa.array([datetime(2024,1,1)+timedelta(days=i%90) for i in range(10000)], type=pa.timestamp('us')),
        'delivered_at': pa.array([datetime(2024,1,2)+timedelta(days=i%90) for i in range(10000)], type=pa.timestamp('us')),
        'ship_cost': pa.array([Decimal("1995.00")]*10000, type=pa.decimal128(12, 2)),
    })
    pq.write_table(tbl, out/'shipments.parquet', compression='snappy')

    print(f"✅ Sample raw written to {out}. Includes customers.csv, products.csv, products.parquet, and shipments.parquet")
    print("Files written:\n - customers.csv\n - products.csv\n - products.parquet\n - shipments.parquet")

if __name__ == '__main__':
    main()
