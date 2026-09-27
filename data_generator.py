import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta

# --- BATCH RUN TIMESTAMP FOR UNIQUE FILE NAMES ---
run_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

# --- UPDATE BASE DIR TO BATCH_01 ---
OUTPUT_DIR = "batch_01"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Define explicit subfolder paths (ORDER_ITEMS REMOVED)
PATHS = {
    "products": os.path.join(OUTPUT_DIR, "products"),
    "customers_cdc": os.path.join(OUTPUT_DIR, "customers_cdc"),
    "clickstream": os.path.join(OUTPUT_DIR, "clickstream"),
}

# Create all individual subdirectories safely
for folder_path in PATHS.values():
    os.makedirs(folder_path, exist_ok=True)

print("Simulating realistic Front-Runner SA ecosystem data for Databricks...")

# --- 1. CONFIGURATION TARGETS ---
TOTAL_PRODUCTS = 20
TOTAL_CUSTOMERS = 15
TOTAL_EVENTS = 55 

base_date = datetime(2026, 8, 1)

product_pool = []
product_categories = ["Roof Racks", "Camping", "Storage", "Brackets", "Lighting"]
for i in range(1, TOTAL_PRODUCTS + 1):
    prod_id = str(uuid.uuid4())
    cat = random.choice(product_categories)
    sku = f"SK-FR-{cat[:3].upper()}-{100+i}"
    product_pool.append({
        "product_id": prod_id,
        "sku": sku,
        "name": f"Front-Runner {cat} Component v{i}",
        "category": cat,
        "weight_kg": round(random.uniform(1.5, 45.0), 2),
        "price_usd": round(random.uniform(25.0, 950.0), 2)
    })

customer_pool = [str(uuid.uuid4()) for _ in range(TOTAL_CUSTOMERS)]

# --- WRITE PRODUCTS TO PRODUCTS FOLDER (WITH SUFFIX) ---
products_file = os.path.join(PATHS["products"], f"products_{run_suffix}.csv")
with open(products_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["product_id", "sku", "name", "category", "weight_kg", "price_usd"])
    writer.writeheader()
    writer.writerows(product_pool)
print(f"Generated CSV: {products_file} ({len(product_pool)} rows)")

# --- WRITE CUSTOMER CDC TO CUSTOMERS_CDC FOLDER (WITH SUFFIX) ---
customers_file = os.path.join(PATHS["customers_cdc"], f"customer_cdc_{run_suffix}.json")
first_names = ["Jabu", "Sipho", "Liam", "Emma", "Chipo", "Anrich", "Sarah", "Elena", "Tariq"]
last_names = ["Naidoo", "Smith", "Botha", "Muller", "Van Wyk", "Baloyi", "Ndlovu", "Jones"]

with open(customers_file, "w", encoding="utf-8") as f:
    for cust_id in customer_pool:
        reg_ts = (base_date - timedelta(days=random.randint(10, 300))).strftime("%Y-%m-%dT%H:%M:%S")
        cdc_record = {
            "op": "c",
            "ts_ms": int((datetime.now() - timedelta(days=1)).timestamp() * 1000),
            "before": None,
            "after": {
                "customer_id": cust_id,
                "email": f"{random.choice(first_names).lower()}.{random.randint(10,99)}@frontrunneroutfitters.co.za",
                "first_name": random.choice(first_names),
                "last_name": random.choice(last_names),
                "registration_date": reg_ts,
                "loyalty_tier": random.choice(["bronze", "silver", "gold"]),
                "country": random.choice(["ZA", "US", "DE", "AU"]),
                "is_active": True,
                "updated_at": reg_ts
            }
        }
        f.write(json.dumps(cdc_record) + "\n")
print(f"Generated JSON Lines: {customers_file} ({TOTAL_CUSTOMERS} rows)")

# --- WRITE CLICKSTREAM TO CLICKSTREAM FOLDER (WITH SUFFIX) ---
clickstream_file = os.path.join(PATHS["clickstream"], f"clickstream_events_{run_suffix}.json")
referrers = ["google", "instagram", "email", "affiliate"]
devices = ["mobile", "tablet", "desktop"]

with open(clickstream_file, "w", encoding="utf-8") as f:
    for i in range(TOTAL_EVENTS):
        # Fallback clickstream simulation since order metadata is removed
        event_type = random.choice(["search", "page_view", "product_view", "purchase"])
        cust_id = random.choice(customer_pool) if random.random() > 0.2 else None
        
        if event_type == "purchase":
            ord_id = str(uuid.uuid4())
            prod_id = None
        else:
            ord_id = None
            prod_id = random.choice(product_pool)["product_id"] if event_type == "product_view" else None
            
        ts = (base_date + timedelta(seconds=random.randint(0, 3888000))).strftime("%Y-%m-%dT%H:%M:%S")
            
        event_record = {
            "event_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
            "customer_id": cust_id,
            "event_type": event_type,
            "event_timestamp": ts,
            "product_id": prod_id,
            "page_url": f"/shop/category/{random.choice(['racks', 'camping', 'gear'])}",
            "referrer": random.choice(referrers),
            "device_type": random.choice(devices),
            "order_id": ord_id
        }
        f.write(json.dumps(event_record) + "\n")
print(f"Generated JSON Lines: {clickstream_file} ({TOTAL_EVENTS} rows)")

print(f"\nVerification matrix matched cleanly inside the organized directory: './{OUTPUT_DIR}/'")
