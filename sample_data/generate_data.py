# =========================
# GENERATE SYNTHETIC E-COMMERCE ORDERS DATA IN SAME FOLDER AS SCRIPT, 20 JSON FILES WITH 10 ROWS EACH
# =========================


import json
import random
import os
from datetime import datetime, timedelta

random.seed(42)

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
FILES_COUNT = 20
RECORDS_PER_FILE = 10

segments = ["B2C", "B2B"]
tiers = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
statuses = ["NEW", "SHIPPED", "DELIVERED", "CANCELLED"]
payment_methods = ["CARD", "PAYPAL", "INVOICE"]
currencies = ["EUR"]
categories = ["electronics", "home", "books", "office", "fitness", "toys"]
countries = ["DE", "PL", "FR", "ES", "IT", "NL", "BE"]
delivery_types = ["STANDARD", "EXPRESS", "FREIGHT"]

TIER_PROGRESS = {
    "BRONZE": "SILVER",
    "SILVER": "GOLD",
    "GOLD": "PLATINUM"
}

base_datetime = datetime(2026, 2, 1, 8, 0, 0)  # start at 08:00
order_id = 1

CUSTOMERS_COUNT = 150
PRODUCTS_COUNT = 300

customers_pool = {}
products_pool = {}

# =========================
# MASTER DATA
# =========================

for customer_id in range(100, 100 + CUSTOMERS_COUNT):
    customers_pool[customer_id] = {
        "customer_id": customer_id,
        "segment": random.choice(segments),
        "loyalty_tier": random.choice(tiers),
        "country": random.choice(countries),
        "city": f"City_{random.randint(1,50)}"
    }

for product_id in range(1000, 1000 + PRODUCTS_COUNT):
    products_pool[product_id] = {
        "product_id": product_id,
        "category": random.choice(categories),
        "unit_price": round(random.uniform(10, 500), 2)
    }

# =========================
# ORDERS
# =========================

for file_index in range(1, FILES_COUNT + 1):

    current_file_datetime = base_datetime + timedelta(hours=file_index - 1)
    file_timestamp_str = current_file_datetime.strftime("%Y%m%d_%H")

    orders = []

    # after half of files -> loyalty tier evolution
    if file_index > FILES_COUNT // 2:
        for customer in customers_pool.values():
            if random.random() < 0.15:
                current = customer["loyalty_tier"]
                if current in TIER_PROGRESS:
                    customer["loyalty_tier"] = TIER_PROGRESS[current]

    for _ in range(RECORDS_PER_FILE):

        customer = random.choice(list(customers_pool.values()))
        items_count = random.randint(1, 4)
        items = []
        total_amount = 0

        for _ in range(items_count):
            product = random.choice(list(products_pool.values()))
            quantity = random.randint(1, 5)
            total_amount += quantity * product["unit_price"]

            items.append({
                "product_id": product["product_id"],
                "category": product["category"],
                "quantity": quantity,
                "unit_price": product["unit_price"]
            })

        order = {
            "order_id": order_id,
            "customer": {
                "customer_id": customer["customer_id"],
                "segment": customer["segment"],
                "loyalty_tier": customer["loyalty_tier"]
            },
            "order_date": current_file_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "status": random.choice(statuses),
            "payment": {
                "method": random.choice(payment_methods),
                "currency": random.choice(currencies),
                "amount": round(total_amount, 2)
            },
            "items": items,
            "shipping": {
                "country": customer["country"],
                "city": customer["city"],
                "delivery_type": random.choice(delivery_types)
            }
        }

        orders.append(order)
        order_id += 1

    file_name = f"orders_{file_timestamp_str}.json"
    file_path = os.path.join(OUTPUT_DIR, file_name)

    with open(file_path, "w") as f:
        json.dump(orders, f, indent=2)

print(f"Generated {FILES_COUNT} files with hourly timestamps in filename.")