# Generates synthetic test data: events representing a "clickstream" of
# user interactions on an e-commerce site/app
#
# Outputs a CSV file "events.csv"

import csv
import random
from datetime import datetime, timedelta
import uuid

random.seed(42)

TIMESPAN_IN_DAYS = 60           # Generate 60 days of data with dates up to today's date

EVENT_TYPES = ["view", "click", "add_to_cart", "purchase"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["US", "CA", "UK"]
PRODUCT_IDS = [f"product_{i}" for i in range(1, 101)]

def random_event_type():
    r = random.random()
    if r < 0.65:
        return "view"
    elif r < 0.85:
        return "click"
    elif r < 0.95:
        return "add_to_cart"
    return "purchase"

def generate_events(num_users=1000, num_events=100000, output_file="events.csv"):
    start_time = datetime.now() - timedelta(days=TIMESPAN_IN_DAYS)

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "event_id", "user_id", "event_type", "product_id",
            "timestamp", "price", "device_type", "country"
        ])

        for _ in range(num_events):
            user_id = f"user_{random.randint(1, num_users)}"
            event_type = random_event_type()
            product_id = random.choice(PRODUCT_IDS)
            ts = start_time + timedelta(minutes=random.randint(0, TIMESPAN_IN_DAYS * 24 * 60))
            price = round(random.uniform(10, 500), 2) if event_type == "purchase" else ""
            device = random.choice(DEVICE_TYPES)
            country = random.choice(COUNTRIES)

            writer.writerow([
                str(uuid.uuid4()),
                user_id,
                event_type,
                product_id,
                ts.isoformat(),
                price,
                device,
                country
            ])

if __name__ == "__main__":
    generate_events()
    