# Generates synthetic test data: events representing a "clickstream" of
# app user interactions on an e-commerce site
#
# Outputs a JSON Lines file "events.jsonl"

import json
import random
from datetime import datetime, timedelta
import uuid

random.seed(42)

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

def generate_events(num_users=1000, num_events=100000, output_file="events.jsonl"):
    start_time = datetime.now() - timedelta(days=30)

    with open(output_file, "w", encoding="utf-8") as f:
        for _ in range(num_events):
            event_type = random_event_type()

            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": f"user_{random.randint(1, num_users)}",
                "event_type": event_type,
                "product_id": random.choice(PRODUCT_IDS),
                "timestamp": (
                    start_time + timedelta(minutes=random.randint(0, 30 * 24 * 60))
                ).isoformat(),
                "price": round(random.uniform(10, 500), 2) if event_type == "purchase" else None,
                "device_type": random.choice(DEVICE_TYPES),
                "country": random.choice(COUNTRIES),
            }

            f.write(json.dumps(event) + "\n")

if __name__ == "__main__":
    generate_events()
