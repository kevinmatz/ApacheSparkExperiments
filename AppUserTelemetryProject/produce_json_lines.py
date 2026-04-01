import json
import time
from confluent_kafka import Producer

TOPIC = "app-user-events"
BOOTSTRAP = "localhost:9092"
INPUT_FILE = "events.jsonl"

producer = Producer({"bootstrap.servers": BOOTSTRAP})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()!r}: {err}")
    else:
        print(
            f"Delivered to topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    for line in f:
        record = json.loads(line)

        key = record.get("event_id")

        producer.produce(
            TOPIC,
            key=key.encode("utf-8") if key else None,
            value=line.strip().encode("utf-8"),
            callback=delivery_report,
        )

        producer.poll(0)
        time.sleep(0.25)

producer.flush()
print("Done producing JSON lines.")
