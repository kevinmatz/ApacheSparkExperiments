import csv
import time
from confluent_kafka import Producer

TOPIC = "app-user-events"
BOOTSTRAP = "localhost:9092"
CSV_FILE = "events.csv"

producer = Producer({"bootstrap.servers": BOOTSTRAP})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()!r}: {err}")
    else:
        print(
            f"Delivered to topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

with open(CSV_FILE, "r", newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)  # skip header row

    for row in reader:
        # Reconstruct one CSV line as the message payload
        value = ",".join("" if x is None else str(x) for x in row)

        # Use event_id as key if present
        key = row[0] if row else None

        producer.produce(
            TOPIC,
            key=key.encode("utf-8") if key else None,
            value=value.encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

        # Slow it down a little so you can watch the stream
        time.sleep(0.25)

producer.flush()
print("Done producing CSV rows.")
