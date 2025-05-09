"""
Kafka consumer & transformer: reads from `raw-events`, transforms, and publishes to `processed-events`.
"""
import json
import logging
import signal
import sys
from confluent_kafka import Consumer, Producer
from typing import Any, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

BOOTSTRAP_SERVERS = "localhost:9092"
RAW_TOPIC = "raw-events"
PROCESSED_TOPIC = "processed-events"
GROUP_ID = "pipeline-group"
OUTPUT_FILE = "output.jsonl"

consumer: Consumer
producer: Producer
running = True


def shutdown(sig, frame) -> None:
    global running
    logging.info("Shutdown signal received. Exiting...")
    running = False


def transform(record: Dict[str, Any]) -> Dict[str, Any]:
    # Example transformation: filter clicks and keep id + value
    if record.get("type") != "click":
        return {}
    return {"id": record["id"], "value": record["value"]}


def main() -> None:
    global consumer, producer
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 3
    }

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    consumer.subscribe([RAW_TOPIC])

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        with open(OUTPUT_FILE, "a") as outfile:
            while running:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(msg.error())
                    continue
                record = json.loads(msg.value().decode("utf-8"))
                out = transform(record)
                if not out:
                    consumer.commit(msg)
                    continue
                data = json.dumps(out).encode("utf-8")
                # publish to Kafka
                producer.produce(
                    PROCESSED_TOPIC,
                    key=str(out["id"]).encode("utf-8"),
                    value=data
                )
                producer.poll(0)
                # append to file
                outfile.write(json.dumps(out) + "\n")
                outfile.flush()
                consumer.commit(msg)
    finally:
        consumer.close()
        producer.flush()
        logging.info("Consumer and producer closed.")


if __name__ == "__main__":
    main()