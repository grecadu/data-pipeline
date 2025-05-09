"""
Kafka producer: reads events from input.jsonl and publishes to `raw-events` topic.
"""
import json
import logging
import signal
import sys
import time
from confluent_kafka import Producer
from typing import Any, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_FILE = "input.jsonl"
TOPIC = "raw-events"

producer: Producer
running = True


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logging.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logging.debug(f"Record delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def shutdown(sig, frame) -> None:
    global running
    logging.info("Shutting down producer...")
    running = False


def main() -> None:
    global producer
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 3,
        "linger.ms": 100
    }
    producer = Producer(conf)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logging.info("Starting to publish messages...")
    try:
        with open(INPUT_FILE, "r") as f:
            for line in f:
                if not running:
                    break
                record = json.loads(line)
                key = str(record.get("id")).encode("utf-8")
                value = json.dumps(record).encode("utf-8")
                for attempt in range(3):
                    try:
                        producer.produce(
                            TOPIC,
                            key=key,
                            value=value,
                            callback=delivery_report
                        )
                        producer.poll(0)
                        break
                    except Exception as e:
                        logging.warning(f"Publish attempt {attempt + 1} failed: {e}")
                        time.sleep(1)
                else:
                    logging.error("Failed to publish record after retries")
                    continue
        producer.flush()
        logging.info("All messages published.")
    except FileNotFoundError:
        logging.error(f"Input file {INPUT_FILE} not found.")
        sys.exit(1)


if __name__ == "__main__":
    main()