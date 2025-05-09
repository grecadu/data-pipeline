# Kafka Data Pipeline (Python)

This project demonstrates a simple, production-inspired end-to-end data pipeline using Apache Kafka and Python. It reads events from a local file, publishes them to a Kafka topic, consumes and transforms them, and finally writes them to a second Kafka topic and an output file.

---

## 🚀 Features

- ✅ **Kafka Producer**: Publishes raw events from a JSONL file to a Kafka topic.
- ✅ **Kafka Consumer + Transformer**: Reads, filters, and processes messages, republishing them to a second topic and writing results to disk.
- ✅ **Retry & Delivery Guarantees**: Built-in retries and delivery acknowledgment.
- ✅ **Modular Design**: Clean, testable, idiomatic Python (PEP8, type hints).
- ✅ **Unit Tests**: Sample test case using `pytest`.

---

## 🧱 Requirements

- **Python 3.8+**  
- **Docker & Docker Compose**  
- **Kafka & Zookeeper** (automatically started via Docker Compose)

Install Docker from:  
👉 [https://docs.docker.com/get-docker](https://docs.docker.com/get-docker)

Install Python from:  
👉 [https://www.python.org/downloads](https://www.python.org/downloads)

---

## 📁 Project Structure

```
data-pipeline/
├── input.jsonl          # Input events in JSON Lines format
├── output.jsonl         # Output after transformation
├── producer.py          # Kafka producer implementation
├── consumer.py          # Kafka consumer & transformer
├── requirements.txt     # Python dependencies
├── docker-compose.yml   # Local Kafka + Zookeeper
├── tests/
│   └── test_transform.py  # Unit test for transformation logic
└── README.md
```

---

## ⚙️ Installation

### 1. Clone the Repository

```bash
git clone https://github.com/grecadu/data-pipeline.git
cd data-pipeline
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Kafka and Zookeeper (Locally)

Ensure `docker-compose.yml` is available, then run:

```bash
docker-compose up -d
```

Check container status:

```bash
docker ps
```

Kafka should be available at `localhost:9092`.

---

## ▶️ Running the Pipeline

### Step 1: Start the Producer

```bash
python producer.py
```

This script reads `input.jsonl` and publishes records to the Kafka topic `raw-events`.

### Step 2: Start the Consumer & Transformer

```bash
python consumer.py
```

This component:
- Subscribes to `raw-events`
- Filters only events with `"type": "click"`
- Publishes them to `processed-events`
- Appends each transformed record to `output.jsonl`

---

## 🧪 Running Tests

To run unit tests for the transformation logic:

```bash
pytest
```

---

## 📊 Example

**input.jsonl**
```jsonl
{"id": 1, "type": "click", "value": 10}
{"id": 2, "type": "view",  "value": 5}
```

**output.jsonl** (after consumer runs)
```json
{"id": 1, "value": 10}
```

---

## 🧹 Cleanup

Stop containers:

```bash
docker-compose down
```
