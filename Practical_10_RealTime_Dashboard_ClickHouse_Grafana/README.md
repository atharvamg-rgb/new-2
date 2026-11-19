# Practical 10: Real-Time Dashboard (Kafka Stream + ClickHouse OLAP + Grafana Visualization)

## Aim
To build an end-to-end real-time analytics pipeline that ingests continuous IoT/Air Quality sensor streams into **Apache Kafka**, loads them into a columnar OLAP database (**ClickHouse**), and visualizes the streaming metrics live on a **Grafana Dashboard**.

---

## Architecture & Data Flow

```
┌─────────────────────┐        Port 9092         ┌───────────────────────┐
│     producer.py     │ ───────────────────────> │  Apache Kafka (KRaft) │
│ (IoT Sensor Reading)│    topic: air_quality    └───────────┬───────────┘
└─────────────────────┘                                      │
                                                             ▼
                                                 ┌───────────────────────┐
                                                 │      consumer.py      │
                                                 │ (Batch Stream Ingest) │
                                                 └───────────┬───────────┘
                                                             │
                                                             ▼
                                                 ┌───────────────────────┐
                                                 │   ClickHouse OLAP     │
                                                 │ (MergeTree Table)     │
                                                 └───────────┬───────────┘
                                                             │
                                                             ▼
                                                 ┌───────────────────────┐
                                                 │   Grafana Dashboard   │
                                                 │   http://localhost:3000
                                                 └───────────────────────┘
```

---

## Services Configured
1. **Kafka (`3.9.1`):** High-throughput messaging broker.
2. **ClickHouse (`latest`):** Column-oriented DBMS for ultra-fast aggregations.
3. **Grafana (`latest`):** Analytics and interactive visualization web UI with pre-installed `grafana-clickhouse-datasource` plugin.
4. **Producer & Consumer:** Microservices automated through containerization.

---

## Step-by-Step Execution Instructions

### Step 1: Start All Services via Docker Compose
Open PowerShell or Terminal:
```powershell
cd Practical_10_RealTime_Dashboard_ClickHouse_Grafana
docker compose up --build
```
*Docker will build the Python microservices and bring up Kafka, ClickHouse, Grafana, producer, and consumer containers.*

### Step 2: Observe Streaming Ingestion
In your terminal, watch the live logs:
- `producer-1` generates real-time telemetry (city, PM2.5, PM10, temperature).
- `consumer-1` streams records directly into ClickHouse's `default.air_quality` table.

### Step 3: View in Grafana Dashboard
1. Open your browser and navigate to: `http://localhost:3000`
2. Login with default credentials:
   - **Username:** `admin`
   - **Password:** `admin` (skip or set a new password).
3. Add ClickHouse Data Source:
   - **Settings:** `Configuration -> Data Sources -> Add data source -> ClickHouse`
   - **Server address:** `clickhouse`
   - **Port:** `9000` (native) or `8123` (HTTP)
   - **User:** `default`, **Password:** *(leave blank)*.
4. Create a panel querying real-time air quality:
   ```sql
   SELECT event_time, city, pm25, pm10, temperature
   FROM default.air_quality
   ORDER BY event_time DESC
   LIMIT 100
   ```

### Step 4: Cleanup
```powershell
docker compose down
```

---

## Complete Source Codes

### 1. `docker-compose.yml`
```yaml
services:
  kafka:
    image: apache/kafka:3.9.1
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 1

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_SKIP_USER_SETUP: 1

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_INSTALL_PLUGINS: grafana-clickhouse-datasource
    depends_on:
      - clickhouse

  producer:
    build: .
    command: python -u producer.py
    depends_on:
      - kafka

  consumer:
    build: .
    command: python -u consumer.py
    depends_on:
      - kafka
      - clickhouse
```

### 2. `producer.py`
```python
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC = "air_quality"


def connect_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                value_serializer=lambda value: json.dumps(value).encode("utf-8")
            )
            print("Connected to Kafka.", flush=True)
            return producer
        except NoBrokersAvailable:
            print("Waiting for Kafka...", flush=True)
            time.sleep(3)


def main():
    producer = connect_producer()
    cities = ["Mumbai", "Pune", "Nashik", "Nagpur"]
    print("Air Quality Producer Started", flush=True)

    try:
        while True:
            reading = {
                "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "city": random.choice(cities),
                "pm25": round(random.uniform(10, 180), 2),
                "pm10": round(random.uniform(20, 250), 2),
                "temperature": round(random.uniform(24, 38), 2)
            }
            producer.send(TOPIC, value=reading).get(timeout=10)
            print("Sent:", reading, flush=True)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Producer stopped.", flush=True)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
```

### 3. `consumer.py`
```python
import json
import time
from datetime import datetime, timezone
import clickhouse_connect
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


def connect_clickhouse():
    while True:
        try:
            client = clickhouse_connect.get_client(
                host="clickhouse",
                port=8123,
                username="default",
                password=""
            )
            client.command("""
            CREATE TABLE IF NOT EXISTS default.air_quality
            (
                event_time DateTime,
                city String,
                pm25 Float64,
                pm10 Float64,
                temperature Float64
            )
            ENGINE = MergeTree
            ORDER BY (city, event_time)
            """)
            print("Connected to ClickHouse.", flush=True)
            return client
        except Exception as error:
            print("Waiting for ClickHouse:", error, flush=True)
            time.sleep(3)


def connect_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                "air_quality",
                bootstrap_servers=["kafka:9092"],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="air-quality-group",
                value_deserializer=lambda value: json.loads(value.decode("utf-8"))
            )
            print("Connected to Kafka topic.", flush=True)
            return consumer
        except NoBrokersAvailable:
            print("Waiting for Kafka...", flush=True)
            time.sleep(3)


def main():
    client = connect_clickhouse()
    consumer = connect_kafka()
    print("Kafka Consumer Started", flush=True)

    try:
        for message in consumer:
            reading = message.value
            try:
                event_time = datetime.strptime(
                    reading["event_time"], "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=timezone.utc)

                client.insert(
                    "default.air_quality",
                    [[
                        event_time,
                        reading["city"],
                        reading["pm25"],
                        reading["pm10"],
                        reading["temperature"]
                    ]],
                    column_names=[
                        "event_time",
                        "city",
                        "pm25",
                        "pm10",
                        "temperature"
                    ]
                )
                print("Inserted:", reading, flush=True)
            except Exception as error:
                print("Insertion error:", error, flush=True)
    except KeyboardInterrupt:
        print("Consumer stopped.", flush=True)
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
```

---

## Expected Output

```
producer-1 | Connected to Kafka.
producer-1 | Air Quality Producer Started
producer-1 | Sent: {'event_time': '2026-10-28 06:20:11', 'city': 'Pune', 'pm25': 87.41, 'pm10': 143.2, 'temperature': 29.8}
consumer-1 | Connected to ClickHouse.
consumer-1 | Connected to Kafka topic.
consumer-1 | Kafka Consumer Started
consumer-1 | Inserted: {'event_time': '2026-10-28 06:20:11', 'city': 'Pune', 'pm25': 87.41, 'pm10': 143.2, 'temperature': 29.8}
producer-1 | Sent: {'event_time': '2026-10-28 06:20:13', 'city': 'Mumbai', 'pm25': 121.07, 'pm10': 64.55, 'temperature': 33.12}
consumer-1 | Inserted: {'event_time': '2026-10-28 06:20:13', 'city': 'Mumbai', 'pm25': 121.07, 'pm10': 64.55, 'temperature': 33.12}
```
