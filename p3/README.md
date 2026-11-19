# Practical 3: 3-Broker Kafka Cluster Setup using Docker & Python Clients

## Aim
To configure a fault-tolerant **3-broker Apache Kafka cluster** using Docker Compose (KRaft consensus mode without ZooKeeper) and implement distributed messaging with a Python **Producer** and **Consumer**.

---

## Architecture & Concepts

### 3-Broker KRaft Cluster
- Apache Kafka 3.7+ uses **KRaft** (Kafka Raft Metadata mode) instead of ZooKeeper.
- Each broker runs with dual roles (`broker,controller`).
- The controller quorum voters are configured as:
  `1@kafka1:9093, 2@kafka2:9093, 3@kafka3:9093`
- Host mapping for external clients:
  - Broker 1: `localhost:29092`
  - Broker 2: `localhost:39092`
  - Broker 3: `localhost:49092`

---

## Prerequisites
- Docker & Docker Compose installed (Docker Desktop running).
- Python 3.8+ with `kafka-python`:
  ```bash
  pip install kafka-python==2.2.15
  ```

---

## Step-by-Step Execution Instructions

### Step 1: Start the 3-Broker Kafka Cluster
Open PowerShell or Terminal and run:
```powershell
cd p3
docker compose up -d
```
Verify all 3 containers are healthy and running:
```powershell
docker compose ps
```

### Step 2: Run the Consumer in Terminal 1
Open a terminal, activate the consumer, and wait for messages:
```powershell
python consumer.py
```

### Step 3: Run the Producer in Terminal 2
In a second terminal, execute the producer to send 10 messages across the cluster:
```powershell
python producer.py
```

### Step 4: Tear Down Cluster
When testing is complete, stop and remove containers:
```powershell
docker compose down
```

---

## Complete Source Codes

### 1. `docker-compose.yml`
```yaml
services:
  kafka1:
    image: bitnami/kafka:3.7
    container_name: kafka1
    ports:
      - "29092:29092"
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka1:9092,EXTERNAL://localhost:29092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    volumes:
      - kafka1_data:/bitnami/kafka

  kafka2:
    image: bitnami/kafka:3.7
    container_name: kafka2
    ports:
      - "39092:39092"
    environment:
      - KAFKA_CFG_NODE_ID=2
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:39092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka2:9092,EXTERNAL://localhost:39092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    volumes:
      - kafka2_data:/bitnami/kafka

  kafka3:
    image: bitnami/kafka:3.7
    container_name: kafka3
    ports:
      - "49092:49092"
    environment:
      - KAFKA_CFG_NODE_ID=3
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:49092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka3:9092,EXTERNAL://localhost:49092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    volumes:
      - kafka3_data:/bitnami/kafka

volumes:
  kafka1_data:
  kafka2_data:
  kafka3_data:
```

### 2. `producer.py`
```python
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=[
        "localhost:29092",
        "localhost:39092",
        "localhost:49092",
    ],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for i in range(10):
    message = {
        "student_id": i + 1,
        "name": f"Student-{i + 1}",
        "course": "Data Science",
        "message": f"Hello from student {i + 1}",
    }

    producer.send("student-events", value=message)
    print("Produced:", message)
    time.sleep(1)

producer.flush()
producer.close()
```

### 3. `consumer.py`
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "student-events",
    bootstrap_servers=[
        "localhost:29092",
        "localhost:39092",
        "localhost:49092",
    ],
    auto_offset_reset="earliest",
    group_id="student-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("Consumer started...")
print("Waiting for messages...\n")

for message in consumer:
    print(
        "Received:",
        message.value,
        "| Partition:",
        message.partition,
        "| Offset:",
        message.offset,
    )
```

---

## Expected Output

### Producer Terminal:
```
Produced: {'student_id': 1, 'name': 'Student-1', 'course': 'Data Science', 'message': 'Hello from student 1'}
Produced: {'student_id': 2, 'name': 'Student-2', 'course': 'Data Science', 'message': 'Hello from student 2'}
Produced: {'student_id': 3, 'name': 'Student-3', 'course': 'Data Science', 'message': 'Hello from student 3'}
...
Produced: {'student_id': 10, 'name': 'Student-10', 'course': 'Data Science', 'message': 'Hello from student 10'}
```

### Consumer Terminal:
```
Consumer started...
Waiting for messages...

Received: {'student_id': 1, 'name': 'Student-1', 'course': 'Data Science', 'message': 'Hello from student 1'} | Partition: 0 | Offset: 0
Received: {'student_id': 2, 'name': 'Student-2', 'course': 'Data Science', 'message': 'Hello from student 2'} | Partition: 0 | Offset: 1
Received: {'student_id': 3, 'name': 'Student-3', 'course': 'Data Science', 'message': 'Hello from student 3'} | Partition: 0 | Offset: 2
...
Received: {'student_id': 10, 'name': 'Student-10', 'course': 'Data Science', 'message': 'Hello from student 10'} | Partition: 0 | Offset: 9
```
