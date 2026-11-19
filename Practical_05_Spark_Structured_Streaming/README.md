# Practical 5: Spark Structured Streaming (Live Socket Word Count)

## Aim
To build an Apache Spark **Structured Streaming** real-time pipeline that ingests continuous stream lines from a TCP socket and calculates running word frequencies using micro-batch processing.

---

## Architecture & Concepts

```
┌─────────────────────────┐          TCP Port 9999          ┌───────────────────────────────┐
│     socket_sender.py    │ ──────────────────────────────> │      word_count_stream.py     │
│ (Interactive TCP Server)│    "hello spark hello python"   │ (PySpark Structured Streaming)│
└─────────────────────────┘                                 └───────────────┬───────────────┘
                                                                            │
                                                                 Micro-batch Aggregation
                                                                            │
                                                                            ▼
                                                            ┌───────────────────────────────┐
                                                            │         Console Sink          │
                                                            │ word: hello (2), spark (1)... │
                                                            └───────────────────────────────┘
```

- **Unbounded Table:** Streaming data is modeled as an append-only table.
- **Output Mode ("complete"):** On every micro-batch trigger, the full aggregated state table is output to the console.

---

## Prerequisites
- Python 3.8+
- Java 8 / 11 / 17 / 21+
- Install PySpark:
  ```bash
  pip install pyspark
  ```

---

## Step-by-Step Execution Instructions

### Terminal 1 — Start the TCP Socket Server
```powershell
cd Practical_05_Spark_Structured_Streaming
python socket_sender.py
```
*The server starts on port `9999` and waits for Spark to connect.*

### Terminal 2 — Start the Spark Structured Streaming Application
In a separate terminal window:
```powershell
cd Practical_05_Spark_Structured_Streaming
python word_count_stream.py
```
*Spark connects to `localhost:9999` and starts micro-batching.*

### Testing Live Streaming
Go back to **Terminal 1** and type text messages:
```
Enter message: hello spark hello python hello spark
Enter message: big data analytics with spark
```
Watch **Terminal 2** output the real-time aggregated counts in tables automatically!
To finish, type `exit` in Terminal 1 and press `Ctrl+C` in Terminal 2.

---

## Complete Source Codes

### 1. `socket_sender.py`
```python
import socket

HOST = "localhost"
PORT = 9999

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

server_socket.bind((HOST, PORT))
server_socket.listen(1)

print("======================================")
print("Socket Server Started")
print("Waiting for Spark to connect...")
print("Host:", HOST)
print("Port:", PORT)
print("======================================")

connection, address = server_socket.accept()
print("Spark connected from:", address)
print("Start entering messages.")
print("Type 'exit' to stop.")
print("--------------------------------------")

try:
    while True:
        message = input("Enter message: ")
        if message.lower() == "exit":
            break
        connection.sendall((message + "\n").encode("utf-8"))
finally:
    print("Closing connection...")
    connection.close()
    server_socket.close()
    print("Socket server stopped.")
```

### 2. `word_count_stream.py`
```python
import os
import sys
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

temp_dir = os.path.abspath("./spark-temp")
os.makedirs(temp_dir, exist_ok=True)
tempfile.tempdir = temp_dir

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("WordCountStream")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("Spark Structured Streaming Initialized. Connecting to socket localhost:9999...")

data = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

words = data.select(
    explode(split(data.value, "\\s+")).alias("word")
).filter("word != ''")

counts = words.groupBy("word").count()

query = (
    counts.writeStream
    .outputMode("complete")
    .format("console")
    .start()
)

print("Streaming query started. Awaiting batches...\n")
query.awaitTermination()
```

---

## Expected Output

### Terminal 1 (Socket Sender):
```
======================================
Socket Server Started
Waiting for Spark to connect...
Host: localhost
Port: 9999
======================================
Spark connected!
Start entering messages.
Type 'exit' to stop.
--------------------------------------
Enter message: hello spark hello python hello spark
Enter message: 
```

### Terminal 2 (Spark Word Count Stream):
```
-------------------------------------------
Batch: 0
-------------------------------------------
+----+-----+
|word|count|
+----+-----+
+----+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+------+-----+
|  word|count|
+------+-----+
| hello|    3|
| spark|    2|
|python|    1|
+------+-----+
```
