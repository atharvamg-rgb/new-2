# Practical 1: Spark Optimization (Data Skew & Shuffle Bottlenecks)

## Aim
To implement an Apache Spark optimization job and use the Spark UI to identify and resolve **data skew** and **shuffle bottlenecks** using key salting.

---

## Theory & Concept

### What is Data Skew?
In distributed computing, data skew occurs when data is partitioned unevenly across cluster nodes. If 90% of records share the same partition key (e.g. `key = 0`), a single executor core handles 90% of the workload (straggler task) while other cores sit idle, degrading execution time.

### The Fix: Salting Technique
1. **Add Random Salt:** Append a random number (`0` to `N-1`, matching shuffle partitions) to each record's key.
2. **Partial Aggregation:** Group by `(key + salt)` to evenly distribute the heavy key across all executors.
3. **Final Rollup:** Group by the original `key` and sum up the partial counts.

---

## Prerequisites
- Python 3.8+
- Java 8 / 11 / 17 / 21+
- PySpark installed:
  ```bash
  pip install pyspark
  ```

---

## Step-by-Step Execution Instructions

1. Open PowerShell or Command Prompt.
2. Navigate to the practical directory:
   ```powershell
   cd p1
   ```
3. Run the script:
   ```powershell
   python skew.py
   ```
4. While the script prints `Open Spark UI: http://localhost:4040`, open your browser:
   - Navigate to: `http://localhost:4040`
   - Go to the **Jobs** and **Stages** tab to inspect execution time, shuffle read/write sizes, and task distributions across partitions.

---

## Complete Source Code (`skew.py`)
```python
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, concat_ws, sum as _sum

# Configure JVM flags required for Java 17+ compatibility
java_flags = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
)

spark = (
    SparkSession.builder
    .appName("SparkSkewPractical")
    .master("local[4]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.extraJavaOptions", java_flags)
    .config("spark.executor.extraJavaOptions", java_flags)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("====================================")
print("Spark application started!")
print("====================================")

# Create 5 million records
number_of_records = 5_000_000
df = spark.range(number_of_records)

# Generate skewed dataset (90% assigned to key=0)
skewed_df = df.withColumn(
    "key",
    when(
        col("id") < number_of_records * 0.90,
        lit(0)
    ).otherwise(
        (col("id") % 999) + 1
    )
)

print("Skewed dataset created!")

# Show the initial distribution
print("\nKey distribution (Top 10):")
skewed_df.groupBy("key").count().orderBy(col("count").desc()).show(10)

print("\nStarting SALTED GROUP BY (Data Skew Fix)...")

# Step 1: Add a random salt (0 to 7) matching shuffle partition count
salted_df = skewed_df.withColumn("salt", (rand() * 8).cast("int"))

# Step 2: Combine original key with salt
salted_df = salted_df.withColumn("salted_key", concat_ws("_", col("key"), col("salt")))

# Step 3: Partial Aggregation (distributes key=0 across all 8 cores)
partial_count = salted_df.groupBy("salted_key", "key").count()

# Step 4: Final Aggregation (roll up back to original key)
final_result = (
    partial_count
    .groupBy("key")
    .agg(_sum("count").alias("count"))
    .orderBy(col("count").desc())
)

final_result.collect()

print("\nSALTED GROUP BY completed successfully!")
print("====================================")
print("Open Spark UI: http://localhost:4040")
print("====================================")

# Allow time to inspect Spark UI
time.sleep(120)
spark.stop()
```

---

## Expected Output

```
====================================
Spark application started!
====================================
Skewed dataset created!

Key distribution (Top 10):
+---+-------+
|key|  count|
+---+-------+
|  0|4500000|
|517|    501|
|518|    501|
|519|    501|
|537|    501|
|564|    501|
|566|    501|
|569|    501|
|577|    501|
|582|    501|
+---+-------+
only showing top 10 rows

Starting SALTED GROUP BY (Data Skew Fix)...

SALTED GROUP BY completed successfully!
====================================
Open Spark UI: http://localhost:4040
====================================
```
