# Practical 2: Data Lakehouse (Delta Lake Time Travel & ACID Transactions)

## Aim
To create a Delta Lake table on Apache Spark and demonstrate:
1. **ACID Transactions** (atomic writes and in-place row updates).
2. **Time Travel** (querying historical table snapshots using `versionAsOf`).
3. **Audit History** (inspecting commit logs via `_delta_log`).

---

## Theory & Concept

### What is Delta Lake?
Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. It stores data as versioned Parquet files and maintains an ordered transaction log (`_delta_log/` in JSON) that tracks every atomic commit.

### Key Capabilities Demonstrated:
- **ACID In-Place Updates:** Traditional Parquet/HDFS tables are immutable and require rewriting whole partitions. Delta Lake executes fine-grained `UPDATE` operations atomically.
- **Time Travel:** Queries can inspect any prior version of the table (`versionAsOf = 0`) to reproduce past results, debug regressions, or audit compliance.
- **Table History:** Provides complete auditability of operations (`WRITE`, `UPDATE`, `DELETE`, `MERGE`) with timestamps.

---

## Prerequisites

1. **Python & Java:**
   - Python 3.8+
   - Java 8 / 11 / 17 / 21+

2. **Python Packages:**
   ```bash
   pip install pyspark delta-spark
   ```

3. **Windows Requirement (`HADOOP_HOME` & `winutils.exe`):**
   Spark on Windows relies on Hadoop native utilities to manage file permissions (`chmod`). Without these binaries, Spark throws `java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`.

   To set up Hadoop binaries on Windows:
   ```powershell
   # 1. Create hadoop directory
   New-Item -ItemType Directory -Path "C:\hadoop\bin" -Force

   # 2. Download winutils.exe and hadoop.dll
   Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "C:\hadoop\bin\winutils.exe"
   Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "C:\hadoop\bin\hadoop.dll"

   # 3. Set HADOOP_HOME and update Path (User level)
   [Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
   $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
   if ($userPath -notlike "*C:\hadoop\bin*") {
       [Environment]::SetEnvironmentVariable("Path", "$userPath;C:\hadoop\bin", "User")
   }
   ```
   *(Note: `practical2.py` also contains an automated fallback that checks and applies `C:\hadoop` directly at runtime).*

---

## Step-by-Step Execution Instructions

1. Open PowerShell or Command Prompt.
2. (Optional on current terminal if environment variables were just added):
   ```powershell
   $env:HADOOP_HOME = "C:\hadoop"
   $env:Path = "C:\hadoop\bin;" + $env:Path
   ```
3. Navigate to the `p2` directory:
   ```powershell
   cd p2
   ```
4. Run the script:
   ```powershell
   python practical2.py
   ```

---

## Complete Source Code (`practical2.py`)
```python
import os
import sys

# FIX: make Spark use the same Python executable that runs this script
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# FIX: ensure HADOOP_HOME and winutils are detected on Windows
if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Start Spark with Delta Lake support
builder = (
    SparkSession.builder
    .appName("DeltaLakeDemo")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark started successfully!")

# Create sample student data
data = [
    (1, "Siddhant", 90),
    (2, "Shreya", 88),
    (3, "Shruti", 75),
    (4, "Chinmay", 90),
    (5, "Vishnu", 88),
    (6, "Nikita", 75),
]

df = spark.createDataFrame(data, ["id", "name", "marks"])

print("\nStudent Data:")
df.show()

# Write as a Delta table (using relative path for portability)
path = os.path.abspath("./delta-students")
df.write.format("delta").mode("overwrite").save(path)

print("\nRead back from Delta table:")
spark.read.format("delta").load(path).show()

# Update a row: change Nikita's marks to 80
dt = DeltaTable.forPath(spark, path)
dt.update("name = 'Nikita'", {"marks": "'80'"})

print("\nAfter update:")
spark.read.format("delta").load(path).show()

# Time Travel: query original version 0 before update
print("\nVersion 0 (before update):")
spark.read.format("delta").option("versionAsOf", 0).load(path).show()

# Display Transaction Log History
print("\nTable history:")
dt.history().select("version", "timestamp", "operation").show(truncate=False)

spark.stop()
```

---

## Expected Output

```
Spark started successfully!

Student Data:
+---+--------+-----+
| id|    name|marks|
+---+--------+-----+
|  1|Siddhant|   90|
|  2|  Shreya|   88|
|  3|  Shruti|   75|
|  4| Chinmay|   90|
|  5|  Vishnu|   88|
|  6|  Nikita|   75|
+---+--------+-----+

Read back from Delta table:
+---+--------+-----+
| id|    name|marks|
+---+--------+-----+
|  1|Siddhant|   90|
|  2|  Shreya|   88|
|  3|  Shruti|   75|
|  4| Chinmay|   90|
|  5|  Vishnu|   88|
|  6|  Nikita|   75|
+---+--------+-----+

After update:
+---+--------+-----+
| id|    name|marks|
+---+--------+-----+
|  1|Siddhant|   90|
|  4| Chinmay|   90|
|  3|  Shruti|   75|
|  2|  Shreya|   88|
|  5|  Vishnu|   88|
|  6|  Nikita|   80|
+---+--------+-----+

Version 0 (before update):
+---+--------+-----+
| id|    name|marks|
+---+--------+-----+
|  1|Siddhant|   90|
|  4| Chinmay|   90|
|  6|  Nikita|   75|
|  3|  Shruti|   75|
|  2|  Shreya|   88|
|  5|  Vishnu|   88|
+---+--------+-----+

Table history:
+-------+-----------------------+---------+
|version|timestamp              |operation|
+-------+-----------------------+---------+
|1      |2026-11-19 23:53:12.762|UPDATE   |
|0      |2026-11-19 23:52:54.059|WRITE    |
+-------+-----------------------+---------+
```

---

## Troubleshooting (Windows-Specific)

### Issue: `HADOOP_HOME and hadoop.home.dir are unset`
- **Cause:** Apache Spark requires native Windows Hadoop binaries (`winutils.exe` & `hadoop.dll`) for POSIX permission commands.
- **Fix:** Follow the steps under [Prerequisites](#prerequisites) to download `winutils.exe` and `hadoop.dll` into `C:\hadoop\bin` and set the `HADOOP_HOME` environment variable.
