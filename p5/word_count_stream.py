import os
import sys
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# FIX: ensure Spark uses current Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

# Ensure writable local temporary directory for Spark checkpointing
temp_dir = os.path.abspath("./spark-temp")
os.makedirs(temp_dir, exist_ok=True)
tempfile.tempdir = temp_dir

# Create Spark Session
spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("WordCountStream")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("Spark Structured Streaming Initialized. Connecting to socket localhost:9999...")

# Read streaming data from Python socket server
data = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Split each line into individual words
words = data.select(
    explode(split(data.value, "\\s+")).alias("word")
).filter("word != ''")

# Count occurrences of each word
counts = words.groupBy("word").count()

# Output updated word counts in console on each micro-batch
query = (
    counts.writeStream
    .outputMode("complete")
    .format("console")
    .start()
)

print("Streaming query started. Awaiting batches...\n")
query.awaitTermination()
