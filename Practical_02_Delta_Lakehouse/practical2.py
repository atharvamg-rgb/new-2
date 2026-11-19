import os
import sys

# FIX: make Spark use the same Python executable that runs this script
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

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
