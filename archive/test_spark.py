import os
import sys

# Tell Spark to use the same Python as our venv (prevents a common Windows error)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("test")
         .master("local[2]")                               # use only 2 CPU cores
         .config("spark.driver.host", "127.0.0.1")         # run only on this laptop
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.memory", "1g")              # use at most 1 GB RAM
         .config("spark.sql.shuffle.partitions", "4")      # small data = few pieces
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 1) Read our synthetic data with Spark
df = spark.read.csv("data/raw/pakistan_air_quality.csv", header=True, inferSchema=True)
print("Spark version:", spark.version)
print("Rows:", df.count())

# 2) Do a simple calculation with Spark
df.groupBy("city").avg("aqi").show()

# 3) Save a small file with Spark (this needs winutils on Windows)
df.limit(5).write.mode("overwrite").csv("data/processed/spark_test")
print("SPARK IS WORKING!")

spark.stop()