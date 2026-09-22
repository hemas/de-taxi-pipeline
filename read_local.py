from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("day1-local").getOrCreate()
df = spark.read.parquet("data/raw/yellow_tripdata_2024-01.parquet")

print("Row Count:", df.count())
df.show(5)
spark.stop()