from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import row_number, col

spark = (SparkSession.builder.appName("day1-clean")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    .getOrCreate())


BUCKET = "de-taxi-hema-2026"

#READ raw from S3
df = spark.read.parquet(f"s3a://{BUCKET}/raw/")
print("Row count from S3:", df.count())

#Clean - drop junk rows, remove duplicates
clean = (df
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("passenger_count") > 0)
    .filter(F.col("tpep_pickup_datetime") >= "2024-01-01")   # NEW
    .filter(F.col("tpep_pickup_datetime") <  "2024-02-01")
    .dropDuplicates())

print("Clean rows:", clean.count()) 

dedup_key = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "total_amount",
    
]

w = Window.partitionBy(*dedup_key).orderBy(F.col("tpep_pickup_datetime"))

clean = (clean
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn"))
print("Rows after deduplication:", clean.count())


#Write clean layer back to s3
(clean.write
    .mode("overwrite")
    .parquet(f"s3a://{BUCKET}/clean/"))

print(f"Cleaned data written to s3a://{BUCKET}/clean/")
spark.stop()
