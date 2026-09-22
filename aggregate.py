from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("day1-aggregate")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    .getOrCreate())

BUCKET = "de-taxi-hema-2026"

#READ clean from S3
df = spark.read.parquet(f"s3a://{BUCKET}/clean/")

#Aggregate - daily trips, revenue , avg fare

daily = (df
.withColumn("trip_date", F.to_date(F.col("tpep_pickup_datetime")))
 .groupBy("trip_date")
 .agg(
    F.count("*").alias("total_trips"),
    F.round(F.sum("fare_amount"),2).alias("total_revenue"),
    F.round(F.avg("fare_amount"),2).alias("avg_fare"))
    .orderBy("trip_date"))

    
daily.show(10)

#Write aggregate layer back to s3
(daily.write.mode("overwrite").parquet(f"s3a://{BUCKET}/aggregate/"))
print("Aggregate data written")
spark.stop()