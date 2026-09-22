from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("day1-s3")
   .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    .getOrCreate())


df= spark.read.parquet("s3a://de-taxi-hema-2026/raw/")
print("Row count from S3:", df.count())
#df.printSchema()
spark.stop()