from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("export-agg")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    .getOrCreate())

BUCKET = "de-taxi-hema-2026"

daily = spark.read.parquet(f"s3a://{BUCKET}/aggregate/")

#coalesce(1) -> one part; write to a local folder
(daily.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv("out_agg"))

print("Wrote local CSV to out_agg folder")
spark.stop()