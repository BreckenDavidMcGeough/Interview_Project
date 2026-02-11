import sys
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, current_timestamp
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_S3_PATH", "DEST_S3_PREFIX"]
)

job_name = args["JOB_NAME"]
source_path = args["SOURCE_S3_PATH"]
dest_prefix = args["DEST_S3_PREFIX"]

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(job_name, args)

df = spark.read.json(source_path)

df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

transformed_df = (
    df.withColumn("processed_at", current_timestamp())
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
)

print(f"source_path: {source_path}\n")
print(f"dest_prefix: {dest_prefix}\n")

transformed_df.write.mode("append").partitionBy("year", "month", "day").parquet(dest_prefix)

job.commit()
