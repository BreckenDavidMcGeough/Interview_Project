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
    ["JOB_NAME","raw_bucket", "processed_bucket"]
)

raw_bucket = args["raw_bucket"]
processed_bucket = args["processed_bucket"]
job_name = args["JOB_NAME"]

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(job_name, args)

input_path = f"s3://{raw_bucket}/"
output_path = f"s3://{processed_bucket}/processed/"

df = spark.read.option("recursiveFileLookup", "true").json(input_path)

df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

transformed_df = (
    df.withColumn("processed_at", current_timestamp())
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
)



transformed_df.write.mode("append").partitionBy("year", "month", "day").parquet(output_path)

job.commit()
