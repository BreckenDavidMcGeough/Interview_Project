import sys
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp
from pyspark.sql import SparkSession

args = getResolvedOptions(
    sys.argv,
    ["raw_bucket", "processed_bucket"]
)

raw_bucket = args["raw_bucket"]
processed_bucket = args["processed_bucket"]

spark = SparkSession.builder.appName("ClickstreamTransform").getOrCreate()

input_path = f"s3://{raw_bucket}/"
output_path = f"s3://{processed_bucket}/processed/"

df = spark.read.option("recursiveFileLookup","true").json(input_path)

transformed_df = df.withColumn(
    "processed_at",
    current_timestamp()
)

transformed_df.write.mode("overwrite").parquet(output_path)
