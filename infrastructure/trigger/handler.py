import json
import os
import boto3
import urllib.parse

glue = boto3.client("glue")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]

#start glue job and pass necessary arguments
def lambda_handler(event, context):
    job_run_ids = []

    for record in event.get("Records", []):
        raw_bucket = record["s3"]["bucket"]["name"]
        key_encoded = record["s3"]["object"]["key"]
        key = urllib.parse.unquote_plus(key_encoded)

        if key.endswith("/"):
            continue

        source_path = f"s3://{raw_bucket}/{key}"
        dest_prefix = f"s3://{PROCESSED_BUCKET}/processed/" #hardcode the processed prefix

        resp = glue.start_job_run(
            JobName = GLUE_JOB_NAME,
            Arguments = {
                "--SOURCE_S3_PATH" : source_path, #pass arguments for transform script so it can read and write to correct buckets
                "--DEST_S3_PREFIX" : dest_prefix
            }
        )

        job_run_id = resp["JobRunId"]
        job_run_ids.append(job_run_id)

        return {"status" : "ok"}