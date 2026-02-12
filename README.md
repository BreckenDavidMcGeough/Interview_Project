# Interview_Project

# Key File Locations

Interview_Project/infrastructure/infrastructure/infrastructure_stack.py is where stack code lives

Interview_Project/infrastructure/glue/transform.py is where the transform script lives

Interview_Project/infrastructure/tests/unit/send_clicks.py is where the test file lives that puts test json data to the firehose

Interview_Project/infrastructure/trigger/handle.py is where the lambda handler function lives 



# Process:
The stack creates three S3 buckets, one for raw json clickstream data, one for processed parquet data, and one for Athena query result output. a Kinesis Firehose delivery stream is configured to ingest the clickstream data directly via direct put into the raw bucket. When new data lands in the raw bucket (when send_test_scripts.py script is ran and data is put to the firehose) and S3 event triggers a lambda function which starts an AWS Glue job, which transforms the raw json into parquet format and writes it to the processed bucket (using the transform.py script). Then a glue Crawler scans the processed bucket, creating or updating a table in the Glue Data catalog. Finally, Athena can query the processed parquet data using sql.



# How to run:
- cd into Interview_Project/infrastructure (1st infrastructure folder in Interview_Project repo)
- python -m venv .venv
- source .venv/bin/activate
- pip install -r requirements.txt
- cdk bootstrap (if necessary)
- cdk deploy
- pip install boto3 (if necessary)
- python tests/unit/send_test_clicks.py
#wait 3 minutes or run   
#aws glue get-job-runs \  
#--job-name glue-job \                   
#--max-results 1 \  
#--query "JobRuns[0].JobRunState" \  
#--output text -->  
#to know if job succeeded   
- aws glue start-crawler --name Processed-Clickstream-Crawler
#run a sample query below and check athena s3 bucket for csv file, which contains the queried data



# Use of AI:
I conversed and consulted with ChatGPT on the project for conceptual clarity and troubleshooting. I talked to it about the conceptual aspects, including understanding AWS services like Firehose, S3, Lambda, Glue and how they fit together into a pipeline. I also consulted it when reading the documentation of the AWS sdk for python to fully understand key function parameters. I used it most heavily when troubleshooting runtime errors and unexpected behavior with the services (ex. data not appearing in my buckets, lambda not triggering glue job, etc.).


# Sample Athena queries:
"SELECT * FROM clickstream_db.processed_year_2026 WHERE month='2' AND day='<current_day>' LIMIT 50;"

"SELECT month, COUNT(DISTINCT user_id) AS unique_users FROM clickstream_db.processed_year_2026 WHERE month='2'


# Notes
I had a lot of fun with this project and learned a lot about IaC aws cdk, amazon web services and serverless data processing pipelines 
