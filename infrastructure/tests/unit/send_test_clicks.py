import boto3
import json
import time

firehose_stream_name = "ClickstreamDeliveryStream"

firehose = boto3.client("firehose",region_name = "us-east-1")

#put 10 small json records to the firehose stream
for i in range(10):
    record = {
        "user_id" : f"user_{i}",
        "event" : "click",
        "timestamp" : int(time.time())
    }

    firehose.put_record(
        DeliveryStreamName = firehose_stream_name,
        Record = {"Data" : json.dumps(record) + "\n"}
    )

    print("sent click event to firehose")