from aws_cdk import (
    # Duration,
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    RemovalPolicy
    # aws_sqs as sqs,
)
from constructs import Construct

class InfrastructureStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(self, 
            "RawClickstreamBucket",
            removal_policy = RemovalPolicy.DESTROY,
            auto_delete_objects = True
        )

        firehose_role = iam.Role(self, 
            "FirehoseRole",
            assumed_by = iam.ServicePrincipal("firehose.amazonaws.com")
        )

        bucket.grant_read_write(firehose_role)

        firehose.CfnDeliveryStream(self, 
            "ClickstreamDeliveryStream",
            delivery_stream_name = "ClickstreamDeliveryStream",
            delivery_stream_type = "DirectPut",
            s3_destination_configuration = {
                "bucketArn" : bucket.bucket_arn,
                "roleArn" : firehose_role.role_arn,
                "BufferingHints" : {
                    "SizeInMBs" : 128,
                    "IntervalInSeconds" : 300
                },
                "Prefix" : "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}",
                "CompressionFormat" : "UNCOMPRESSED"
            }
        )
