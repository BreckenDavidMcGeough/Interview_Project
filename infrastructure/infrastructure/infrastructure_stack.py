from aws_cdk import (
    # Duration,
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    aws_glue as glue,
    aws_s3_assets as s3_assets,
    aws_s3_deployment as s3_deployment,
    RemovalPolicy
    # aws_sqs as sqs,
)
from constructs import Construct

#create stack 
class InfrastructureStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #create s3 bucket
        raw_bucket = s3.Bucket(self, 
            "RawClickstreamBucket",
            removal_policy = RemovalPolicy.DESTROY,
            auto_delete_objects = True
        )

        #create iam role for firehose to assign permissions 
        firehose_role = iam.Role(self, 
            "FirehoseRole",
            assumed_by = iam.ServicePrincipal("firehose.amazonaws.com")
        )

        #grant read write permission to our bucket for the firehose role
        raw_bucket.grant_read_write(firehose_role)

        #create firehose delivery stream and specify direct put so data gets sent diretly into firehose, and specify bucket arn 
        #to put data into the s3 bucket as well as role arn to assume role to get permissions, then batch save and partition by date
        firehose.CfnDeliveryStream(self, 
            "ClickstreamDeliveryStream",
            delivery_stream_name = "ClickstreamDeliveryStream",
            delivery_stream_type = "DirectPut",
            s3_destination_configuration = firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn = raw_bucket.bucket_arn,
                role_arn = firehose_role.role_arn,
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    size_in_m_bs = 128,
                    interval_in_seconds = 60
                ),
                prefix = "!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/",
                error_output_prefix = "errors/!{firehose:error-output-type}/",
                compression_format = "UNCOMPRESSED"
            )
        )

        #create s3 bucket for the processed data 
        processed_bucket = s3.Bucket(self,
            "ProcessedClickstreamBucket",
            removal_policy = RemovalPolicy.DESTROY,
            auto_delete_objects = True
        )

        #create iam role for glue
        glue_role = iam.Role(self,
            "GlueJobRole",
            assumed_by = iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies = [
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
                #iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        #grant read permission to bucket with raw clickstream data for glue role
        raw_bucket.grant_read(glue_role)
        #grant read write permissions to processed bucket for glue role to write to 
        processed_bucket.grant_read_write(glue_role)

        script_bucket = s3.Bucket(self,
            "GlueScriptBucket"
        )

        s3_deployment.BucketDeployment(
            self,
            "DeployGlueScript",
            sources = [s3_deployment.Source.asset("glue")],
            destination_bucket = script_bucket,
            destination_key_prefix = "script"
        )

        script_bucket.grant_read(glue_role)

        glue.CfnJob(
            self,
            "GlueJob",
            name = "glue-job",
            role = glue_role.role_arn,
            glue_version = "4.0",
            command = glue.CfnJob.JobCommandProperty(
                name = "glueetl",
                script_location = f"s3://{script_bucket.bucket_name}/script/transform.py",
                python_version = "3"
            ),
            default_arguments = {
                "--raw_bucket" : raw_bucket.bucket_name,
                "--processed_bucket" : processed_bucket.bucket_name,
                "--job-language" : "python",
                "--TempDir" : f"s3://{processed_bucket.bucket_name}/temp/"
                #"--enable-continuous-cloudwatch-log" : "true"
            }
        )


