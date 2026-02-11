from aws_cdk import (
    # Duration,
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    aws_glue as glue,
    aws_s3_assets as s3_assets,
    aws_s3_deployment as s3_deployment,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    Duration,
    RemovalPolicy
    # aws_sqs as sqs,
)
from constructs import Construct

#create stack 
class InfrastructureStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #create s3 bucket for the raw data
        raw_bucket = s3.Bucket(self, 
            "RawClickstreamBucket",
            removal_policy = RemovalPolicy.DESTROY,
            auto_delete_objects = True
        )

        #create s3 bucket for the processed data 
        processed_bucket = s3.Bucket(self,
            "ProcessedClickstreamBucket",
            removal_policy = RemovalPolicy.DESTROY,
            auto_delete_objects = True
        )

        #create iam role for firehose to assign permissions 
        firehose_role = iam.Role(self, 
            "FirehoseRole",
            assumed_by = iam.ServicePrincipal("firehose.amazonaws.com")
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

        #upload transfrom script to assets bucket
        glue_script_asset = s3_assets.Asset(
            self, "GlueScriptAsset",
            path = "glue/transform.py"
        )

        #grant read permission to bucket with raw clickstream data for glue role
        raw_bucket.grant_read(glue_role)
        #grant read write permissions to processed bucket for glue role to write to 
        processed_bucket.grant_read_write(glue_role)
        #grant read permissions to assets bucket for glue role to read transform script
        glue_script_asset.bucket.grant_read(glue_role)

        #create the glue job that will read data from the raw bucket, run the transform script on it and write the output parquet
        #file to the processed bucket
        glue_job = glue.CfnJob(
            self,
            "GlueJob",
            name = "glue-job",
            role = glue_role.role_arn,
            glue_version = "4.0",
            command = glue.CfnJob.JobCommandProperty(
                name = "glueetl",
                script_location = glue_script_asset.s3_object_url,
                python_version = "3"
            ),
            default_arguments = {
                #"--raw_bucket" : raw_bucket.bucket_name,
                #"--processed_bucket" : processed_bucket.bucket_name,
                "--job-language" : "python",
                "--TempDir" : f"s3://{processed_bucket.bucket_name}/temp/",
                "--enable-glue-datacatalog" : "true"
                #"--enable-continuous-cloudwatch-log" : "true"
            }
        )


        trigger_lambda = _lambda.Function(
            self,
            "TriggerLambda",
            runtime = _lambda.Runtime.PYTHON_3_10,
            handler = "handler.lambda_handler",
            code = _lambda.Code.from_asset("trigger"),
            timeout = Duration.seconds(60),
            memory_size = 128,
            environment = {
                "GLUE_JOB_NAME" : glue_job.name,
                "PROCESSED_BUCKET" : processed_bucket.bucket_name
            }
        )

        trigger_lambda.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        trigger_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["glue:StartJobRun"],
                resources = ["*"]
            )
        )

        raw_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(trigger_lambda),
            #s3.NotificationKeyFilter(prefix = "")
        )






