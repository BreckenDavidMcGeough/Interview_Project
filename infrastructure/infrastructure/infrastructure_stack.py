from aws_cdk import (
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

        #create s3 bucket for athena query output
        athena_results_bucket = s3.Bucket(self,
            "AthenaResultsBucket",
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
                            ]
        )

        #grant read write permission to our bucket for the firehose role
        raw_bucket.grant_read_write(firehose_role)

        #create firehose delivery stream 
        firehose.CfnDeliveryStream(self, 
            "ClickstreamDeliveryStream",
            delivery_stream_name = "ClickstreamDeliveryStream",
            delivery_stream_type = "DirectPut", #direct put since sending to data to firehose directly via put record
            s3_destination_configuration = firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn = raw_bucket.bucket_arn, #our destination bucket
                role_arn = firehose_role.role_arn, #necessary permissions
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty( #batch data, 128 mbs or 60s for this project
                    size_in_m_bs = 128,
                    interval_in_seconds = 60
                ),
                prefix = "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/", #partition data in bucket by date
                error_output_prefix = "errors/!{firehose:error-output-type}/",
                compression_format = "UNCOMPRESSED" #uncompressed for project
            )
        )

        #upload transfrom script to assets bucket for glue job to call
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

        #create glue job
        glue_job = glue.CfnJob(
            self,
            "GlueJob",
            name = "glue-job",
            role = glue_role.role_arn, #role for glue job
            glue_version = "4.0",
            command = glue.CfnJob.JobCommandProperty( #run transform script on data to write to processed bucket
                name = "glueetl",
                script_location = glue_script_asset.s3_object_url,
                python_version = "3"
            ),
            default_arguments = {
                "--job-language" : "python",
                "--TempDir" : f"s3://{processed_bucket.bucket_name}/temp/",
                "--enable-glue-datacatalog" : "true"
            }
        )

        #create lambda function
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

        #attach policy 
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

        #notify lambda function when new data enters s3 raw bucket
        raw_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(trigger_lambda),
        )

        #create iam role for glue crawler to get permissions 
        crawler_role = iam.Role(
            self, "CrawlerRole",
            assumed_by = iam.ServicePrincipal("glue.amazonaws.com")
        )

        #add policy
        crawler_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        )

        #grant crawler role read permissions to processed bucket
        processed_bucket.grant_read(crawler_role)

        #create database 
        glue_db = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id = self.account,
            database_input = glue.CfnDatabase.DatabaseInputProperty(
                name = "clickstream_db"
            )
        )

        #create glue crawler 
        glue_crawler = glue.CfnCrawler(
            self,
            "ProcessedCrawler",
            name = "Processed-Clickstream-Crawler",
            role = crawler_role.role_arn,
            database_name = "clickstream_db",
            table_prefix = "processed_", #table prefix to store in database
            targets = glue.CfnCrawler.TargetsProperty(
                s3_targets = [
                    glue.CfnCrawler.S3TargetProperty(
                        path = f"s3://{processed_bucket.bucket_name}/processed/"
                    )
                ]
            )
        )

        glue_crawler.add_dependency(glue_db)


