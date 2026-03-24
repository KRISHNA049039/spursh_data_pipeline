from pathlib import Path

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_s3_notifications as s3n,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class FxPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bronze_bucket = s3.Bucket(
            self,
            "BronzeBucket",
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        silver_bucket = s3.Bucket(
            self,
            "SilverBucket",
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        gold_bucket = s3.Bucket(
            self,
            "GoldBucket",
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        quarantine_bucket = s3.Bucket(
            self,
            "QuarantineBucket",
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        scripts_bucket = s3.Bucket(
            self,
            "ScriptsBucket",
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        lake_database = glue.CfnDatabase(
            self,
            "FxLakeDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="mgm_fx_lake"
            ),
        )

        idempotency_table = dynamodb.Table(
            self,
            "FxIdempotencyTable",
            partition_key=dynamodb.Attribute(
                name="surrogate_key", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="stage", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        booking_audit_table = dynamodb.Table(
            self,
            "FxBookingAuditTable",
            partition_key=dynamodb.Attribute(
                name="booking_run_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="journal_batch_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        intercompany_stream = kinesis.Stream(
            self,
            "IntercompanyFxStream",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            retention_period=Duration.hours(24),
        )

        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        for bucket in [
            bronze_bucket,
            silver_bucket,
            gold_bucket,
            quarantine_bucket,
            scripts_bucket,
        ]:
            bucket.grant_read_write(glue_role)

        idempotency_table.grant_read_write_data(glue_role)

        project_root = Path(__file__).resolve().parent.parent
        glue_dir = str(project_root / "glue")

        s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",
            destination_bucket=scripts_bucket,
            destination_key_prefix="glue",
            sources=[s3deploy.Source.asset(glue_dir)],
        )

        ref_data_bucket = s3.Bucket(
            self,
            "RefDataBucket",
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        ref_data_bucket.grant_read(glue_role)

        cleanse_job = glue.CfnJob(
            self,
            "SilverCleanseJob",
            name="mgm-fx-silver-cleanse",
            role=glue_role.role_arn,
            glue_version="4.0",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/glue/cleanse_job.py",
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--datalake-formats": "parquet",
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
            max_retries=1,
            number_of_workers=2,
            worker_type="G.1X",
            timeout=30,
        )
        cleanse_job.add_dependency(lake_database)

        gold_job = glue.CfnJob(
            self,
            "GoldShapeJob",
            name="mgm-fx-gold-shape",
            role=glue_role.role_arn,
            glue_version="4.0",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/glue/gold_job.py",
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--datalake-formats": "parquet",
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
            max_retries=1,
            number_of_workers=2,
            worker_type="G.1X",
            timeout=30,
        )
        gold_job.add_dependency(lake_database)

        batch_trigger_fn = lambda_.Function(
            self,
            "BatchTriggerFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(str(project_root / "lambdas" / "batch_trigger")),
            timeout=Duration.seconds(30),
        )

        stream_ingestor_fn = lambda_.Function(
            self,
            "StreamIngestorFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(str(project_root / "lambdas" / "stream_ingestor")),
            timeout=Duration.seconds(60),
            environment={
                "BRONZE_BUCKET": bronze_bucket.bucket_name,
                "IDEMPOTENCY_TABLE": idempotency_table.table_name,
            },
        )

        mec_validator_fn = lambda_.Function(
            self,
            "MecValidatorFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(str(project_root / "lambdas" / "mec_validator")),
            timeout=Duration.seconds(120),
            memory_size=512,
            environment={
                "QUARANTINE_BUCKET": quarantine_bucket.bucket_name,
                "GOLD_BUCKET": gold_bucket.bucket_name,
                "REF_DATA_BUCKET": ref_data_bucket.bucket_name,
            },
        )

        ofa_booking_fn = lambda_.Function(
            self,
            "OfaBookingFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(str(project_root / "lambdas" / "ofa_booking")),
            timeout=Duration.seconds(60),
            environment={
                "BOOKING_AUDIT_TABLE": booking_audit_table.table_name,
            },
        )

        bronze_bucket.grant_read(batch_trigger_fn)
        bronze_bucket.grant_put(stream_ingestor_fn)
        quarantine_bucket.grant_put(mec_validator_fn)
        gold_bucket.grant_read(mec_validator_fn)
        ref_data_bucket.grant_read(mec_validator_fn)
        idempotency_table.grant_read_write_data(stream_ingestor_fn)
        booking_audit_table.grant_read_write_data(ofa_booking_fn)
        intercompany_stream.grant_read(stream_ingestor_fn)

        stream_ingestor_fn.add_event_source(
            lambda_event_sources.KinesisEventSource(
                intercompany_stream,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                retry_attempts=2,
            )
        )

        run_cleanse = tasks.GlueStartJobRun(
            self,
            "RunSilverCleanse",
            glue_job_name=cleanse_job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object(
                {
                    "--SOURCE_BUCKET.$": "$.source_bucket",
                    "--SOURCE_KEY.$": "$.source_key",
                    "--SILVER_BUCKET": silver_bucket.bucket_name,
                    "--QUARANTINE_BUCKET": quarantine_bucket.bucket_name,
                    "--IDEMPOTENCY_TABLE": idempotency_table.table_name,
                }
            ),
            result_path="$.silver_job",
        )

        run_gold = tasks.GlueStartJobRun(
            self,
            "RunGoldShape",
            glue_job_name=gold_job.ref,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object(
                {
                    "--SILVER_BUCKET": silver_bucket.bucket_name,
                    "--GOLD_BUCKET": gold_bucket.bucket_name,
                    "--BATCH_ID.$": "$.batch_id",
                }
            ),
            result_path="$.gold_job",
        )

        validate_mec = tasks.LambdaInvoke(
            self,
            "ValidateMec",
            lambda_function=mec_validator_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "batch_id.$": "$.batch_id",
                    "source_bucket.$": "$.source_bucket",
                    "source_key.$": "$.source_key",
                    "gold_bucket": gold_bucket.bucket_name,
                    "journal_lines.$": "$.journal_lines",
                }
            ),
            result_selector={"payload.$": "$.Payload"},
            result_path="$.mec_validation",
        )

        book_ofa = tasks.LambdaInvoke(
            self,
            "BookToOfa",
            lambda_function=ofa_booking_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "batch_id.$": "$.batch_id",
                    "source_bucket.$": "$.source_bucket",
                    "source_key.$": "$.source_key",
                    "validation.$": "$.mec_validation.payload",
                }
            ),
            result_selector={"payload.$": "$.Payload"},
            result_path="$.booking",
        )

        mec_passed = sfn.Choice(self, "MecPassed")
        mec_failed = sfn.Fail(self, "MecFailed", cause="MEC validation failed")
        success = sfn.Succeed(self, "Booked")

        definition = (
            run_cleanse.next(run_gold)
            .next(validate_mec)
            .next(
                mec_passed.when(
                    sfn.Condition.boolean_equals("$.mec_validation.payload.is_valid", True),
                    book_ofa.next(success),
                ).otherwise(mec_failed)
            )
        )

        state_machine = sfn.StateMachine(
            self,
            "ArFxBatchStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(30),
        )

        state_machine.grant_start_execution(batch_trigger_fn)
        batch_trigger_fn.add_environment("STATE_MACHINE_ARN", state_machine.state_machine_arn)

        bronze_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(batch_trigger_fn),
            s3.NotificationKeyFilter(prefix="landing/ar_fx/"),
        )

