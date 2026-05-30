from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigateway as apigateway,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from constructs import Construct


class UrbanAnomalyStack(Stack):
    """Responsible, auditable sensor-fusion pipeline for anomaly triage."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        project_root = Path(__file__).resolve().parent.parent

        bronze_bucket = s3.Bucket(
            self,
            "UrbanBronzeBucket",
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        gold_bucket = s3.Bucket(
            self,
            "UrbanGoldBucket",
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        quarantine_bucket = s3.Bucket(
            self,
            "UrbanQuarantineBucket",
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        dashboard_bucket = s3.Bucket(
            self,
            "UrbanDashboardBucket",
            website_index_document="index.html",
            enforce_ssl=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            public_read_access=False,
        )

        event_stream = kinesis.Stream(
            self,
            "UrbanSensorEventStream",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            retention_period=Duration.hours(24),
        )

        event_idempotency_table = dynamodb.Table(
            self,
            "UrbanEventIdempotencyTable",
            partition_key=dynamodb.Attribute(
                name="event_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="stage", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        alert_table = dynamodb.Table(
            self,
            "UrbanAlertTable",
            partition_key=dynamodb.Attribute(
                name="city", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="alert_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        alert_table.add_global_secondary_index(
            index_name="severity-created-at-index",
            partition_key=dynamodb.Attribute(
                name="severity", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="created_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        lake_database = glue.CfnDatabase(
            self,
            "UrbanAnomalyLakeDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="urban_anomaly_lake"
            ),
        )

        glue_role = iam.Role(
            self,
            "UrbanGlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        for bucket in [bronze_bucket, gold_bucket, quarantine_bucket]:
            bucket.grant_read_write(glue_role)

        ingestor_fn = lambda_.Function(
            self,
            "UrbanEventIngestorFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(
                str(project_root / "lambdas" / "urban_event_ingestor")
            ),
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "BRONZE_BUCKET": bronze_bucket.bucket_name,
                "QUARANTINE_BUCKET": quarantine_bucket.bucket_name,
                "IDEMPOTENCY_TABLE": event_idempotency_table.table_name,
            },
        )

        scorer_fn = lambda_.Function(
            self,
            "UrbanAnomalyScorerFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(
                str(project_root / "lambdas" / "urban_anomaly_scorer")
            ),
            timeout=Duration.seconds(60),
            memory_size=512,
            environment={
                "ALERT_TABLE": alert_table.table_name,
                "GOLD_BUCKET": gold_bucket.bucket_name,
                "QUARANTINE_BUCKET": quarantine_bucket.bucket_name,
                "MIN_ALERT_SCORE": "70",
            },
        )

        dashboard_api_fn = lambda_.Function(
            self,
            "UrbanDashboardApiFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(
                str(project_root / "lambdas" / "urban_dashboard_api")
            ),
            timeout=Duration.seconds(30),
            environment={
                "ALERT_TABLE": alert_table.table_name,
                "DEFAULT_CITY": "New Delhi",
            },
        )

        api = apigateway.LambdaRestApi(
            self,
            "UrbanAnomalyDashboardApi",
            handler=dashboard_api_fn,
            proxy=True,
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=["GET", "OPTIONS"],
                allow_headers=["Content-Type", "Authorization"],
            ),
        )

        ingestor_fn.add_event_source(
            lambda_event_sources.KinesisEventSource(
                event_stream,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                retry_attempts=2,
            )
        )
        scorer_fn.add_event_source(
            lambda_event_sources.KinesisEventSource(
                event_stream,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                retry_attempts=2,
            )
        )

        event_stream.grant_read(ingestor_fn)
        event_stream.grant_read(scorer_fn)
        bronze_bucket.grant_put(ingestor_fn)
        quarantine_bucket.grant_put(ingestor_fn)
        gold_bucket.grant_put(scorer_fn)
        quarantine_bucket.grant_put(scorer_fn)
        event_idempotency_table.grant_read_write_data(ingestor_fn)
        alert_table.grant_read_write_data(scorer_fn)
        alert_table.grant_read_data(dashboard_api_fn)

        s3deploy.BucketDeployment(
            self,
            "DeployUrbanDashboard",
            destination_bucket=dashboard_bucket,
            sources=[
                s3deploy.Source.asset(str(project_root / "dashboard" / "urban-anomaly"))
            ],
        )

        self.event_stream_name = event_stream.stream_name
        self.dashboard_api_url = api.url
        self.dashboard_bucket_name = dashboard_bucket.bucket_name

        CfnOutput(
            self,
            "UrbanSensorEventStreamName",
            value=event_stream.stream_name,
        )
        CfnOutput(
            self,
            "UrbanDashboardApiUrl",
            value=api.url,
        )
        CfnOutput(
            self,
            "UrbanDashboardBucketName",
            value=dashboard_bucket.bucket_name,
        )

        lake_database.node.add_dependency(alert_table)
