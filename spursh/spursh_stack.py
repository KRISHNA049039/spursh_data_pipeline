"""
Spursh Stack — CDK
JE generation from AP/AR/IC ledgers, trial balance validation,
L7 manager approval gate, and multi-period FX booking to OFA.

Step Functions flow:
  Generate JEs → Validate TB → MEC Validate → Spursh Approval (human) → Book to OFA
"""
from pathlib import Path

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigateway as apigw,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class SpurshStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        project_root = Path(__file__).resolve().parent.parent

        # -----------------------------------------------------------------
        # Storage
        # -----------------------------------------------------------------
        gold_bucket = s3.Bucket(
            self, "SpurshGoldBucket",
            versioned=True, enforce_ssl=True,
            auto_delete_objects=True, removal_policy=RemovalPolicy.DESTROY,
        )
        quarantine_bucket = s3.Bucket(
            self, "SpurshQuarantineBucket",
            versioned=True, enforce_ssl=True,
            auto_delete_objects=True, removal_policy=RemovalPolicy.DESTROY,
        )
        ref_data_bucket = s3.Bucket(
            self, "SpurshRefDataBucket",
            versioned=True, enforce_ssl=True,
            auto_delete_objects=True, removal_policy=RemovalPolicy.DESTROY,
        )


        # -----------------------------------------------------------------
        # DynamoDB tables
        # -----------------------------------------------------------------
        je_table = dynamodb.Table(
            self, "JeTable",
            partition_key=dynamodb.Attribute(
                name="surrogate_key", type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="batch_id", type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        approval_table = dynamodb.Table(
            self, "SpurshApprovalTable",
            partition_key=dynamodb.Attribute(
                name="approval_id", type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        booking_audit_table = dynamodb.Table(
            self, "SpurshBookingAuditTable",
            partition_key=dynamodb.Attribute(
                name="booking_run_id", type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="journal_batch_id", type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # -----------------------------------------------------------------
        # SNS topic for L7 manager approval notifications
        # -----------------------------------------------------------------
        approval_topic = sns.Topic(
            self, "SpurshApprovalTopic",
            display_name="Spursh L7 Manager Approval Requests",
        )
        # Subscribe L7 manager email — parameterise in production
        approval_topic.add_subscription(
            subs.EmailSubscription("l7-manager@example.com")
        )


        # -----------------------------------------------------------------
        # Lambda functions
        # -----------------------------------------------------------------
        je_generator_fn = lambda_.Function(
            self, "JeGeneratorFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(str(project_root / "lambdas" / "je_generator")),
            timeout=Duration.seconds(120),
            memory_size=512,
            environment={
                "JE_TABLE": je_table.table_name,
                "GOLD_BUCKET": gold_bucket.bucket_name,
            },
        )
        je_table.grant_read_write_data(je_generator_fn)
        gold_bucket.grant_put(je_generator_fn)

        tb_validator_fn = lambda_.Function(
            self, "TrialBalanceValidatorFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(str(project_root / "lambdas" / "trial_balance_validator")),
            timeout=Duration.seconds(120),
            memory_size=512,
            environment={
                "QUARANTINE_BUCKET": quarantine_bucket.bucket_name,
                "REF_DATA_BUCKET": ref_data_bucket.bucket_name,
            },
        )
        quarantine_bucket.grant_put(tb_validator_fn)
        ref_data_bucket.grant_read(tb_validator_fn)

        mec_validator_fn = lambda_.Function(
            self, "SpurshMecValidatorFunction",
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
        quarantine_bucket.grant_put(mec_validator_fn)

        spursh_approval_fn = lambda_.Function(
            self, "SpurshApprovalFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(str(project_root / "lambdas" / "spursh_approval")),
            timeout=Duration.seconds(30),
            environment={
                "APPROVAL_TOPIC_ARN": approval_topic.topic_arn,
                "APPROVAL_TABLE": approval_table.table_name,
            },
        )
        approval_topic.grant_publish(spursh_approval_fn)
        approval_table.grant_read_write_data(spursh_approval_fn)

        fx_period_booking_fn = lambda_.Function(
            self, "FxPeriodBookingFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(str(project_root / "lambdas" / "fx_period_booking")),
            timeout=Duration.seconds(120),
            environment={
                "BOOKING_AUDIT_TABLE": booking_audit_table.table_name,
                "GOLD_BUCKET": gold_bucket.bucket_name,
            },
        )
        booking_audit_table.grant_read_write_data(fx_period_booking_fn)
        gold_bucket.grant_read_write(fx_period_booking_fn)

        spursh_callback_fn = lambda_.Function(
            self, "SpurshCallbackFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=lambda_.Code.from_asset(str(project_root / "lambdas" / "spursh_callback")),
            timeout=Duration.seconds(30),
            environment={
                "APPROVAL_TABLE": approval_table.table_name,
            },
        )
        approval_table.grant_read_write_data(spursh_callback_fn)


        # -----------------------------------------------------------------
        # API Gateway — approval callback endpoint for L7 managers
        # -----------------------------------------------------------------
        approval_api = apigw.RestApi(
            self, "SpurshApprovalApi",
            rest_api_name="Spursh Approval API",
            description="L7 manager approval/rejection endpoint for Spursh JE batches",
        )

        approve_resource = approval_api.root.add_resource("approve")
        approve_resource.add_method(
            "GET",
            apigw.LambdaIntegration(spursh_callback_fn),
        )

        # Pass API endpoint to approval lambda so it can build callback URLs
        spursh_approval_fn.add_environment(
            "API_ENDPOINT", approval_api.url.rstrip("/")
        )

        # Callback lambda needs permission to send task success/failure
        spursh_callback_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "states:SendTaskSuccess",
                    "states:SendTaskFailure",
                ],
                resources=["*"],
            )
        )


        # -----------------------------------------------------------------
        # Step Functions — Spursh JE pipeline
        # Generate JEs → TB Validate → MEC Validate → L7 Approval → Book
        # -----------------------------------------------------------------

        # Step 1: Generate JEs from AP/AR/IC ledgers
        generate_jes = tasks.LambdaInvoke(
            self, "GenerateJEs",
            lambda_function=je_generator_fn,
            result_selector={"payload.$": "$.Payload"},
            result_path="$.je_result",
        )

        # Step 2: Validate against trial balance
        validate_tb = tasks.LambdaInvoke(
            self, "ValidateTrialBalance",
            lambda_function=tb_validator_fn,
            payload=sfn.TaskInput.from_object({
                "batch_id.$": "$.je_result.payload.batch_id",
                "journal_entries.$": "$.journal_lines",
                "expected_geos.$": "$.je_result.payload.geos",
            }),
            result_selector={"payload.$": "$.Payload"},
            result_path="$.tb_validation",
        )

        # Step 3: MEC validation (24-col, Dr=Cr, 7-segment)
        validate_mec = tasks.LambdaInvoke(
            self, "SpurshValidateMec",
            lambda_function=mec_validator_fn,
            payload=sfn.TaskInput.from_object({
                "batch_id.$": "$.je_result.payload.batch_id",
                "journal_lines.$": "$.journal_lines",
                "gold_bucket": gold_bucket.bucket_name,
            }),
            result_selector={"payload.$": "$.Payload"},
            result_path="$.mec_validation",
        )

        # Step 4: Spursh L7 manager approval (human-in-the-loop)
        # Uses WAIT_FOR_TASK_TOKEN so Step Functions pauses until
        # the manager clicks approve/reject via API Gateway
        request_approval = tasks.LambdaInvoke(
            self, "RequestL7Approval",
            lambda_function=spursh_approval_fn,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object({
                "batch_id.$": "$.je_result.payload.batch_id",
                "journal_entries.$": "$.journal_lines",
                "period_type.$": "$.period_type",
                "effective_date.$": "$.effective_date",
                "geos.$": "$.je_result.payload.geos",
                "ledger_sources.$": "$.je_result.payload.ledger_sources",
                "task_token": sfn.JsonPath.task_token,
            }),
            result_path="$.approval",
        )

        # Step 5: Book FX adjustments to OFA
        book_fx = tasks.LambdaInvoke(
            self, "BookFxAdjustments",
            lambda_function=fx_period_booking_fn,
            payload=sfn.TaskInput.from_object({
                "batch_id.$": "$.je_result.payload.batch_id",
                "period_type.$": "$.period_type",
                "journal_entries.$": "$.journal_lines",
                "approval.$": "$.approval",
            }),
            result_selector={"payload.$": "$.Payload"},
            result_path="$.booking",
        )


        # -----------------------------------------------------------------
        # Choice states for validation gates
        # -----------------------------------------------------------------
        tb_passed = sfn.Choice(self, "TBPassed")
        tb_failed = sfn.Fail(
            self, "TBFailed",
            cause="Trial balance validation failed",
            error="TrialBalanceError",
        )

        mec_passed = sfn.Choice(self, "SpurshMecPassed")
        mec_failed = sfn.Fail(
            self, "SpurshMecFailed",
            cause="MEC validation failed",
            error="MECValidationError",
        )

        approval_rejected = sfn.Fail(
            self, "ApprovalRejected",
            cause="L7 manager rejected the batch",
            error="SpurshRejected",
        )

        success = sfn.Succeed(self, "SpurshBooked")

        # -----------------------------------------------------------------
        # Wire the state machine
        # -----------------------------------------------------------------
        definition = (
            generate_jes
            .next(validate_tb)
            .next(
                tb_passed.when(
                    sfn.Condition.boolean_equals(
                        "$.tb_validation.payload.is_valid", True
                    ),
                    validate_mec.next(
                        mec_passed.when(
                            sfn.Condition.boolean_equals(
                                "$.mec_validation.payload.is_valid", True
                            ),
                            request_approval
                            .next(book_fx)
                            .next(success),
                        ).otherwise(mec_failed)
                    ),
                ).otherwise(tb_failed)
            )
        )

        state_machine = sfn.StateMachine(
            self, "SpurshPipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(72),  # 3 days for human approval window
        )

        # Grant the approval lambda permission to be called by SFN
        state_machine.grant_task_response(spursh_callback_fn)
