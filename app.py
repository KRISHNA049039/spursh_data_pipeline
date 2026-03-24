#!/usr/bin/env python3
import aws_cdk as cdk

from fx_pipeline.fx_pipeline_stack import FxPipelineStack
from spursh.spursh_stack import SpurshStack


app = cdk.App()

FxPipelineStack(
    app,
    "MgmFxPipelineStack",
    description="MGM Accounting FX data pipeline on AWS",
)

SpurshStack(
    app,
    "SpurshApprovalStack",
    description="Spursh JE validation & L7 approval system — AP/AR/IC multi-period FX booking",
)

app.synth()

