#!/usr/bin/env python3
import aws_cdk as cdk

from fx_pipeline.fx_pipeline_stack import FxPipelineStack


app = cdk.App()

FxPipelineStack(
    app,
    "MgmFxPipelineStack",
    description="MGM Accounting FX data pipeline on AWS",
)

app.synth()

