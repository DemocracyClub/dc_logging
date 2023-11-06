#!/usr/bin/env python3
import os

from aws_cdk import App, Environment
from stacks.dc_logs_stack import DCLogsStack

app = App()
DCLogsStack(
    app,
    "DCLogsStack",
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region="eu-west-2"
    ),
)

app.synth()
