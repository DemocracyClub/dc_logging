#!/usr/bin/env python3
import os

from aws_cdk import core as cdk
from stacks.dc_logs_stack import DCLogsStack

app = cdk.App()
DCLogsStack(
    app,
    "DCLogsStack",
    env=cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region="eu-west-2"),
)

app.synth()
