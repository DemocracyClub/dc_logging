#!/usr/bin/env python3
import os

from aws_cdk import App, Environment, Tags
from stacks.base_reporting_stack import BaseReportingStack
from stacks.dc_logs_stack import DCLogsStack
from stacks.postcode_searches_stack import PostcodeSearchesStack

valid_environments = (
    "development",
    "staging",
    "production",
)

app_wide_context = {}
if dc_env := os.environ.get("DC_ENVIRONMENT"):
    app_wide_context["dc-environment"] = dc_env

app = App(context=app_wide_context)

# Set the DC Environment early on. This is important to be able to conditionally
# change the stack configurations
dc_environment = app.node.try_get_context("dc-environment") or None
assert (
    dc_environment in valid_environments
), f"context `dc-environment` must be one of {valid_environments}"

DCLogsStack(
    app,
    "DCLogsStack",
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region="eu-west-2"
    ),
)

BaseReportingStack(
    app,
    "BaseReportingStack",
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region="eu-west-2"
    ),
)

PostcodeSearchesStack(
    app,
    "PostcodeSearchesStack",
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region="eu-west-2"
    ),
)

Tags.of(app).add("dc-product", "dc-logging")
Tags.of(app).add("dc-environment", dc_environment)

app.synth()
