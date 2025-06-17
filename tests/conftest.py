import os
import signal
import subprocess
import time
import zipfile
from io import BytesIO
from pathlib import Path

import boto3
import pytest
import yaml
from moto import mock_aws
from mypy_boto3_firehose import FirehoseClient
from mypy_boto3_s3 import S3Client

from dc_logging_client import DCWidePostcodeLoggingClient


@pytest.fixture(scope="session", autouse=True)
def moto_proxy_start():
    os.environ["TEST_PROXY_MODE"] = "true"
    moto_proxy = subprocess.Popen(
        "moto_proxy -H 0.0.0.0",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
        preexec_fn=os.setsid,
    )
    time.sleep(2)
    assert not moto_proxy.poll(), moto_proxy.stdout.read().decode("utf-8")
    yield moto_proxy
    os.killpg(os.getpgid(moto_proxy.pid), signal.SIGTERM)


@pytest.fixture(scope="session")
def aws_credentials(moto_proxy_start):
    """Mocked AWS Credentials for moto."""
    os.environ["TEST_SERVER_MODE"] = "true"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ.pop("FIREHOSE_ACCOUNT_ARN", None)
    os.environ.pop("AWS_PROFILE", None)


@pytest.fixture(scope="session")
def mock_aws_services(aws_credentials):
    with mock_aws():
        yield


@pytest.fixture(scope="session")
def mock_log_streams(mock_aws_services):
    s3_client: S3Client = boto3.client("s3", region_name="eu-west-2")
    firehose_client: FirehoseClient = boto3.client(
        "firehose", region_name="eu-west-2"
    )
    s3_client.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    stream_class_list = [DCWidePostcodeLoggingClient]
    for cls in stream_class_list:
        firehose_client.create_delivery_stream(
            DeliveryStreamName=cls.stream_name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": "arn:aws:s3:::test-bucket",
                "BufferingHints": {
                    "IntervalInSeconds": 300,
                    "SizeInMBs": 5,
                },
                "CompressionFormat": "UNCOMPRESSED",
                "DataFormatConversionConfiguration": {"Enabled": False},
                "EncryptionConfiguration": {
                    "NoEncryptionConfig": "NoEncryption"
                },
                "Prefix": "AWSLogs/000000000000/route53querylogs/eu-west-2/",
                "ProcessingConfiguration": {
                    "Enabled": False,
                    "Processors": [],
                },
                "RoleARN": "arn:aws:iam::000000000000:role/query-log-firehose-P8V0BH6695TQ7HD",
                "S3BackupMode": "Disabled",
            },
        )


@pytest.fixture(scope="session")
def cdk(mock_aws_services):
    """
    Consumes a CloudFormation Template and creates mock resources based on it.

    Not all of CloudFormation is supported, so some parts will need to be manually created.

    """
    ssm_client = boto3.client("ssm", region_name="eu-west-2")
    s3_client = boto3.client("s3", region_name="eu-west-2")
    with open(
        Path(os.path.dirname(__file__)) / "test_stack_cfn" / "template.yaml"
    ) as f:
        template = f.read()
    parsed = yaml.safe_load(template)
    if not isinstance(parsed, dict):
        raise TypeError(
            f"Expected 'parsed' to be a dict. Instead, got {type(parsed)}\n{parsed =}\n"
        )
    assets_bucket_name = None
    lambda_zips = {}
    for name, spec in parsed["Resources"].items():
        spec["Properties"]["StageName"] = "test"
        # Bug means tags don't work at the moment, remove them
        spec["Properties"].pop("Tags", None)
        if spec["Type"] == "AWS::Lambda::Function":
            assets_bucket_name = (
                assets_bucket_name or spec["Properties"]["Code"]["S3Bucket"]
            )
            lambda_zips[name] = spec["Properties"]["Code"]["S3Key"]

    fixed_template = yaml.dump(parsed)

    # fixing moto param parsing, fails with no variable found
    ssm_client.put_parameter(
        Name=parsed["Parameters"]["BootstrapVersion"]["Default"],
        Value="1",
        Type="String",
    )

    s3_client.create_bucket(
        Bucket=assets_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    root_path = Path(__file__).parent.parent
    # At the moment we only have a single Lambda so we can
    # hard code the below. If we were to have more, we'd have to do something
    # clever like inspecting the hashed CFN asset values and matching them
    # to paths. Thankfully this is a problem for another day
    for name, key in lambda_zips.items():
        f = BytesIO()
        z = zipfile.ZipFile(f, "w")
        lambda_path = root_path / "dc_logging_aws/lambdas/ingest/handler.py"
        with lambda_path.open() as lambda_file:
            z.writestr(
                "handler.py",
                lambda_file.read(),
            )
        # It looks like layers aren't properly supported at the moment, so
        # add the files manually. Again, this is simple while we only have a
        # single function…
        client_path = root_path / "dc_logging_client"
        for path in client_path.glob("*.py"):
            z.writestr(
                str(path.name),
                path.open().read(),
            )
        z.close()
        s3_client.put_object(
            Body=f.getvalue(), Bucket=assets_bucket_name, Key=key
        )
        f.close()
    cf_mock = boto3.client("cloudformation", region_name="eu-west-2")
    cf_mock.create_stack(StackName="Test", TemplateBody=fixed_template)
    # Add anything that we need in the tests here
    test_data = {"ingest_functions": []}
    for resource in cf_mock.describe_stack_resources(StackName="Test")[
        "StackResources"
    ]:
        if resource["ResourceType"] == "AWS::Lambda::Function" and resource[
            "PhysicalResourceId"
        ].startswith("ingest-"):
            test_data["ingest_functions"].append(resource["PhysicalResourceId"])
            test_data["function_name"] = resource["PhysicalResourceId"]
    yield test_data
