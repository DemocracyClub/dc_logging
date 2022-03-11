import os

import boto3
import pytest
from moto import mock_sts, mock_firehose, mock_s3
from mypy_boto3_s3 import S3Client

from dc_logging_client import DCWidePostcodeLoggingClient
from dc_logging_client.log_client import DummyLoggingClient


@pytest.fixture(scope="function")
def example_arn(aws_credentials):
    return "arn:aws:iam::012345678910:role/test-role"


@pytest.fixture(scope="function")
def log_stream_arn_env(example_arn):
    os.environ["FIREHOSE_ACCOUNT_ARN"] = example_arn


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ.pop("FIREHOSE_ACCOUNT_ARN", None)
    os.environ.pop("AWS_PROFILE", None)


@pytest.fixture(scope="function")
def sts(aws_credentials):
    with mock_sts():
        yield


@pytest.fixture(scope="function")
def firehose(aws_credentials):
    with mock_firehose():
        yield


def _base_mocked_log_stream(logging_client):
    with mock_s3():
        s3_client: S3Client = boto3.client("s3")
        s3_client.create_bucket(Bucket="firehose-test")
        client = boto3.client("firehose", region_name="eu-west-2")
        client.create_delivery_stream(
            DeliveryStreamName=logging_client.stream_name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": "arn:aws:s3:::firehose-test",
                "BufferingHints": {"IntervalInSeconds": 300, "SizeInMBs": 5},
                "CompressionFormat": "UNCOMPRESSED",
                "DataFormatConversionConfiguration": {"Enabled": False},
                "EncryptionConfiguration": {"NoEncryptionConfig": "NoEncryption"},
                "Prefix": "AWSLogs/000000000000/route53querylogs/eu-west-2/",
                "ProcessingConfiguration": {"Enabled": False, "Processors": []},
                "RoleARN": "arn:aws:iam::000000000000:role/query-log-firehose-P8V0BH6695TQ7HD",
                "S3BackupMode": "Disabled",
            },
        )
        yield s3_client


# TODO: DRY this up: can we pass in the logging client somehow?
@pytest.fixture(scope="function")
def dummy_log_stream(sts, firehose):
    with mock_s3():
        s3_client: S3Client = boto3.client("s3")
        s3_client.create_bucket(Bucket="firehose-test")
        client = boto3.client("firehose", region_name="eu-west-2")
        client.create_delivery_stream(
            DeliveryStreamName=DummyLoggingClient.stream_name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": "arn:aws:s3:::firehose-test",
                "BufferingHints": {"IntervalInSeconds": 300, "SizeInMBs": 5},
                "CompressionFormat": "UNCOMPRESSED",
                "DataFormatConversionConfiguration": {"Enabled": False},
                "EncryptionConfiguration": {"NoEncryptionConfig": "NoEncryption"},
                "Prefix": "AWSLogs/000000000000/route53querylogs/eu-west-2/",
                "ProcessingConfiguration": {"Enabled": False, "Processors": []},
                "RoleARN": "arn:aws:iam::000000000000:role/query-log-firehose-P8V0BH6695TQ7HD",
                "S3BackupMode": "Disabled",
            },
        )
        yield s3_client


@pytest.fixture(scope="function")
def dc_wide_postcode_log_stream(sts, firehose):
    with mock_s3():
        s3_client: S3Client = boto3.client("s3")
        s3_client.create_bucket(Bucket="firehose-test")
        client = boto3.client("firehose", region_name="eu-west-2")
        client.create_delivery_stream(
            DeliveryStreamName=DCWidePostcodeLoggingClient.stream_name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": "arn:aws:s3:::firehose-test",
                "BufferingHints": {"IntervalInSeconds": 300, "SizeInMBs": 5},
                "CompressionFormat": "UNCOMPRESSED",
                "DataFormatConversionConfiguration": {"Enabled": False},
                "EncryptionConfiguration": {"NoEncryptionConfig": "NoEncryption"},
                "Prefix": "AWSLogs/000000000000/route53querylogs/eu-west-2/",
                "ProcessingConfiguration": {"Enabled": False, "Processors": []},
                "RoleARN": "arn:aws:iam::000000000000:role/query-log-firehose-P8V0BH6695TQ7HD",
                "S3BackupMode": "Disabled",
            },
        )
        yield s3_client
