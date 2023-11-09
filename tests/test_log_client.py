import json
from dataclasses import dataclass

import boto3
import pytest

from dc_logging_client import DCProduct
from dc_logging_client.log_client import (
    DCWidePostcodeLoggingClient,
)
from dc_logging_client.log_entries import BaseLogEntry


def read_log_for_logging_client():
    bucket_name = "test-bucket"
    s3_client = boto3.client("s3")
    key = s3_client.list_objects(Bucket=bucket_name)["Contents"][0]["Key"]
    s3_client.get_object(Key=key, Bucket=bucket_name)
    for line in s3_client.get_object(Key=key, Bucket=bucket_name)[
        "Body"
    ].readlines():
        yield json.loads(line)


def test_log(cdk, mock_log_streams):
    logger = DCWidePostcodeLoggingClient(function_arn=cdk["function_name"])
    logger.log(
        logger.entry_class(
            postcode="SW1A 1AA", dc_product=logger.dc_product.wcivf
        )
    )
    log = next(read_log_for_logging_client())
    # Remove the timestamp because, well, it changes
    assert "timestamp" in log
    del log["timestamp"]
    assert log == {
        "api_key": "",
        "dc_product": "WCIVF",
        "postcode": "SW1A 1AA",
        "utm_campaign": "",
        "utm_medium": "",
        "utm_source": "",
    }


def test_log_invalid_entry(cdk):
    logger = DCWidePostcodeLoggingClient(function_arn=cdk["function_name"])

    @dataclass
    class DummyLogEntry(BaseLogEntry):
        foo: str

    with pytest.raises(ValueError) as e_info:
        logger.log(DummyLogEntry(foo="SW1A 1AA"))
    assert str(e_info.value) == (
        """<class 'tests.test_log_client.test_log_invalid_entry.<locals>.DummyLogEntry'>"""
        """ isn't a valid log entry for stream 'dc-postcode-searches'"""
    )

    with pytest.raises(ValueError) as e_info:
        logger.log(
            logger.entry_class(postcode="SW1A 1AA", dc_product="new product")
        )  # type: ignore
    assert str(e_info.value) == ("""'new product' is not currently supported""")

    # Allow creating an entry from a string value of the product enum
    entry = logger.entry_class(postcode="SW1A 1AA", dc_product="WCIVF")
    assert entry.dc_product == DCProduct.wcivf.value
