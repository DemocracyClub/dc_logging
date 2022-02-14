import pytest
from mypy_boto3_s3 import S3Client

from dc_logging_client.log_client import DummyLoggingClient, DCWidePostcodeLoggingClient
from dc_logging_client.log_entries import PostcodeLogEntry


def test_log_client_init_errors():
    with pytest.raises(ValueError) as e_info:
        DummyLoggingClient(fake=False)
    assert str(e_info.value) == """`assume_role_arn` when not faking"""

    assert DummyLoggingClient(fake=True)


def test_log_client_with_env_var(log_stream_arn_env, dc_wide_postcode_log_stream):
    assert DCWidePostcodeLoggingClient(fake=False)
    logger = DCWidePostcodeLoggingClient()
    entry = logger.entry_class(dc_product=logger.dc_product.wcivf, postcode="SW1A 1AA")
    logger.log(entry)


def test_log_client_init_working(firehose, sts, example_arn):
    assert DummyLoggingClient(
        fake=False,
        assume_role_arn=example_arn,
    )


def _read_log(s3_client, bucket_name):
    key = s3_client.list_objects(Bucket=bucket_name)["Contents"][0]["Key"]
    s3_client.get_object(Key=key, Bucket=bucket_name)
    return s3_client.get_object(Key=key, Bucket=bucket_name)["Body"].read()


def test_log(dummy_log_stream: S3Client, example_arn):
    logger = DummyLoggingClient(assume_role_arn=example_arn)
    logger.log(logger.entry_class(text="test", dc_product=logger.dc_product.wcivf))
    log = _read_log(dummy_log_stream, "firehose-test")
    assert log == b"""{"dc_product": "WCIVF", "text": "test"}\n"""


def test_log_invalid_entry(dummy_log_stream, example_arn):
    logger = DummyLoggingClient(assume_role_arn=example_arn)
    with pytest.raises(ValueError) as e_info:
        logger.log(
            PostcodeLogEntry(postcode="SW1A 1AA", dc_product=logger.dc_product.wcivf)
        )
    assert str(e_info.value) == (
        """<class 'dc_logging_client.log_entries.PostcodeLogEntry'>"""
        """ isn't a valid log entry for stream 'dummy'"""
    )

    with pytest.raises(ValueError) as e_info:
        logger.log(logger.entry_class(text="test", dc_product="new product"))  # type: ignore
    assert str(e_info.value) == ("""'new product' is not currently supported""")


def test_log_batch(dummy_log_stream, example_arn):
    logger = DummyLoggingClient(assume_role_arn=example_arn)

    enteries = [
        logger.entry_class(text="test1", dc_product=logger.dc_product.wcivf),
        logger.entry_class(text="test2", dc_product=logger.dc_product.wdiv),
        logger.entry_class(text="test3", dc_product=logger.dc_product.aggregator_api),
    ]

    logger.log_batch(enteries)
    log = _read_log(dummy_log_stream, "firehose-test")
    assert (
        log
        == b"""{"dc_product": "WCIVF", "text": "test1"}{"dc_product": "WDIV", "text": "test2"}{"dc_product": "AGGREGATOR_API", "text": "test3"}"""
    )
