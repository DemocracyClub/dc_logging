import abc
import logging
import os
from typing import Optional

import boto3
from mypy_boto3_firehose import FirehoseClient

from .log_entries import (
    BaseLogEntry,
    PostcodeLogEntry,
    DCProduct,
    DummyLogEntry,
)

__all__ = [
    "DCWidePostcodeLoggingClient",
    "DCProduct",
]

logger = logging.getLogger(__name__)


class FirehoseClientWrapper:
    def __init__(self, assume_role_arn: str, region: str = "eu-west-2"):
        self.region = region
        self.assume_role_arn = assume_role_arn
        self.client: Optional[FirehoseClient] = None
        self.connect()

    def connect(self):
        self.client = None
        sts_default_provider_chain = boto3.client("sts", region_name=self.region)
        role_to_assume_arn = self.assume_role_arn
        role_session_name = "test_session"
        response = sts_default_provider_chain.assume_role(
            RoleArn=role_to_assume_arn, RoleSessionName=role_session_name
        )
        creds = response["Credentials"]

        self.client: FirehoseClient = boto3.client(
            "firehose",
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=self.region,
        )

    def _func_with_reconnect(self, method, *args, **kwargs):
        """
        wraps `func` but will re-attempt connection if it's
        closed

        :param method: the Firehose client method
        :param args:
        :param kwargs:
        :return:
        """
        try:
            return method(*args, **kwargs)
        except ConnectionError:
            self.connect()
            return method(*args, **kwargs)

    def put_record(self, *args, **kwargs):
        return self._func_with_reconnect(self.client.put_record, *args, **kwargs)

    def put_record_batch(self, *args, **kwargs):
        return self._func_with_reconnect(self.client.put_record_batch, *args, **kwargs)


class BaseLoggingClient(abc.ABC):
    """
    Base logger client for submitting BaseLogEntry classes to a BaseLogStream
    """

    stream_name = None
    entry_class = None
    dc_product = DCProduct

    def __init__(
        self,
        fake: bool = False,
        assume_role_arn: str = None,
    ):
        """
        :param fake: If True, no data is actually logged. DEBUG entries
                     are sent to the local `logger` client.
        :param assume_role_arn: The ARN of the role to assume
        """

        self.fake = fake
        self.assume_role_arn = self.get_log_stream_arn(assume_role_arn)
        if not fake:
            if not self.assume_role_arn:
                raise ValueError("`assume_role_arn` when not faking")
            self.firehose = FirehoseClientWrapper(self.assume_role_arn)

    def get_log_stream_arn(self, arn_arg):
        if arn_arg:
            return arn_arg
        return os.environ.get("FIREHOSE_ACCOUNT_ARN")

    def log(self, data: BaseLogEntry):
        if not isinstance(data, self.entry_class):
            raise ValueError(
                f"{type(data)} isn't a valid log entry for stream '{self.stream_name}'"
            )
        logging.debug(f"{self.stream_name}\t{data.as_log_line()}")
        if not self.fake:
            self.firehose.put_record(
                DeliveryStreamName=self.stream_name,
                Record={"Data": data.as_log_line()},
            )

    def log_batch(self, batch):
        logging.debug(f"{self.stream_name}\tBATCH: {len(batch)}")
        if not self.fake:
            self.firehose.put_record_batch(
                DeliveryStreamName=self.stream_name,
                Records=[{"Data": data.as_log_line(newline=False)} for data in batch],
            )


class DummyLoggingClient(BaseLoggingClient):
    stream_name = "dummy"
    entry_class = DummyLogEntry


class DCWidePostcodeLoggingClient(BaseLoggingClient):
    stream_name = "dc-postcode-searches"
    entry_class = PostcodeLogEntry
