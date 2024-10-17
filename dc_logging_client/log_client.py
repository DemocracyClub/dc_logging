import abc
import logging
import os

import boto3

from .log_entries import (
    BaseLogEntry,
    DCProduct,
    PostcodeLogEntry,
)

__all__ = [
    "DCWidePostcodeLoggingClient",
    "DCProduct",
]

logger = logging.getLogger(__name__)


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
        function_arn: str = None,
        region: str = "eu-west-2",
    ):
        """
        :param fake: If True, no data is actually logged. DEBUG entries
                     are sent to the local `logger` client.
        :param function_arn: The ARN of the Lambda function to submit records to
        """
        self.fake = fake
        self.function_arn = self.get_function_arn(function_arn)
        self.region = region
        if not fake:
            if not self.function_arn:
                raise ValueError("`function_arn` when not faking")
            self.client = boto3.client("lambda", region_name=self.region)

    def get_function_arn(self, function_arn):
        if function_arn:
            return function_arn
        return os.environ.get("LOGGER_FUNCTION_ARN")

    def log(self, data: BaseLogEntry):
        if not isinstance(data, self.entry_class):
            raise ValueError(
                f"{type(data)} isn't a valid log entry for stream '{self.stream_name}'"
            )
        logger.debug(f"{self.stream_name}\t{data.as_log_line()}")
        if not self.fake:
            response = self.client.invoke(
                FunctionName=self.function_arn,
                InvocationType="Event",
                Payload=data.as_log_line(),
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] != 202:
                logger.warning(
                    f"Failed to log `{data.as_log_line()}`. Got `{response}`"
                )
            if response.get("FunctionError"):
                logger.warning(
                    f"Failed to log `{data.as_log_line()}`. Got `{response["Payload"].read().decode('utf-8')}`"
                )


class DCWidePostcodeLoggingClient(BaseLoggingClient):
    stream_name = "dc-postcode-searches"
    entry_class = PostcodeLogEntry
