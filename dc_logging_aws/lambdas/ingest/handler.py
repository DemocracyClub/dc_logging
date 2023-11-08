import os

import boto3
import log_entries

firehose_client = boto3.client("firehose", region_name="eu-west-2")
stream_name = os.environ["STREAM_NAME"]
entry_class_name = os.environ["ENTRY_CLASS"]


def handler(event, context):
    entry_class = getattr(log_entries, entry_class_name)
    log_entry: log_entries.BaseLogEntry = entry_class(**event)
    firehose_client.put_record(
        DeliveryStreamName=stream_name,
        Record={"Data": log_entry.as_log_line()},
    )
