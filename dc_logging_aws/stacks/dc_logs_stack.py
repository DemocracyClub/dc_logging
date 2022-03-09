import os
import sys
import typing
from datetime import datetime
from typing import Type

import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kinesisfirehose as firehose
import aws_cdk.aws_kinesisfirehose_destinations as firehose_destinations
import aws_cdk.aws_s3 as s3
import boto3
from aws_cdk.core import Stack, Duration
from constructs import Construct

sys.path.append("..")

from dc_logging_client.log_client import BaseLoggingClient


class DCLogsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.create_iam_role()
        self.database = self.get_database()
        self.bucket = self.get_bucket()
        self.tables = self.create_tables_and_streams()

    def create_iam_role(self):
        policy = iam.Policy(
            self,
            "cross-account-put-record",
            policy_name="cross-account-put-record",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "firehose:PutRecord",
                        "firehose:PutRecordBatch",
                    ],
                    resources=["*"],
                )
            ],
        )
        # Ideally we'd get this from SSM directly in the cloudofrmation,
        # however there is a CDK python bug reported here:
        # https://github.com/aws/aws-cdk/issues/924
        # Because of this, we have to use boto to get the list of accounts at
        # deploy time
        client = boto3.client("ssm", region_name="eu-west-2")
        allowed_accounts = client.get_parameter(Name="assume_role_aws_accounts")[
            "Parameter"
        ]["Value"].split(",")
        for account in allowed_accounts:
            role = iam.Role(
                self,
                f"put-record-from-{account}",
                assumed_by=iam.AccountPrincipal(account),
                role_name=f"put-record-from-{account}",
                max_session_duration=Duration.hours(12),
            )
            role.attach_inline_policy(policy)

    def get_database(self):
        return glue.Database(
            self,
            "DCLogs",
            database_name="dc-wide-logs",
        )

    def get_bucket(self):
        return s3.Bucket.from_bucket_name(
            self,
            "DestinationLogsBucket",
            bucket_name=os.environ.get("LOGS_BUCKET_NAME"),
        )

    def create_tables_and_streams(self):
        from dc_logging_client import DCWidePostcodeLoggingClient

        stream_class_list = [DCWidePostcodeLoggingClient]
        tables = []
        streams = []
        for cls in stream_class_list:
            tables.append(self.create_table_from_stream_class(cls))
            streams.append(self.create_stream(cls))
        return tables

    def _field_type_to_glue_type(self, field_type):
        # Special case Union types
        type_list = list(typing.get_args(field_type))
        if type_list:
            # This is a Union type
            field_type = type_list[0]

        types = {
            str: glue.Schema.STRING,
            int: glue.Schema.INTEGER,
            datetime: glue.Schema.DATE,
        }
        return types.get(field_type, glue.Schema.STRING)

    def create_table_from_stream_class(self, cls: Type[BaseLoggingClient]):
        columns = []
        entry_fields = cls.entry_class.__dataclass_fields__
        for field_name, field in entry_fields.items():
            field_type = self._field_type_to_glue_type(field.type)
            column = glue.Column(
                name=field_name,
                type=field_type,
            )
            columns.append(column)

        return glue.Table(
            self,
            id=f"{cls.stream_name}-table",
            database=self.database,
            table_name=f"{cls.stream_name}-table",
            columns=columns,
            bucket=self.bucket,
            s3_prefix=cls.stream_name,
            data_format=glue.DataFormat(
                input_format=glue.InputFormat(
                    "org.apache.hadoop.mapred.TextInputFormat"
                ),
                output_format=glue.OutputFormat(
                    "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
                ),
                serialization_library=glue.SerializationLibrary(
                    "org.openx.data.jsonserde.JsonSerDe"
                ),
            ),
        )

    def create_stream(self, cls):
        firehose.DeliveryStream(
            self,
            cls.stream_name,
            destinations=[
                firehose_destinations.S3Bucket(
                    self.bucket, data_output_prefix=f"{cls.stream_name}/"
                )
            ],
            delivery_stream_name=cls.stream_name,
        )
