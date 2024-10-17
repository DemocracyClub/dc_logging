import os
import sys
import typing
from datetime import datetime
from typing import Type

import aws_cdk.aws_glue_alpha as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kinesisfirehose_alpha as firehose
import aws_cdk.aws_kinesisfirehose_destinations_alpha as firehose_destinations
import aws_cdk.aws_lambda as aws_lambda
import aws_cdk.aws_lambda_python_alpha as lambda_python
import aws_cdk.aws_s3 as s3
import boto3
from aws_cdk import Duration, Stack
from constructs import Construct

sys.path.append("..")

from dc_logging_client.log_client import BaseLoggingClient  # noqa


class DCLogsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        org_client = boto3.client("organizations", region_name=self.region)
        self.org_id = org_client.describe_organization()["Organization"]["Id"]
        self.dc_environment = self.node.try_get_context("dc-environment")
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
        allowed_accounts = client.get_parameter(
            Name="assume_role_aws_accounts"
        )["Parameter"]["Value"].split(",")
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
            self.create_lambda_function(cls)
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
            datetime: glue.Schema.TIMESTAMP,
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

        table_name = f"{cls.stream_name.replace('-', '_')}_table"

        table = glue.Table(
            self,
            id=table_name,
            database=self.database,
            table_name=table_name,
            columns=columns,
            bucket=self.bucket,
            s3_prefix=cls.stream_name,
            partition_keys=[
                glue.Column(name="day", type=glue.Schema.STRING),
                glue.Column(name="hour", type=glue.Schema.INTEGER),
            ],
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
            storage_parameters=[
                glue.StorageParameter.compression_type(
                    glue.CompressionType.GZIP
                )
            ],
        )

        # Projection isn't supported directly by the CDK, so we have to set
        # overrides of the CloudFormation template. We also need to escape the
        # dots in the keys, as otherwise the CDK will try to interpret them as
        # nested properties.
        cfn_table = table.node.default_child
        overrides = [
            ("projection.enabled", "true"),
            ("projection.day.type", "date"),
            ("projection.day.format", "yyyy/MM/dd"),
            ("projection.day.range", "2017/05/01,NOW"),
            ("projection.day.interval", "1"),
            ("projection.day.interval.unit", "DAYS"),
            ("projection.hour.type", "integer"),
            ("projection.hour.range", "0,23"),
            ("projection.hour.digits", "2"),
            (
                "storage.location.template",
                f"s3://{self.bucket.bucket_name}/{cls.stream_name}/${{day}}/${{hour}}",
            ),
            ("projection.enabled", "true"),
        ]
        for key, value in overrides:
            key = key.replace(".", "\.")
            cfn_table.add_override(
                f"Properties.TableInput.Parameters.{key}", value
            )

        return table

    def create_stream(self, cls):
        firehose.DeliveryStream(
            self,
            cls.stream_name,
            destinations=[
                firehose_destinations.S3Bucket(
                    self.bucket,
                    data_output_prefix=f"{cls.stream_name}/",
                    compression=firehose_destinations.Compression.GZIP,
                )
            ],
            delivery_stream_name=cls.stream_name,
        )

    def create_lambda_function(self, cls):
        client_layer = lambda_python.PythonLayerVersion(
            self,
            "logging_client_layer",
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_12],
            entry="./dc_logging_client",
        )

        stream_ingest_lambda = lambda_python.PythonFunction(
            self,
            f"ingest-{cls.stream_name}",
            function_name=f"ingest-{cls.stream_name}-{self.dc_environment}",
            entry="./dc_logging_aws/lambdas/ingest",
            index="handler.py",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.minutes(2),
            environment={
                "STREAM_NAME": cls.stream_name,
                "ENTRY_CLASS": cls.entry_class.__name__,
            },
            layers=[client_layer],
        )

        # TODO: Lambda doesn't currently support aws:PrincipalOrgPaths
        # meaning we can't limit invocation to account paths (e.g only dev accounts).
        # This isn't ideal as in theory a dev account could log to prod, or (worse)
        # prod to dev. When Lambda supports aws:PrincipalOrgPaths we should change
        # the principal to be:
        # ```
        # iam.PrincipalWithConditions(
        #     iam.OrganizationPrincipal("[org id]"),
        #     conditions={
        #         "ForAnyValue:StringLike": {
        #             "aws:PrincipalOrgPaths": [
        #                 "[path]"
        #             ]
        #         },
        #     },
        # )
        # ```

        stream_ingest_lambda.add_permission(
            f"cross-org-invoke-{cls.stream_name}",
            principal=iam.OrganizationPrincipal(self.org_id),
            action="lambda:InvokeFunction",
        )

        stream_ingest_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["firehose:PutRecord"],
                resources=[
                    f"arn:aws:firehose:*:*:deliverystream/{cls.stream_name}"
                ],
                effect=iam.Effect.ALLOW,
            )
        )
