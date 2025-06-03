from typing import List

import aws_cdk.aws_glue_alpha as glue
from aws_cdk import Stack
from aws_cdk import aws_s3 as s3
from constructs import Construct
from models.buckets import (
    dc_monitoring_production_logging,
    postcode_searches_results_bucket,
)
from models.databases import dc_wide_logs_db
from models.models import GlueDatabase, GlueTable, S3Bucket
from models.tables import dc_postcode_searches_table


class PostcodeSearchesStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.buckets_by_name = {}
        self.collect_buckets()

        self.databases_by_name = {}
        self.get_databases()

        self.tables_by_name = {}
        self.collect_tables()

    def s3_buckets(self) -> List[S3Bucket]:
        return [
            postcode_searches_results_bucket,
            dc_monitoring_production_logging,
        ]

    def collect_buckets(self):
        for bucket in self.s3_buckets():
            self.buckets_by_name[
                bucket.bucket_name
            ] = s3.Bucket.from_bucket_name(
                self,
                f"{bucket.bucket_name}_bucket",
                bucket.bucket_name,
            )

    def existing_tables(self) -> List[GlueTable]:
        return [dc_postcode_searches_table]

    def managed_tables(self) -> List[GlueTable]:
        return []

    def collect_tables(self):
        for table in self.managed_tables():
            self.tables_by_name[table.table_name] = self.make_table(table)

        for table in self.existing_tables():
            self.tables_by_name[table.table_name] = self.get_table(table)

    def get_table(self, table: GlueTable) -> glue.ITable:
        table_arn = self.format_arn(
            service="glue",
            resource="table",
            resource_name=table.table_name,
        )
        return glue.S3Table.from_table_arn(
            self, f"{table.table_name}_table", table_arn
        )

    def make_table(self, table) -> glue.S3Table:
        columns = []
        for column_name, column_type in table.columns.items():
            columns.append(
                glue.Column(name=column_name, type=column_type, comment="")
            )

        return glue.S3Table(
            self,
            table.table_name,
            table_name=table.table_name,
            description=table.description,
            bucket=self.buckets_by_name[table.bucket.bucket_name],
            s3_prefix=table.s3_prefix,
            database=self.databases_by_name[table.database.database_name],
            columns=columns,
            data_format=table.data_format,
            partition_keys=table.partition_keys,
        )

    def databases(self) -> List[GlueDatabase]:
        return [dc_wide_logs_db]

    def get_databases(self):
        for database in self.databases():
            self.databases_by_name[database.database_name] = self.get_database(
                database
            )

    def get_database(self, db: GlueDatabase) -> glue.IDatabase:
        db_arn = self.format_arn(
            service="glue",
            resource="database",
            resource_name=db.database_name,
        )
        return glue.Database.from_database_arn(
            self, f"{db.database_name}_db", db_arn
        )
