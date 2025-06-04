from dataclasses import dataclass

from aws_cdk import aws_glue_alpha as glue


@dataclass
class S3Bucket:
    bucket_name: str


@dataclass
class GlueDatabase:
    database_name: str


@dataclass
class BaseQuery:
    name: str
    creation_context: dict
    query_context: dict
    database: GlueDatabase


@dataclass
class GlueTable:
    table_name: str
    description: str
    bucket: S3Bucket
    s3_prefix: str
    database: GlueDatabase
    data_format: type(glue.DataFormat)
    columns: dict[str : glue.Schema]
    partition_keys: list[glue.Column] = None
    depends_on: list = None
    populated_with: BaseQuery = None
