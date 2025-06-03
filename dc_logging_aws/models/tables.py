import aws_cdk.aws_glue_alpha as glue
from models.buckets import (
    dc_monitoring_production_logging,
)
from models.databases import dc_wide_logs_db
from models.models import GlueTable

dc_postcode_searches_table = GlueTable(
    table_name="dc_postcode_searches_table",
    description="All postcode searches from all services.",
    bucket=dc_monitoring_production_logging,
    s3_prefix="dc-postcode-searches",
    database=dc_wide_logs_db,
    data_format=glue.DataFormat.CSV,
    columns={
        "dc_product": glue.Schema.STRING,
        "utm_source": glue.Schema.STRING,
        "utm_campaign": glue.Schema.STRING,
        "utm_medium": glue.Schema.STRING,
        "postcode": glue.Schema.STRING,
        "timestamp": glue.Schema.TIMESTAMP,
        "api_key": glue.Schema.STRING,
        "calls_devs_dc_api": glue.Schema.STRING,
    },
    partition_keys=[
        glue.Column(
            name="day",
            type=glue.Schema.STRING,
        ),
        glue.Column(
            name="hour",
            type=glue.Schema.INTEGER,
        ),
    ],
)
