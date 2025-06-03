"""
Define the S3 buckets used anywhere in the stack
"""

from models.models import S3Bucket

postcode_searches_results_bucket = S3Bucket(
    bucket_name="dc-monitoring-query-results"
)

postcode_searches_results_dev_bucket = S3Bucket(
    bucket_name="dc-monitoring-dev-query-results"
)

dc_monitoring_production_logging = S3Bucket(
    bucket_name="dc-monitoring-production-logging"
)
