from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_s3 as s3
from constructs import Construct
from models.buckets import (
    postcode_searches_results_bucket,
    postcode_searches_results_dev_bucket,
)


class BaseReportingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        results_bucket_model = postcode_searches_results_dev_bucket

        if scope.node.try_get_context("dc-environment") == "production":
            results_bucket_model = postcode_searches_results_bucket

        self.results_bucket = s3.Bucket.from_bucket_name(
            self,
            results_bucket_model.bucket_name,
            results_bucket_model.bucket_name,
        )

        workgroup_name = "postcode-searches-workgroup"
        workgroup_output_key = "postcode-searches-athena-results"

        self.workgroup = self.make_workgroup(
            workgroup_name, workgroup_output_key
        )

        CfnOutput(
            self,
            "WorkgroupNameOutput",
            value=self.workgroup.name,
            export_name="PostcodeSearchesWorkgroupName",
        )

    def make_workgroup(
        self, workgroup_name, workgroup_output_key
    ) -> athena.CfnWorkGroup:
        return athena.CfnWorkGroup(
            self,
            f"{workgroup_name}-id",
            name=workgroup_name,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=self.results_bucket.s3_url_for_object(
                        key=workgroup_output_key
                    )
                ),
            ),
        )
