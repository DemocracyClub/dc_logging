from pathlib import Path
from typing import Optional

from aws_cdk import (
    Duration,
    aws_lambda,
)
from aws_cdk import (
    aws_iam as iam,
)
from constructs import Construct


class AthenaQueryLambda(Construct):
    def __init__(
        self,
        scope: Construct,
        resource_id: str,
        workgroup_name: str,
        database_name: str,
        timeout: Duration = Duration.seconds(300),
        environment: Optional[dict] = None,
    ):
        super().__init__(scope, resource_id)

        code_path = str(
            Path(__file__).resolve().parent.parent.parent
            / "lambdas"
            / "run_athena_query_and_report_status"
        )
        lambda_env = {
            "WORKGROUP_NAME": workgroup_name,
            "DATABASE_NAME": database_name,
            **(environment or {}),
        }
        self.lambda_function = aws_lambda.Function(
            self,
            "run_athena_query_lambda",
            function_name="run_athena_query_lambda",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            code=aws_lambda.Code.from_asset(code_path),
            handler="handler.handler",
            timeout=timeout,
            environment=lambda_env,
        )

        # Athena permissions
        self.lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetNamedQuery",
                    "athena:ListNamedQueries",
                ],
                resources=["*"],
            )
        )
        # glue permissions
        self.lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable"],
                resources=["*"],
            )
        )
        # s3 permissions
        self.lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload",
                    "s3:CreateBucket",
                    "s3:PutObject",
                ],
                resources=["*"],
            )
        )
