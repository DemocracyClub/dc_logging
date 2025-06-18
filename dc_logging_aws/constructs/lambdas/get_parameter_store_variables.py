from pathlib import Path

from aws_cdk import Duration, Stack, aws_iam, aws_lambda
from constructs import Construct


class GetParameterStoreVariables(Construct):
    def __init__(self, scope: Construct, resource_id: str):
        super().__init__(scope, resource_id)

        code_path = str(
            Path(__file__).resolve().parent.parent.parent
            / "lambdas"
            / "get_parameter_store_variables"
        )

        self.lambda_function = aws_lambda.Function(
            self,
            "get_parameter_store_variables",
            function_name="get_parameter_store_variables",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            code=aws_lambda.Code.from_asset(code_path),
            handler="handler.handler",
            timeout=Duration.seconds(300),
        )

        self.lambda_function.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["ssm:GetParameter", "ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:{Stack.of(self).region}:{Stack.of(self).account}:parameter/*"
                ],
            )
        )
