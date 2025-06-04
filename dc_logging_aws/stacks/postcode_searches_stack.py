from pathlib import Path
from typing import List

import aws_cdk.aws_glue_alpha as glue
from aws_cdk import Duration, Fn, Stack, aws_lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from constructs.athena_named_query_from_model import AthenaNamedQueryFromModel
from constructs.lambdas.athena_query_lambda import AthenaQueryLambda
from constructs.lambdas.get_parameter_store_variables import (
    GetParameterStoreVariables,
)
from models.buckets import (
    dc_monitoring_production_logging,
    postcode_searches_results_bucket,
)
from models.databases import dc_wide_logs_db
from models.models import BaseQuery, GlueDatabase, GlueTable, S3Bucket
from models.queries import total_searches_query
from models.tables import dc_postcode_searches_table


class PostcodeSearchesStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        workgroup_name = Fn.import_value("PostcodeSearchesWorkgroupName")

        self.buckets_by_name = {}
        self.collect_buckets()

        self.databases_by_name = {}
        self.get_databases()

        self.tables_by_name = {}
        self.collect_tables()

        self.make_queries(workgroup_name)

        self.run_athena_query_lambda = AthenaQueryLambda(
            self,
            resource_id="RunAthenaQueryLambda",
            workgroup_name=workgroup_name,
            database_name=dc_wide_logs_db.database_name,
        )

        self.get_parameter_store_variables_lambda = GetParameterStoreVariables(
            self,
            resource_id="GetParameterStoreVariables",
        ).lambda_function

        assign_input_to_variables_task = self.assign_input_variables_task()
        get_parameter_store_variables_task = (
            self.get_parameter_store_variables_task()
        )
        calculate_reporting_period_task = self.calculate_reporting_period_task()
        election_period_total_task = self.election_period_total_task()

        get_election_period_total = tasks.AthenaGetQueryResults(
            self,
            "Get election period total",
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
            query_language=sfn.QueryLanguage.JSONATA,
            assign={
                "election_period_total": "{% $states.result.ResultSet.Rows[1].Data[0].VarCharValue %}"
            },
        )

        definition = (
            assign_input_to_variables_task.next(
                get_parameter_store_variables_task
            )
            .next(calculate_reporting_period_task)
            .next(election_period_total_task)
            .next(get_election_period_total)
        )

        self.step_function = sfn.StateMachine(
            self,
            "PostcodeSearchesReporting",
            state_machine_name="PostcodeSearchesReporting",
            definition=definition,
            timeout=Duration.minutes(10),
        )

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

    def assign_input_variables_task(self):
        return sfn.Pass(
            self,
            "Assign Input to Variables",
            query_language=sfn.QueryLanguage.JSONATA,
            assign={
                "polling_day": "{% $states.input.polling_day %}",
            },
        )

    def get_parameter_store_variables_task(self):
        return tasks.LambdaInvoke(
            self,
            "Get Parameter Store Variables",
            lambda_function=self.get_parameter_store_variables_lambda,
            query_language=sfn.QueryLanguage.JSONATA,
            payload=sfn.TaskInput.from_object(
                {"parameter_names": ["UPDOWN_API_KEY"]}
            ),
            assign={
                "updown_api_key": "{% $states.result.Payload.UPDOWN_API_KEY %}"
            },
        )

    def calculate_reporting_period_task(self):
        # Step Functions task to assign variables for reporting periods
        return tasks.LambdaInvoke(
            self,
            "Calculate Reporting Period Dates",
            lambda_function=aws_lambda.Function(
                self,
                "calculate_reporting_period",
                function_name="calculate_reporting_period",
                runtime=aws_lambda.Runtime.PYTHON_3_12,
                code=aws_lambda.Code.from_asset(
                    str(
                        Path(__file__).resolve().parent.parent
                        / "lambdas"
                        / "calculate_reporting_period_dates"
                    )
                ),
                handler="handler.handler",
            ),
            payload=sfn.TaskInput.from_object(
                {"polling_day": "{% $polling_day %}"}
            ),
            query_language=sfn.QueryLanguage.JSONATA,
            assign={
                "polling_day_athena": "{% $states.result.Payload.polling_day_athena %}",
                "start_of_election_period_day_athena": "{% $states.result.Payload.start_of_election_period_day_athena %}",
                "close_of_polls_utc": "{% $states.result.Payload.close_of_polls_utc %}",
                "close_of_polls_london": "{% $states.result.Payload.close_of_polls_london %}",
                "start_of_election_period_utc": "{% $states.result.Payload.start_of_election_period_utc %}",
                "start_of_election_period_london": "{% $states.result.Payload.start_of_election_period_london %}",
                "start_of_election_week_utc": "{% $states.result.Payload.start_of_election_week_utc %}",
                "start_of_election_week_london": "{% $states.result.Payload.start_of_election_week_london %}",
                "start_of_polling_day_utc": "{% $states.result.Payload.start_of_polling_day_utc %}",
                "start_of_polling_day_london": "{% $states.result.Payload.start_of_polling_day_london %}",
            },
        )

    def election_period_total_task(self):
        return tasks.LambdaInvoke(
            self,
            "Election Period Total Query",
            lambda_function=self.run_athena_query_lambda.lambda_function,
            payload=sfn.TaskInput.from_object(
                {
                    "QueryContext": {
                        "start_of_election_period_day": "{% $polling_day_athena %}",
                        "polling_day": "{% $start_of_election_period_day_athena %}",
                        "updown_api_key": "{% $updown_api_key %}",
                        "start_datetime_utc": "{% $start_of_election_period_utc %}",
                        "end_datetime_utc": "{% $close_of_polls_utc %}",
                        "start_datetime_london": "{% $start_of_election_period_london %}",
                        "end_datetime_london": "{% $close_of_polls_london %}",
                    },
                    "QueryName": total_searches_query.name,
                    "blocking": True,
                },
            ),
            query_language=sfn.QueryLanguage.JSONATA,
        )

    def queries(self) -> List[BaseQuery]:
        return [total_searches_query]

    def make_queries(self, workgroup_name):
        for query in self.queries():
            AthenaNamedQueryFromModel(
                self,
                resource_id=f"{query.name} named query",
                query=query,
                workgroup_name=workgroup_name,
            )
