
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from models.models import BaseQuery


class TotalSearchesQueryTask(Construct):
    """
    A construct that creates a Step Functions task for running Athena queries
    with configurable time period variables and automatic result extraction.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        task_name: str,
        query: BaseQuery,
        athena_lambda_function,
        period_type: str,
        result_variable_name: str,
    ) -> None:
        super().__init__(scope, construct_id)

        base_config = {
            "start_of_election_period_day": "{% $polling_day_athena %}",
            "polling_day": "{% $start_of_election_period_day_athena %}",
            "updown_api_key": "{% $updown_api_key %}",
            "end_datetime_utc": "{% $close_of_polls_utc %}",
            "end_datetime_london": "{% $close_of_polls_london %}",
        }
        period_configs = {
            "election_period": {
                **base_config,
                "start_datetime_london": "{% $start_of_election_period_london %}",
                "start_datetime_utc": "{% $start_of_election_period_utc %}",
            },
            "election_week": {
                **base_config,
                "start_datetime_london": "{% $start_of_election_week_london %}",
                "start_datetime_utc": "{% $start_of_election_week_utc %}",
            },
            "polling_day": {
                **base_config,
                "start_datetime_london": "{% $start_of_polling_day_london %}",
                "start_datetime_utc": "{% $start_of_polling_day_utc %}",
            },
        }

        if period_type not in period_configs:
            raise ValueError(
                f"Invalid period_type: {period_type}. Must be one of: {list(period_configs.keys())}"
            )

        query_context = period_configs[period_type]
        query_context["updown_api_key"] = "{% $updown_api_key %}"

        # Create the query execution task
        query_task = tasks.LambdaInvoke(
            self,
            f"{task_name} Execution",
            lambda_function=athena_lambda_function,
            payload=sfn.TaskInput.from_object(
                {
                    "QueryContext": query_context,
                    "QueryName": query.name,
                    "blocking": True,
                }
            ),
            query_language=sfn.QueryLanguage.JSONATA,
        )

        result_task = tasks.AthenaGetQueryResults(
            self,
            f"Get {task_name} Result",
            query_execution_id="{% $states.input.Payload.queryExecutionId %}",
            query_language=sfn.QueryLanguage.JSONATA,
            assign={
                result_variable_name: "{% $states.result.ResultSet.Rows[1].Data[0].VarCharValue %}"
            },
        )

        # Chain the tasks together to be an entry point
        self.task = query_task.next(result_task)
