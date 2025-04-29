import argparse
import csv
import os
import time
from pathlib import Path

import boto3

# Get the S3 bucket name from environment variable
RESULTS_BUCKET = os.environ.get(
    "ATHENA_RESULTS_BUCKET", "dc-monitoring-query-results"
)


def get_query_execution(client, query_execution_id):
    return client.get_query_execution(QueryExecutionId=query_execution_id)


def wait_for_query_to_complete(client, query_execution_id):
    while True:
        query_execution = get_query_execution(client, query_execution_id)
        state = query_execution["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return state
        time.sleep(5)


def save_query_results(client, query_execution_id, output_location):
    output_location.parent.mkdir(parents=True, exist_ok=True)
    with open(output_location, "w", newline="") as out_file:
        page_number = 1
        print(
            f"Fetching page {page_number} of results for {output_location.name}"
        )
        csv_writer = csv.writer(out_file, delimiter="\t")

        # Get the first page of results
        response = client.get_query_results(QueryExecutionId=query_execution_id)

        # Write the header
        header = [
            col["Label"]
            for col in response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        ]
        csv_writer.writerow(header)

        # Write the data from the first page
        for row in response["ResultSet"]["Rows"][1:]:  # Skip the header row
            csv_writer.writerow(
                [col.get("VarCharValue", "") for col in row["Data"]]
            )

        # Continue fetching and writing data while there's a NextToken
        while "NextToken" in response:
            page_number += 1
            print(
                f"Fetching page {page_number} of results for {output_location.name}"
            )
            response = client.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=response["NextToken"],
            )
            for row in response["ResultSet"]["Rows"]:
                csv_writer.writerow(
                    [col.get("VarCharValue", "") for col in row["Data"]]
                )

    print(f"Results saved to: {output_location}")


def run_election_queries(election_date, profile):
    start_time = time.time()
    session = boto3.Session(profile_name=profile)
    athena_client = session.client("athena")

    base_dir = Path("dc_logging_aws/named_queries")
    results_dir = base_dir / "results" / election_date

    query_types = {
        "count": results_dir / "counts",
        "postcode-searches-by-product": results_dir / "product_searches",
        "postcode-searches-by-local-authority": results_dir
        / "local_authority_searches",
        "postcode-searches-by-constituency": results_dir
        / "constituency_searches",
        "timeseries-by-product": results_dir / "time_series_by_product",
    }

    named_queries = athena_client.list_named_queries()

    print(f"Running queries that start with {election_date}")
    query_attempt_count = 0
    query_success_count = 0
    query_failure_count = 0
    execution_ids = {}
    for query_id in named_queries["NamedQueryIds"]:
        query = athena_client.get_named_query(NamedQueryId=query_id)
        query_name = query["NamedQuery"]["Name"]

        if query_name.startswith(election_date):
            print(f"Starting query: {query_name}")

            response = athena_client.start_query_execution(
                QueryString=query["NamedQuery"]["QueryString"],
                QueryExecutionContext={"Database": "dc-wide-logs"},
                ResultConfiguration={
                    "OutputLocation": f"s3://{RESULTS_BUCKET}/"
                },
            )

            query_execution_id = response["QueryExecutionId"]
            execution_ids[query_name] = query_execution_id
            query_attempt_count += 1

    for query_name, query_execution_id in execution_ids.items():
        query_status = wait_for_query_to_complete(
            athena_client, query_execution_id
        )

        if query_status == "SUCCEEDED":
            for query_type, output_dir in query_types.items():
                if query_type in query_name:
                    output_file = (
                        output_dir
                        / f"{query_name.replace(f'{election_date}/', '')}.tsv"
                    )
                    save_query_results(
                        athena_client, query_execution_id, output_file
                    )
                    query_success_count += 1
                    break
        else:
            query_failure_count += 1
            print(f"Query failed with status: {query_status}")

    end_time = time.time()
    duration = end_time - start_time

    print(f"{query_attempt_count} queries attempted")
    print(f"{query_success_count} queries successfully run")
    print(f"{query_failure_count} queries failed")
    print(f"Total execution time: {duration:.2f} seconds")


def handle():
    parser = argparse.ArgumentParser(
        description="Run Athena queries for a specific election and save results."
    )
    parser.add_argument(
        "election_date", help="The election date in YYYY-MM-DD format"
    )
    parser.add_argument("--profile", required=True, help="AWS profile to use")
    args = parser.parse_args()

    run_election_queries(args.election_date, args.profile)


if __name__ == "__main__":
    handle()
