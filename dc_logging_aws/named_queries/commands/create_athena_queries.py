import argparse
import os
from pathlib import Path

import boto3


def get_existing_queries(athena_client):
    existing_queries = {}
    print("Getting existing queries...")
    paginator = athena_client.get_paginator("list_named_queries")
    for page in paginator.paginate():
        for query_id in page["NamedQueryIds"]:
            query = athena_client.get_named_query(NamedQueryId=query_id)
            existing_queries[query["NamedQuery"]["Name"]] = query_id
    return existing_queries


def get_queries_dir(subdirectory):
    script_path = Path(__file__).resolve()
    queries_dir = script_path.parent.parent / "queries" / subdirectory

    if not queries_dir.is_dir():
        raise FileNotFoundError(
            f"Error: Directory '{queries_dir}' does not exist."
        )

    return queries_dir


def update_named_query(
    athena_client, query_name, query_string, existing_queries
):
    query_id = existing_queries[query_name]
    try:
        athena_client.update_named_query(
            NamedQueryId=query_id,
            Name=query_name,
            QueryString=query_string,
        )
        print(f"Updated named query: {query_name}")
    except Exception as e:
        print(f"Error updating named query '{query_name}': {str(e)}")


def create_named_query(athena_client, query_name, query_string):
    try:
        athena_client.create_named_query(
            Name=query_name,
            Database="dc-wide-logs",
            QueryString=query_string,
        )
        print(f"Created named query: {query_name}")
    except athena_client.exceptions.NamedQueryAlreadyExistsException:
        print(
            f"Named query '{query_name}' already exists. Use update function to modify."
        )
    except Exception as e:
        print(f"Error creating named query '{query_name}': {str(e)}")


def get_query_string(file_path):
    query_string = file_path.read_text()

    if updown_api_key := os.environ.get("UPDOWN_API_KEY"):
        query_string = query_string.replace("UPDOWN_API_KEY", updown_api_key)
    return query_string


def create_athena_queries(subdirectory, profile, overwrite):
    queries_dir = get_queries_dir(subdirectory)

    session = boto3.Session(profile_name=profile)
    athena_client = session.client("athena")

    # Get list of existing named queries
    existing_queries = get_existing_queries(athena_client)

    for file_path in queries_dir.glob("*.sql"):
        query_name = f"{subdirectory}/{file_path.stem}"
        query_string = get_query_string(file_path)

        if query_name in existing_queries and not overwrite:
            print(
                f"Query '{query_name}' already exists. Use --overwrite to replace it."
            )
            continue

        if query_name in existing_queries:
            update_named_query(
                athena_client, query_name, query_string, existing_queries
            )
            continue

        create_named_query(athena_client, query_name, query_string)


def handle():
    parser = argparse.ArgumentParser(
        description="Create Athena named queries from SQL files in a specified subdirectory."
    )
    parser.add_argument(
        "subdirectory", help="Name of the subdirectory in the queries folder"
    )
    parser.add_argument("--profile", required=True, help="AWS profile to use")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing queries with the same name",
    )
    args = parser.parse_args()

    create_athena_queries(args.subdirectory, args.profile, args.overwrite)


if __name__ == "__main__":
    handle()
