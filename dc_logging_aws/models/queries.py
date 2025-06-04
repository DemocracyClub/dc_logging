from models.databases import dc_wide_logs_db
from models.models import BaseQuery

total_searches_query = BaseQuery(
    name="total_searches_query",
    creation_context={
        "query_file_path": "election_reporting/total_searches_query.sql"
    },
    database=dc_wide_logs_db,
    query_context={
        "start_of_election_period_day": "",
        "polling_day": "",
        "updown_api_key": "",
        "start_datetime_utc": "",
        "end_datetime_utc": "",
        "start_datetime_london": "",
        "end_datetime_london": "",
    },
)

by_local_authority_query = BaseQuery(
    name="by_local_authority_query",
    creation_context={
        "query_file_path": "election_reporting/searches_by_local_authority.sql"
    },
    database=dc_wide_logs_db,
    query_context={
        "start_of_election_period_day": "",
        "polling_day": "",
        "updown_api_key": "",
        "start_datetime_utc": "",
        "end_datetime_utc": "",
        "start_datetime_london": "",
        "end_datetime_london": "",
    },
)
