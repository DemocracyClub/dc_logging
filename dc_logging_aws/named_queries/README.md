### Make report Queries

You'll probably need to update some table names, but this is the gist of it:

Make the files:

eg 

```shell
python dc_logging_aws/named_queries/commands/create_election_query_files.py 2024-05-02 2024-04-01
```

or 

```shell
python dc_logging_aws/named_queries/commands/create_election_query_files.py 2024-07-04 2024-05-22 
```

Check them

Then send to Athena:

```shell
UPDOWN_API_KEY=1234 python dc_logging_aws/named_queries/commands/create_athena_queries.py --profile prod-monitoring-dc 2024-07-04  
```


Run them

```shell
RESULTS_BUCKET=**** python dc_logging_aws/named_queries/commands/run_queries.py 2024-07-04 --profile prod-monitoring-dc
```