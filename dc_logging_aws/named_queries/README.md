## Make report Queries

You'll probably need to update some table names, but this is the gist of it:

### Make the files:

eg for a regular may election include the month of April as the election period

```shell
python dc_logging_aws/named_queries/commands/create_election_query_files.py 2024-05-02 2024-04-01
```

or for a GE include from the point when the election was called. 

```shell
python dc_logging_aws/named_queries/commands/create_election_query_files.py 2024-07-04 2024-05-22 
```

### Check them

Have a read. See if they seem sensible. 

### Then send to Athena:

you can get the updown api key from another query in the athena console. 

```shell
UPDOWN_API_KEY=1234 python dc_logging_aws/named_queries/commands/create_athena_queries.py --profile prod-monitoring-dc 2024-07-04  
```


### Run them

results bucket env var isn't necessary.  

```shell
RESULTS_BUCKET=**** python dc_logging_aws/named_queries/commands/run_queries.py 2024-07-04 --profile prod-monitoring-dc
```