# Uploader for legacy logs

This fetches old loggedpostcode entries from the database, batches them by hour
and uploads them to S3.

## Usage

1. Install dependencies with `pipenv install --dev`
2. Get a shell with an AWS session, eg `export AWS_PROFILE=DeveloperAccess-dc-dev`
   or `aws-vault exec DeveloperAccess-dc-dev`
3. Export the `PGxxxx` environment variables for the database connection, eg:
   ```bash
   PGUSER=wcivf
   PGDATABASE=wcivf_logger
   PGHOST=wcivf-logger.deadbeef.eu-west-2.rds.amazonaws.com
   PGPASSWORD=hunter2
   ```
4. Check the earliest logs in S3 per product with:
   ```sql
   SELECT dc_product, min(timestamp) 
   FROM "dc-wide-logs"."dc_postcode_searches_table"
   GROUP BY 1
   ```

   This was previously:

    | dc_product     | min                     |
    | -------------- | ----------------------- |
    | WCIVF          | 2022-04-21 14:59:17.289 |
    | AGGREGATOR_API | 2022-03-08 16:59:02.140 |
    | WDIV           | 2022-04-21 14:23:31.977 |
    | EC_API         | 2023-11-21 21:15:54.096 |

5. Run the script, passing a slightly lower `end` value from the above query, eg:
   ```bash
   pipenv run python dc_logging_legacy_import/main.py --end "2022-04-21 14:59:17.288" --dc-product WCIVF --bucket the-production-logs-bucket
   ```
   The script will fetch all logs from the database first before starting the 
   upload to S3, so it may take a while to start.

## Running from AWS CloudShell

For databases like WDIV, the quantity of logs is too large to fetch and upload
from a local machine. Instead, we can run the script from AWS CloudShell, which
has better connectivity to the database and S3.

1. Open the [AWS CloudShell](https://eu-west-2.console.aws.amazon.com/cloudshell/home?region=eu-west-2#)
2. Clone this repo:
   ```bash
   git clone https://github.com/DemocracyClub/dc_logging.git
   ```
3. Install Python 3.11:
   ```bash
   sudo dnf install -y python3.11 python3.11-pip
   ```
4. Install pipenv:
   ```bash
   pip3.11 install --user pipenv
   ```
5. Install dependencies:
   ```bash
   cd dc_logging
   pipenv install --dev --python /usr/bin/python3.11
   ```
6. Follow the instructions above to get a shell with an AWS session and the
   database credentials, then run the script.