# DC Logging

## Client
A python library for submitting log events form applications.

The logging client provides four things:

1. A `Logger` class that manages submitting log events
2. Managed log stream classes
3. Classes for creating log entries
4. Validation that everything is being used properly

### Local Development

Docker is required on the local system to run test.

* Install Python dependencies: `pipenv install --dev`
* Run the test suite: `pytest`
* Run lint checks: `ruff .`
* Auto-format: `ruff format`

#### Testing

We use `moto` to test the various AWS moving parts. `moto`
builds (almost) everything from the CDK CloudFormation template. 

If you make change to the Stack, or the first time you install the project, you 
need to run `make cfn_template_for_tests`. This file isn't checked in to Git
as it contains actual values from the deployment. 

### Installation

Install the desired version using pip or pipenv.

For pipenv, especially on projects deployed on AWS Lambda, it's advised to use
the `zip` package from the release page:

`pipenv install https://github.
com/DemocracyClub/dc_logging/archive/refs/tags/[VERSION].zip`


### Using the library

#### Logging classes

The library contains a single logger class per log stream. A log stream 
represents the category of log, and all logs for a single stream are stored 
together.

##### DCWidePostcodeLoggingClient
Currently, there is a single log stream defined: `DCWidePostcodeLoggingClient`.

This is designed to log all postcodes entered from any DC site. 

If the application in turn calls the developers.democracyclub.org.uk API then
`calls_devs_dc_api` MUST be set to `True`. This will prevent double counting 
usage when querying later.


#### Create a logger

It's recommended that loggers are  created globally to the application, for 
example in a Django settings module.

```python
# settings.py
from dc_logging_client.log_client import DCWidePostcodeLoggingClient
POSTCODE_LOGGER = DCWidePostcodeLoggingClient(function_arn="arn")
```

The ARN to pass in should be the correct one for the log stream (currently
only DCWidePostcodeLoggingClient) and the environment (currently only
development or production). That means at the moment there are only two
possible ARNs here. Find them in the DC dev handbook.

#### Create an entry

At the point you want to create a log entry

```python
entry = POSTCODE_LOGGER.entry_class(
    postcode="SW1A 1AA", 
    dc_product=POSTCODE_LOGGER.dc_product.wcivf
)
```

Note the `dc_product`. This is an Enum that is validated against a set of known
and supported DC products. If you are trying to use this library in a DC
product that's not supported then please make a PR to this repo.

And log it

````python
POSTCODE_LOGGER.log(entry)
````



### AWS services

Logs are submitted initially to a Lambda ingest function and then to
[AWS Kenisis Firehose](https://aws.amazon.com/kinesis/data-firehose/).

Understanding how Firehose works shouldn't be required, but some high 
level basics are useful:

Firehose provides _log streams_ that are essentially endpoints that accept data.

Each stream can be configured to process the data in various way. For 
example, by putting it in S3, calling a AWS Lambda ingest function, adding to a 
relational database, etc.

Firehose doesn't validate the incoming data, so it's important that clients 
write consistently.

This library mainly attempts to manage this consistency.

The initial Lambda ingest function is needed for cross account support: Firehose
doesn't support organisational wide permissions, meaning it's only possible to
write to the account that hosts the log stream. To get around this, we have
a Lambda ingest function per log stream (and environment) that _can_ be called
cross-account, and this function relays the log message on to Firehose.


```mermaid
graph TB
    dc_logging_client["DCWidePostcodeLoggingClient()"]
    put_log["DCWidePostcodeLoggingClient().log(entry)"]
    lambda_ingest["Lambda Ingest function"]
    
    subgraph application_account [" Application Account"]
        subgraph application ["Application"]
            direction TB
            dc_logging_client --> put_log
        end
    end

    subgraph aws_monitoring ["AWS Monitoring Account"]
        direction TB
        application --Validate PrincipalOrgID--> lambda_ingest --> firehose
        firehose["Firehose DataStream"]
        convert["Convert to ORC"]
        s3["S3 log storage"]
        glue["AWS Glue table definition"]
        athena["AWS Athena (SQL query logs)"]
        
        firehose --> convert --> s3
        s3 --> glue --> athena
    end
```

The end result of this is that the client needs two things:


1. To have a PrincipalOrgID of the DC organisation. This means, is in an account in 
   th DC org, or is an authenticated user in that organisation
2. The function ARN of the ingest function. Take this from the DC dev 
   handbook, and ensure it matches the environment you're deploying to 
   (currently either `development` or `production`). DO NOT LOG TO THE WRONG 
   PLACE
