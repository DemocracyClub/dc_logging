from dc_logging_client.log_client import FirehoseClientWrapper


def test_client_connection(sts, firehose, example_arn):
    firehose = FirehoseClientWrapper(example_arn)
    firehose.connect()
    assert (
        firehose.client.meta.endpoint_url
        == "https://firehose.eu-west-2.amazonaws.com"
    )
