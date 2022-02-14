import csv
import sys

from .log_streams import DCWidePostcodeLookupsLogStream
from .log_client import LoggingClient
from .log_entries import PostcodeLogEntry

logger = LoggingClient(DCWidePostcodeLookupsLogStream)

batch = []
batch_size = 500

for line in csv.DictReader(sys.stdin):
    batch.append(
        PostcodeLogEntry(
            dc_product="wcivf", timestamp=line["created"], postcode=line["postcode"]
        )
    )
    if len(batch) >= batch_size:
        logger.log_batch(batch)
        batch = []
