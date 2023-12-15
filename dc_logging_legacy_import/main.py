import argparse
import datetime
import gzip
import hashlib
from typing import Iterator, List, Tuple

import boto3
import psycopg
import tqdm

from dc_logging_client.log_entries import PostcodeLogEntry

s3 = boto3.resource("s3")


class Options:
    start: datetime.datetime
    end: datetime.datetime
    bucket: str
    prefix: str
    dc_product: str


def hourly_batches(
    cur: psycopg.cursor
) -> Iterator[Tuple[Tuple[int], List[PostcodeLogEntry]]]:
    dayhour = (0, 0, 0, 0)
    batch: List[PostcodeLogEntry] = []
    for row in cur:
        if row[0].hour != dayhour[3]:
            dayhour = (row[0].year, row[0].month, row[0].day, row[0].hour)
            if batch:
                yield batch
            batch = []

        batch.append(
            PostcodeLogEntry(
                timestamp=row[0],
                postcode=row[1],
                utm_source=row[2],
                utm_medium=row[3],
                utm_campaign=row[4],
                dc_product=OPTIONS.dc_product,
                api_key="",
                calls_devs_dc_api=False,
            )
        )
    yield batch


def serialize_to_file(batch: List[PostcodeLogEntry]) -> (str, str):
    date = ""
    data = ""
    for entry in batch:
        if date == "":
            date = entry.timestamp.strftime("%Y/%m/%d/%H")
        data += entry.as_log_line()

    # hash the data
    hash = hashlib.sha256(data.encode("utf-8")).hexdigest()
    return (f"{date}/{OPTIONS.dc_product}-{hash}", data)


def upload_file(key, data):
    s3.Object(OPTIONS.bucket, f"{OPTIONS.prefix}{key}.gz").put(
        Body=gzip.compress(data.encode("utf-8"))
    )


def main():
    with psycopg.connect() as conn:
        min, max = (None, None)
        # Get max and min for progress
        with conn.execute(
            """
                SELECT MIN(created), MAX(created)
                FROM core_loggedpostcode
                WHERE created >= %s AND created <= %s
            """,
            (OPTIONS.start, OPTIONS.end),
        ) as cur:
            min, max = cur.fetchone()
            round_min = min.replace(minute=0, second=0, microsecond=0)
            round_max = max.replace(minute=0, second=0, microsecond=0)

            total_partitions = (round_max - round_min).total_seconds() / 3600

        print("⬇️  Fetching data from postgres")
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT
                        created as timestamp,
                        postcode,
                        utm_source,
                        utm_medium,
                        utm_campaign
                    FROM core_loggedpostcode
                    WHERE created >= %s AND created <= %s
                    ORDER BY created ASC
                """,
                (OPTIONS.start, OPTIONS.end),
            )
            for batch in tqdm.tqdm(
                hourly_batches(cur), total=int(total_partitions)
            ):
                key, data = serialize_to_file(batch)
                upload_file(key, data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S.%f"),
        help="Start date and hour inclusive (YYYY-mm-DD HH:MM:SS.fff)",
    )
    parser.add_argument(
        "--end",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S.%f"),
        help="End date and hour inclusive (YYYY-mm-DD HH:MM:SS.fff)",
        required=True,
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="dc-monitoring-dev-logging",
        help="S3 bucket to upload to",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="dc-postcode-searches/",
        help="S3 prefix to upload to",
    )
    parser.add_argument(
        "--dc-product",
        type=str,
        help="DC Product to upload as (WCIVF, WDIV)",
        required=True,
    )

    global OPTIONS
    OPTIONS: Options = parser.parse_args()
    if not OPTIONS.start:
        OPTIONS.start = datetime.datetime.strptime(
            "2017/05/01/00", "%Y/%m/%d/%H"
        )

    main()
