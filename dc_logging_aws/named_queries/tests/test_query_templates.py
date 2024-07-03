import textwrap
from datetime import date, datetime

import pytest
from zoneinfo import ZoneInfo

from dc_logging_aws.named_queries.query_template import QueryTemplate


@pytest.fixture
def query_template_2024_05_02():
    polling_day = date(2024, 5, 2)
    start_of_election_period = date(2024, 4, 1)
    return QueryTemplate(polling_day, start_of_election_period)


@pytest.fixture
def query_template_2019_12_12():
    polling_day = date(2019, 12, 12)
    start_of_election_period = date(2019, 11, 1)
    return QueryTemplate(polling_day, start_of_election_period)


@pytest.mark.parametrize(
    "query_template_fixture, expected_close_of_polls, expected_start_of_polling_week",
    [
        (
            "query_template_2024_05_02",
            datetime(2024, 5, 2, 22, tzinfo=ZoneInfo("Europe/London")),
            datetime(2024, 4, 29, 0, tzinfo=ZoneInfo("Europe/London")),
        ),
        (
            "query_template_2019_12_12",
            datetime(2019, 12, 12, 22, tzinfo=ZoneInfo("Europe/London")),
            datetime(2019, 12, 9, 0, tzinfo=ZoneInfo("Europe/London")),
        ),
    ],
)
def test_named_query_templates(
    request,
    query_template_fixture,
    expected_close_of_polls,
    expected_start_of_polling_week,
):
    query_template = request.getfixturevalue(query_template_fixture)

    assert (
        query_template.close_of_polls == expected_close_of_polls
    ), f"Expected {expected_close_of_polls}, but got {query_template.close_of_polls}"
    assert (
        query_template.start_of_polling_week == expected_start_of_polling_week
    ), f"Expected {expected_start_of_polling_week}, but got {query_template.start_of_polling_week}"


def test_election_period_cte(query_template_2024_05_02):
    expected_output = textwrap.dedent(
        """
        SELECT *
        FROM "dc-wide-logs"."dc_postcode_searches_table" all_logs
        WHERE "day" >= '2024/03/31' 
            AND "day" <= '2024/05/02'
            AND all_logs."api_key" != 'UPDOWN_API_KEY' --updown
            AND (
                    (all_logs."dc_product" != 'WDIV')
                OR 
                    (all_logs."dc_product" = 'WDIV' AND replace(all_logs."postcode",' ','') != 'BS44NN') --updown
            )
            AND LOWER("calls_devs_dc_api") = 'false'
        """
    ).strip()

    result = query_template_2024_05_02.election_period_cte()
    assert result == expected_output


def test_election_period_cte_no_exclude_api(query_template_2024_05_02):
    expected_output = textwrap.dedent(
        """
        SELECT *
        FROM "dc-wide-logs"."dc_postcode_searches_table" all_logs
        WHERE "day" >= '2024/03/31' 
            AND "day" <= '2024/05/02'
            AND all_logs."api_key" != 'UPDOWN_API_KEY' --updown
            AND (
                    (all_logs."dc_product" != 'WDIV')
                OR 
                    (all_logs."dc_product" = 'WDIV' AND replace(all_logs."postcode",' ','') != 'BS44NN') --updown
            )
        """
    ).strip()

    result = query_template_2024_05_02.election_period_cte(
        exclude_calls_devs_dc_api=False
    )
    assert result == expected_output


def test_logs_cte(query_template_2024_05_02):
    expected_output = """
        SELECT *
        FROM ELECTION_PERIOD
        WHERE (
            "timestamp" >= cast('2024-04-28 23:00' AS timestamp)
            AND "timestamp" <= cast('2024-05-02 21:00' AS timestamp)
            AND "dc_product" != 'WDIV'
        ) OR (
            "timestamp" >= cast('2024-04-29 00:00' AS timestamp)
            AND "timestamp" <= cast('2024-05-02 22:00' AS timestamp)
            AND "dc_product" = 'WDIV'
        )
        """

    result = query_template_2024_05_02.logs_cte(
        query_template_2024_05_02.start_of_polling_week,
    )

    # Remove leading/trailing whitespace and compare
    assert result == textwrap.dedent(expected_output).strip()


def test_postcode_search_count(query_template_2024_05_02):
    expected_output = textwrap.dedent(
        """
        WITH ELECTION_PERIOD AS (
            SELECT *
            FROM "dc-wide-logs"."dc_postcode_searches_table" all_logs
            WHERE "day" >= '2024/03/31' 
                AND "day" <= '2024/05/02'
                AND all_logs."api_key" != 'UPDOWN_API_KEY' --updown
                AND (
                        (all_logs."dc_product" != 'WDIV')
                    OR 
                        (all_logs."dc_product" = 'WDIV' AND replace(all_logs."postcode",' ','') != 'BS44NN') --updown
                )
                AND LOWER("calls_devs_dc_api") = 'false'
        ), LOGS AS (
            SELECT *
            FROM ELECTION_PERIOD
            WHERE (
                "timestamp" >= cast('2024-04-28 23:00' AS timestamp)
                AND "timestamp" <= cast('2024-05-02 21:00' AS timestamp)
                AND "dc_product" != 'WDIV'
            ) OR (
                "timestamp" >= cast('2024-04-29 00:00' AS timestamp)
                AND "timestamp" <= cast('2024-05-02 22:00' AS timestamp)
                AND "dc_product" = 'WDIV'
            )
        )
        SELECT
            count(*) as count
        FROM
            LOGS;
        """
    ).strip()

    result = query_template_2024_05_02.postcode_search_count(
        query_template_2024_05_02.start_of_polling_week
    )
    assert result == expected_output


def test_product_count_cte(query_template_2024_05_02):
    expected_output = textwrap.dedent(
        """
        SELECT
            count(*) AS count, "dc_product", '' AS key_name, '' AS user_name, '' AS email, utm_source
            FROM LOGS
            WHERE dc_product = 'WDIV'
            GROUP BY "dc_product", "api_key", "utm_source"
        UNION SELECT
            count(*) AS count, "dc_product", '' AS key_name, '' AS user_name, '' AS email, utm_source
            FROM LOGS
            WHERE dc_product = 'WCIVF'
            GROUP BY "dc_product", "api_key", "utm_source"
        UNION SELECT
            count(*) AS count, "dc_product", api_users."key_name", api_users."user_name", api_users."email", utm_source
            FROM LOGS
                JOIN "dc-wide-logs"."api-users-ec-api" as api_users ON LOGS."api_key" = api_users."key"
            WHERE dc_product = 'EC_API'
            GROUP BY "dc_product", "key_name", "user_name", "utm_source", "email"
        UNION SELECT
            count(*) AS count, "dc_product", api_users."key_name", api_users."user_name", api_users."email", utm_source
            FROM LOGS
                JOIN "dc-wide-logs"."api-users-aggregator-api" as api_users ON LOGS."api_key" = api_users."key"
            WHERE
                dc_product = 'AGGREGATOR_API'
                AND api_users."key_name" NOT IN (
                    'EC postcode pages - Dev', 'WhoCanIVoteFor', 'Updown', 'EC API'
                )
            GROUP BY "dc_product", "key_name", "user_name", "utm_source", "email"
        """
    ).strip()

    result = query_template_2024_05_02.product_count_cte()

    assert result.strip() == expected_output


def test_postcode_searches_by_product(query_template_2024_05_02):
    expected_output = textwrap.dedent(
        """
        WITH ELECTION_PERIOD AS (
            SELECT *
            FROM "dc-wide-logs"."dc_postcode_searches_table" all_logs
            WHERE "day" >= '2024/03/31' 
                AND "day" <= '2024/05/02'
                AND all_logs."api_key" != 'UPDOWN_API_KEY' --updown
                AND (
                        (all_logs."dc_product" != 'WDIV')
                    OR 
                        (all_logs."dc_product" = 'WDIV' AND replace(all_logs."postcode",' ','') != 'BS44NN') --updown
                )
        ), LOGS AS (
            SELECT *
            FROM ELECTION_PERIOD
            WHERE (
                "timestamp" >= cast('2024-04-28 23:00' AS timestamp)
                AND "timestamp" <= cast('2024-05-02 21:00' AS timestamp)
                AND "dc_product" != 'WDIV'
            ) OR (
                "timestamp" >= cast('2024-04-29 00:00' AS timestamp)
                AND "timestamp" <= cast('2024-05-02 22:00' AS timestamp)
                AND "dc_product" = 'WDIV'
            )
        ), PRODUCT_COUNTS AS (
            SELECT
                count(*) AS count, "dc_product", '' AS key_name, '' AS user_name, '' AS email, utm_source
                FROM LOGS
                WHERE dc_product = 'WDIV'
                GROUP BY "dc_product", "api_key", "utm_source"
            UNION SELECT
                count(*) AS count, "dc_product", '' AS key_name, '' AS user_name, '' AS email, utm_source
                FROM LOGS
                WHERE dc_product = 'WCIVF'
                GROUP BY "dc_product", "api_key", "utm_source"
            UNION SELECT
                count(*) AS count, "dc_product", api_users."key_name", api_users."user_name", api_users."email", utm_source
                FROM LOGS
                    JOIN "dc-wide-logs"."api-users-ec-api" as api_users ON LOGS."api_key" = api_users."key"
                WHERE dc_product = 'EC_API'
                GROUP BY "dc_product", "key_name", "user_name", "utm_source", "email"
            UNION SELECT
                count(*) AS count, "dc_product", api_users."key_name", api_users."user_name", api_users."email", utm_source
                FROM LOGS
                    JOIN "dc-wide-logs"."api-users-aggregator-api" as api_users ON LOGS."api_key" = api_users."key"
                WHERE
                    dc_product = 'AGGREGATOR_API'
                    AND api_users."key_name" NOT IN (
                        'EC postcode pages - Dev', 'WhoCanIVoteFor', 'Updown', 'EC API'
                    )
                GROUP BY "dc_product", "key_name", "user_name", "utm_source", "email"
        )
        SELECT *
        FROM
            PRODUCT_COUNTS
        ORDER BY count DESC;
        """
    ).strip()

    result = query_template_2024_05_02.postcode_searches_by_product(
        query_template_2024_05_02.start_of_polling_week
    )
    assert result == expected_output


def test_postcode_searches_by_local_authority(query_template_2024_05_02):
    expected_output = textwrap.dedent(
        """
        WITH ELECTION_PERIOD AS (
            SELECT *
            FROM "dc-wide-logs"."dc_postcode_searches_table" all_logs
            WHERE "day" >= '2024/03/31' 
                AND "day" <= '2024/05/02'
                AND all_logs."api_key" != 'UPDOWN_API_KEY' --updown
                AND (
                        (all_logs."dc_product" != 'WDIV')
                    OR 
                        (all_logs."dc_product" = 'WDIV' AND replace(all_logs."postcode",' ','') != 'BS44NN') --updown
                )
                AND LOWER("calls_devs_dc_api") = 'false'
        ), LOGS AS (
            SELECT *
            FROM ELECTION_PERIOD
            WHERE (
                "timestamp" >= cast('2024-04-28 23:00' AS timestamp)
                AND "timestamp" <= cast('2024-05-02 21:00' AS timestamp)
                AND "dc_product" != 'WDIV'
            ) OR (
                "timestamp" >= cast('2024-04-29 00:00' AS timestamp)
                AND "timestamp" <= cast('2024-05-02 22:00' AS timestamp)
                AND "dc_product" = 'WDIV'
            )
        )
        SELECT
            oslaua as gss, count(*) as postcode_searches
        FROM
            LOGS JOIN "pollingstations.public.data"."onspd-2024-feb"
                ON upper(replace(replace("postcode",' ', '' ),'+','')) = upper(replace( "pcds",' ', ''))
        GROUP BY oslaua
        ORDER BY postcode_searches DESC;
        """
    ).strip()

    result = query_template_2024_05_02.postcode_searches_by_local_authority(
        query_template_2024_05_02.start_of_polling_week
    )
    assert result == expected_output


def test_postcode_searches_by_constituency(query_template_2024_05_02):
    expected_output = textwrap.dedent(
        """
        WITH ELECTION_PERIOD AS (
            SELECT *
            FROM "dc-wide-logs"."dc_postcode_searches_table" all_logs
            WHERE "day" >= '2024/03/31' 
                AND "day" <= '2024/05/02'
                AND all_logs."api_key" != 'UPDOWN_API_KEY' --updown
                AND (
                        (all_logs."dc_product" != 'WDIV')
                    OR 
                        (all_logs."dc_product" = 'WDIV' AND replace(all_logs."postcode",' ','') != 'BS44NN') --updown
                )
                AND LOWER("calls_devs_dc_api") = 'false'
        ), LOGS AS (
            SELECT *
            FROM ELECTION_PERIOD
            WHERE (
                "timestamp" >= cast('2024-04-28 23:00' AS timestamp)
                AND "timestamp" <= cast('2024-05-02 21:00' AS timestamp)
                AND "dc_product" != 'WDIV'
            ) OR (
                "timestamp" >= cast('2024-04-29 00:00' AS timestamp)
                AND "timestamp" <= cast('2024-05-02 22:00' AS timestamp)
                AND "dc_product" = 'WDIV'
            )
        )
        SELECT
            c.name,
            c.official_identifier,
            count(*) as postcode_searches
        FROM
            LOGS
            JOIN "ee.public.data"."postcode-to-2024-parl-constituency" c
                ON upper(replace(replace(LOGS."postcode",' ', '' ),'+','')) = upper(replace(c.pcds,' ', ''))
        GROUP BY c.name, c.official_identifier
        ORDER BY postcode_searches DESC;
        """
    ).strip()

    result = query_template_2024_05_02.postcode_searches_by_constituency(
        query_template_2024_05_02.start_of_polling_week
    )
    assert result == expected_output
