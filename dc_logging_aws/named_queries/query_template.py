import textwrap
from datetime import date, datetime, time, timedelta

from zoneinfo import ZoneInfo


def datetime_to_athena_datetime_string(
    dt: datetime, tz_target: ZoneInfo
) -> str:
    """
    Converts a timezone-aware datetime to an Athena-compatible string.

    Parameters:
    dt (datetime): Timezone-aware datetime object.
    tz_target (ZoneInfo): Target timezone (In this case the timezone your logs are in).

    Returns:
    str: Datetime string formatted as "YYYY-MM-DD HH:MM".

    Raises:
    ValueError: If `dt` is not timezone-aware.
    """
    if dt.tzinfo is None:
        raise ValueError("Input datetime must be timezone-aware")

    # Convert to target timezone
    dt_target = dt.astimezone(tz_target)

    # Format the datetime string
    return dt_target.strftime("%Y-%m-%d %H:%M")


def indent_cte_string(multiline_string: str, indent_length: int = 8) -> str:
    """
    Indents all lines of a multiline string except the first one.
    """
    lines = multiline_string.split("\n")
    indented_lines = lines[:1] + [
        " " * indent_length + line for line in lines[1:]
    ]
    return "\n".join(indented_lines)


def utc_athena_time(dt):
    return datetime_to_athena_datetime_string(dt, ZoneInfo("UTC"))


def london_athena_time(dt):
    return datetime_to_athena_datetime_string(dt, ZoneInfo("Europe/London"))


class QueryTemplate:
    def __init__(self, polling_day: date, start_of_election_period: date):
        self.polling_day = polling_day
        self.start_of_election_period = start_of_election_period
        self.close_of_polls = datetime.combine(
            self.polling_day,
            time(22, 0, tzinfo=ZoneInfo("Europe/London")),
        )

    @property
    def start_of_polling_week(self):
        monday = self.polling_day - timedelta(days=3)
        return datetime.combine(
            monday, time(0, 0, tzinfo=ZoneInfo("Europe/London"))
        )

    def election_period_cte(self, exclude_calls_devs_dc_api=True):
        start_date = self.start_of_election_period - timedelta(days=1)
        cte = textwrap.dedent(
            f"""
            SELECT *
            FROM "dc-wide-logs"."dc_postcode_searches_table" all_logs
            WHERE "day" >= '{start_date.strftime('%Y/%m/%d')}' 
                AND "day" <= '{self.polling_day.strftime('%Y/%m/%d')}'
                AND all_logs."api_key" != 'UPDOWN_API_KEY' --updown
                AND (
                        (all_logs."dc_product" != 'WDIV')
                    OR 
                        (all_logs."dc_product" = 'WDIV' AND replace(all_logs."postcode",' ','') != 'BS44NN') --updown
                )
            """
        ).strip()

        if exclude_calls_devs_dc_api:
            cte = cte + """\n    AND LOWER("calls_devs_dc_api") = 'false'"""
        return cte

    def logs_cte(
        self, start_time, from_source="ELECTION_PERIOD", timeseries=False
    ):
        extra_select = ""
        if timeseries:
            extra_select = (
                ", date_format(timestamp, '%Y-%m-%d %H') as hour_segment"
            )
        return textwrap.dedent(
            f"""
            SELECT *{extra_select}
            FROM {from_source}
            WHERE (
                "timestamp" >= cast('{utc_athena_time(start_time)}' AS timestamp)
                AND "timestamp" <= cast('{utc_athena_time(self.close_of_polls)}' AS timestamp)
                AND "dc_product" != 'WDIV'
            ) OR (
                "timestamp" >= cast('{london_athena_time(start_time)}' AS timestamp)
                AND "timestamp" <= cast('{london_athena_time(self.close_of_polls)}' AS timestamp)
                AND "dc_product" = 'WDIV'
            )
            """
        ).strip()

    def postcode_search_count(self, start_time):
        logs_indent_cte_string = indent_cte_string(
            self.logs_cte(start_time), 12
        )
        election_period_cte = indent_cte_string(self.election_period_cte(), 12)
        return textwrap.dedent(
            f"""
            WITH ELECTION_PERIOD AS (
            {textwrap.indent(election_period_cte,'    ')}
            ), LOGS AS (
            {textwrap.indent(logs_indent_cte_string,'    ')}
            )
            SELECT
                count(*) as count
            FROM
                LOGS;
            """
        ).strip()

    def product_count_cte(self, logs_cte_name="LOGS", timeseries=False):
        extra_select = ""
        extra_group_by = ""
        if timeseries:
            extra_select = ", hour_segment"
            extra_group_by = ", hour_segment"
        return textwrap.dedent(
            f"""
            SELECT
                count(*) AS count, "dc_product", '' AS key_name, '' AS user_name, '' AS email, utm_source{extra_select}
                FROM {logs_cte_name}
                WHERE dc_product = 'WDIV'
                GROUP BY "dc_product", "api_key", "utm_source"{extra_group_by}
            UNION SELECT
                count(*) AS count, "dc_product", '' AS key_name, '' AS user_name, '' AS email, utm_source{extra_select}
                FROM {logs_cte_name}
                WHERE dc_product = 'WCIVF'
                GROUP BY "dc_product", "api_key", "utm_source"{extra_group_by}
            UNION SELECT
                count(*) AS count, "dc_product", api_users."key_name", api_users."user_name", api_users."email", utm_source{extra_select}
                FROM {logs_cte_name}
                    JOIN "dc-wide-logs"."api-users-ec-api" as api_users ON {logs_cte_name}."api_key" = api_users."key"
                WHERE dc_product = 'EC_API'
                GROUP BY "dc_product", "key_name", "user_name", "utm_source", "email"{extra_group_by}
            UNION SELECT
                count(*) AS count, "dc_product", api_users."key_name", api_users."user_name", api_users."email", utm_source{extra_select}
                FROM {logs_cte_name}
                    JOIN "dc-wide-logs"."api-users-aggregator-api" as api_users ON {logs_cte_name}."api_key" = api_users."key"
                WHERE
                    dc_product = 'AGGREGATOR_API'
                    AND api_users."key_name" NOT IN (
                        'EC postcode pages - Dev', 'WhoCanIVoteFor', 'Updown', 'EC API'
                    )
                GROUP BY "dc_product", "key_name", "user_name", "utm_source", "email"{extra_group_by}
            """
        ).strip()

    def postcode_searches_by_product(self, start_time):
        election_period_cte = indent_cte_string(
            self.election_period_cte(exclude_calls_devs_dc_api=False), 12
        )
        logs_indent_cte_string = indent_cte_string(
            self.logs_cte(start_time), 12
        )
        product_indent_cte_string = indent_cte_string(
            self.product_count_cte(), 12
        )

        return textwrap.dedent(
            f"""
            WITH ELECTION_PERIOD AS (
            {textwrap.indent(election_period_cte,'    ')}
            ), LOGS AS (
            {textwrap.indent(logs_indent_cte_string,'    ')}
            ), PRODUCT_COUNTS AS (
            {textwrap.indent(product_indent_cte_string, '    ')}
            )
            SELECT *
            FROM
                PRODUCT_COUNTS
            ORDER BY count DESC;
            """
        ).strip()

    def postcode_timeseries_by_product(self, start_time):
        election_period_cte = indent_cte_string(
            self.election_period_cte(exclude_calls_devs_dc_api=False), 12
        )

        logs_indent_cte_string = indent_cte_string(
            self.logs_cte(start_time, timeseries=True), 12
        )
        product_indent_cte_string = indent_cte_string(
            self.product_count_cte(timeseries=True), 12
        )
        return textwrap.dedent(
            f"""
            WITH ELECTION_PERIOD AS (
            {textwrap.indent(election_period_cte, '    ')}
            ), LOGS AS (
            {textwrap.indent(logs_indent_cte_string, '    ')}
            ), PRODUCT_COUNTS AS (
            {textwrap.indent(product_indent_cte_string, '    ')}
            )
            SELECT hour_segment, count, dc_product, key_name, email, utm_source
            FROM
                PRODUCT_COUNTS
            ORDER BY hour_segment, dc_product,key_name, email, utm_source;
            """
        ).strip()

    def postcode_searches_by_local_authority(self, start_time: datetime):
        election_period_cte = indent_cte_string(self.election_period_cte(), 12)
        logs_indent_cte_string = indent_cte_string(
            self.logs_cte(start_time), 12
        )
        return textwrap.dedent(
            f"""
            WITH ELECTION_PERIOD AS (
            {textwrap.indent(election_period_cte,'    ')}
            ), LOGS AS (
            {textwrap.indent(logs_indent_cte_string,'    ')}
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

    def postcode_searches_by_constituency(self, start_time: datetime):
        election_period_cte = indent_cte_string(self.election_period_cte(), 12)
        logs_indent_cte_string = indent_cte_string(
            self.logs_cte(start_time), 12
        )
        return textwrap.dedent(
            f"""
            WITH ELECTION_PERIOD AS (
            {textwrap.indent(election_period_cte,'    ')}
            ), LOGS AS (
            {textwrap.indent(logs_indent_cte_string,'    ')}
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
