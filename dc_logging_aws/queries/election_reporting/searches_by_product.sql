WITH ELECTION_PERIOD AS (
    SELECT *
    FROM "dc-wide-logs"."dc_postcode_searches_table" all_logs
    WHERE "day" >= '{start_of_election_period_day}'
        AND "day" <= '{polling_day}'
        AND all_logs."api_key" != '{updown_api_key}' --updown
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
        "timestamp" >= cast('{start_datetime_utc}' AS timestamp)
        AND "timestamp" <= cast('{end_datetime_utc}' AS timestamp)
        AND "dc_product" != 'WDIV'
    ) OR (
        "timestamp" >= cast('{start_datetime_london}' AS timestamp)
        AND "timestamp" <= cast('{end_datetime_london}' AS timestamp)
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
            JOIN "dc-wide-logs"."ec_api_keys" as api_users ON LOGS."api_key" = api_users."key"
        WHERE dc_product = 'EC_API'
        GROUP BY "dc_product", "key_name", "user_name", "utm_source", "email"
    UNION SELECT
        count(*) AS count, "dc_product", api_users."key_name", api_users."user_name", api_users."email", utm_source
        FROM LOGS
            JOIN "dc-wide-logs"."devs_dc_api_keys" as api_users ON LOGS."api_key" = api_users."key"
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
