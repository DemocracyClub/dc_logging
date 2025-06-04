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
)
SELECT
    count(*) as count
FROM
    LOGS
