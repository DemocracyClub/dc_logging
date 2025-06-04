from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


def handler(event, context):
    """
    The logs are stored on s3. The directory structure is yyyy/mm/dd/hh.
    This means we want dates to be in the format yyyy/mm/dd.
    Athena handles datetime strings in the format 'yyyy-mm-dd hh:mm'
    """
    polling_day_str = event.get("polling_day")
    polling_day = date_from_string(polling_day_str, "polling day")
    start_of_election_period_str = event.get("start_of_election_period", None)

    if start_of_election_period_str is None:
        start_of_election_period = get_election_period_start(polling_day)
    else:
        start_of_election_period = date_from_string(
            start_of_election_period_str, "election period start"
        )

    # 00:00 on date beginning election period
    # calculate this in London time to account for daylight savings.
    start_of_election_period_dt = datetime.combine(
        start_of_election_period, time(0, 0, tzinfo=ZoneInfo("Europe/London"))
    )

    # 22:00 on election day
    # calculate this in London time to account for daylight savings.
    close_of_polls = datetime.combine(
        polling_day,
        time(22, 0, tzinfo=ZoneInfo("Europe/London")),
    )

    # 00:00 on Monday of election week
    # calculate this in London time to account for daylight savings.
    start_of_election_week_dt = datetime.combine(
        get_election_week_start(polling_day),
        time(0, 0, tzinfo=ZoneInfo("Europe/London")),
    )

    # 00:00 On Polling Day
    # calculate this in London time to account for daylight savings.
    start_of_polling_day_dt = datetime.combine(
        polling_day, time(0, 0, tzinfo=ZoneInfo("Europe/London"))
    )

    return {
        "polling_day_athena": polling_day.strftime("%Y/%m/%d"),
        "start_of_election_period_day_athena": start_of_election_period.strftime(
            "%Y/%m/%d"
        ),
        "close_of_polls_utc": utc_athena_time(close_of_polls),
        "close_of_polls_london": london_athena_time(close_of_polls),
        "start_of_election_period_utc": utc_athena_time(
            start_of_election_period_dt
        ),
        "start_of_election_period_london": london_athena_time(
            start_of_election_period_dt
        ),
        "start_of_election_week_utc": utc_athena_time(
            start_of_election_week_dt
        ),
        "start_of_election_week_london": london_athena_time(
            start_of_election_week_dt
        ),
        "start_of_polling_day_utc": utc_athena_time(start_of_polling_day_dt),
        "start_of_polling_day_london": london_athena_time(
            start_of_polling_day_dt
        ),
    }


def date_from_string(date_string: str, string_name: str) -> date:
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(
            f"Invalid {string_name}: {date_string}. Either unexpected format, or date doesn't exist. Format should be YYYY-MM-DD"
        )


def get_election_week_start(polling_day: date) -> date:
    """
    returns date for Monday of week containing polling day

    get_election_week_start(date(2025, 5, 1)) -> datetime.date(2025, 4, 28)
    get_election_week_start(date(2025, 1, 2)) -> datetime.date(2024, 12, 30)
    """
    # This will normally be a thursday (3
    weekday = polling_day.weekday()  # Monday -> 0, Sunday -> 6
    return polling_day - timedelta(days=weekday)


def get_election_period_start(polling_day: date) -> date:
    """
    Returns the first day of the polling_month preceding polling day
    """
    polling_year = polling_day.year
    polling_month = polling_day.month

    if polling_month == 1:  # January
        prev_month = 12
        prev_year = polling_year - 1
    else:
        prev_month = polling_month - 1
        prev_year = polling_year

    return date(prev_year, prev_month, 1)


def utc_athena_time(dt: datetime) -> str:
    return datetime_to_athena_datetime_string(dt, ZoneInfo("UTC"))


def london_athena_time(dt: datetime) -> str:
    return datetime_to_athena_datetime_string(dt, ZoneInfo("Europe/London"))


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
