from datetime import datetime

import pytest
from zoneinfo import ZoneInfo

from dc_logging_aws.named_queries.query_template import (
    datetime_to_athena_datetime_string,
)


@pytest.fixture
def test_dates():
    london_tz = ZoneInfo("Europe/London")
    return [
        datetime(2024, 4, 29, 0, 0, tzinfo=london_tz),
        datetime(2024, 5, 2, 22, 0, tzinfo=london_tz),
        datetime(2019, 12, 12, 22, 0, tzinfo=london_tz),
        datetime(2019, 12, 9, 0, 0, tzinfo=london_tz),
    ]


@pytest.fixture
def utc_tz():
    return ZoneInfo("UTC")


@pytest.fixture
def london_tz():
    return ZoneInfo("Europe/London")


@pytest.mark.parametrize(
    "index, expected",
    [
        (0, "2024-04-28 23:00"),  # BST to UTC
        (1, "2024-05-02 21:00"),  # BST to UTC
        (2, "2019-12-12 22:00"),  # GMT to UTC (no change)
        (3, "2019-12-09 00:00"),  # GMT to UTC (no change)
    ],
)
def test_london_to_utc(test_dates, utc_tz, index, expected):
    result = datetime_to_athena_datetime_string(test_dates[index], utc_tz)
    assert result == expected


@pytest.mark.parametrize(
    "index, expected",
    [
        (0, "2024-04-29 00:00"),
        (1, "2024-05-02 22:00"),
        (2, "2019-12-12 22:00"),
        (3, "2019-12-09 00:00"),
    ],
)
def test_london_to_london(test_dates, london_tz, index, expected):
    result = datetime_to_athena_datetime_string(test_dates[index], london_tz)
    assert result == expected


def test_non_tz_aware_input(utc_tz):
    naive_dt = datetime(2024, 4, 29, 0, 0)
    with pytest.raises(ValueError):
        datetime_to_athena_datetime_string(naive_dt, utc_tz)
