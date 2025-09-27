import typing
from datetime import (
    timedelta,
)


class CommonConstants:
    __slots__ = ()

    # A coroutine function with any args returning any awaitable result
    AsyncFunctionType = typing.Callable[
        ...,  # arbitrary positional/keyword parameters
        typing.Awaitable[typing.Any],
    ]

    IntervalDurationByNameMap = {
        '1s': timedelta(
            seconds=1,
        ),
        '5s': timedelta(
            seconds=5,
        ),
        '15s': timedelta(
            seconds=15,
        ),
        '30s': timedelta(
            seconds=30,
        ),
        '1m': timedelta(
            minutes=1,
        ),
        '5m': timedelta(
            minutes=5,
        ),
        '15m': timedelta(
            minutes=15,
        ),
        '30m': timedelta(
            minutes=30,
        ),
        '1H': timedelta(
            hours=1,
        ),
        '4H': timedelta(
            hours=4,
        ),
        '1D': timedelta(
            days=1,
        ),
    }
