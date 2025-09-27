import asyncio
import traceback
import typing
from concurrent.futures import (
    Future,
)

# TODO: AsyncUtils


async def log_exceptions(
    awaitable: typing.Awaitable,
) -> (
    typing.Any  # TODO: is type correct?
):
    assert (  # TODO: remove
        awaitable is not None
    ), None

    try:
        return await awaitable
    except Exception as exception:
        print(
            f'Unhandled exception: {"".join(traceback.format_exception(exception))}',
        )

        raise exception


def create_task_with_exceptions_logging(
    coroutine: typing.Coroutine,
    name: str | None = None,
) -> asyncio.Task:
    return asyncio.create_task(
        log_exceptions(
            coroutine,
        ),
        name=name,
    )


def run_coroutine_threadsafe_with_exceptions_logging(
    coroutine: typing.Coroutine,
    event_loop: asyncio.AbstractEventLoop,
) -> Future:
    return asyncio.run_coroutine_threadsafe(
        log_exceptions(
            coroutine,
        ),
        event_loop,
    )


def create_task_with_exceptions_logging_threadsafe(
    coroutine: typing.Coroutine,
) -> Future:
    event_loop = asyncio.get_running_loop()

    return run_coroutine_threadsafe_with_exceptions_logging(
        coroutine,
        event_loop,
    )
