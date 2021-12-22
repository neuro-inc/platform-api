import asyncio
import functools
import logging
import sys
from collections.abc import Awaitable, Callable, Iterable
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Any, Optional, TypeVar


async def run_and_log_exceptions(coros: Iterable[Awaitable[Any]]) -> None:
    tasks = [asyncio.create_task(coro) for coro in coros]
    await asyncio.gather(*tasks, return_exceptions=True)
    for task in tasks:
        if task.exception():
            logging.exception(task.exception())


T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


if sys.version_info >= (3, 10):
    from contextlib import aclosing
else:

    class aclosing(AbstractAsyncContextManager[T_co]):
        def __init__(self, thing: T_co):
            self.thing = thing

        async def __aenter__(self) -> T_co:
            return self.thing

        async def __aexit__(
            self,
            exc_type: Optional[type[BaseException]],
            exc: Optional[BaseException],
            tb: Optional[TracebackType],
        ) -> None:
            await self.thing.aclose()  # type: ignore


def asyncgeneratorcontextmanager(
    func: Callable[..., T_co]
) -> Callable[..., AbstractAsyncContextManager[T_co]]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> AbstractAsyncContextManager[T_co]:
        return aclosing(func(*args, **kwargs))

    return wrapper
