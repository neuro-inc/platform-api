import asyncio
import functools
import logging
import sys
from types import TracebackType
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    Iterable,
    Optional,
    Type,
    TypeVar,
)


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

    class aclosing(AsyncContextManager[T_co]):
        def __init__(self, thing: T_co):
            self.thing = thing

        async def __aenter__(self) -> T_co:
            return self.thing

        async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            tb: Optional[TracebackType],
        ) -> None:
            await self.thing.aclose()  # type: ignore


def asyncgeneratorcontextmanager(
    func: Callable[..., T_co]
) -> Callable[..., AsyncContextManager[T_co]]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> AsyncContextManager[T_co]:
        return aclosing(func(*args, **kwargs))

    return wrapper
