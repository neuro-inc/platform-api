import asyncio
import functools
import logging
import sys
from collections.abc import Callable, Coroutine, Iterable
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Any, TypeVar


async def run_and_log_exceptions(
    coros: Coroutine[Any, Any, Any] | Iterable[Coroutine[Any, Any, Any]],
) -> None:
    try:
        # Check is iterable
        iter(coros)  # type: ignore
        tasks = [asyncio.create_task(coro) for coro in coros]  # type: ignore
    except TypeError:
        tasks = [asyncio.create_task(coros)]  # type: ignore
    await asyncio.gather(*tasks, return_exceptions=True)
    for task in tasks:
        exc = task.exception()
        if exc:
            logging.exception(exc, exc_info=exc)


T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


if sys.version_info >= (3, 10):  # noqa: UP036
    from contextlib import aclosing
else:

    class aclosing(AbstractAsyncContextManager[T_co]):  # noqa: N801
        def __init__(self, thing: T_co):
            self.thing = thing

        async def __aenter__(self) -> T_co:
            return self.thing

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> None:
            await self.thing.aclose()  # type: ignore


def asyncgeneratorcontextmanager(
    func: Callable[..., T_co],
) -> Callable[..., AbstractAsyncContextManager[T_co]]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> AbstractAsyncContextManager[T_co]:
        return aclosing(func(*args, **kwargs))  # type: ignore

    return wrapper
