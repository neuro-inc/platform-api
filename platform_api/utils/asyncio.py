import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from typing import Any, AsyncGenerator, AsyncIterator, Awaitable, Iterable, TypeVar


async def run_and_log_exceptions(coros: Iterable[Awaitable[Any]]) -> None:
    tasks = [asyncio.create_task(coro) for coro in coros]
    await asyncio.gather(*tasks, return_exceptions=True)
    for task in tasks:
        if task.exception():
            logging.exception(task.exception())


T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


@asynccontextmanager
async def auto_close_aiter(
    gen: AsyncGenerator[T_co, T_contra]
) -> AsyncIterator[AsyncGenerator[T_co, T_contra]]:
    try:
        yield gen
    finally:
        with suppress(StopIteration):
            await gen.aclose()
