import asyncio
import logging
from typing import Any, Awaitable, Iterable


async def run_and_log_exceptions(coros: Iterable[Awaitable[Any]]) -> None:
    tasks = [asyncio.create_task(coro) for coro in coros]
    await asyncio.gather(*tasks, return_exceptions=True)
    for task in tasks:
        if task.exception():
            logging.exception(task.exception())
