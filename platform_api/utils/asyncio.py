import asyncio
import logging
from typing import Any, Coroutine, Iterable


async def run_and_log_exceptions(coros: Iterable[Coroutine[Any, Any, Any]]) -> None:
    tasks = [asyncio.create_task(coro) for coro in coros]
    await asyncio.gather(*tasks, return_exceptions=True)
    for task in tasks:
        if task.exception():
            logging.exception(task.exception())
