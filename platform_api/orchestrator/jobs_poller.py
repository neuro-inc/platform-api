import asyncio
import logging
from typing import Any, Optional

from .jobs_service import JobsService


logger = logging.getLogger(__name__)


class JobsPoller:
    def __init__(self, *, jobs_service: JobsService, interval_s: int = 1) -> None:
        self._loop = asyncio.get_event_loop()

        self._jobs_service = jobs_service
        self._interval_s = interval_s

        self._is_active: Optional[asyncio.Future[None]] = None
        self._task: Optional[asyncio.Future[None]] = None

    async def start(self) -> None:
        logger.info("Starting jobs polling")
        await self._init_task()

    async def __aenter__(self) -> "JobsPoller":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def _init_task(self) -> None:
        assert not self._is_active
        assert not self._task

        self._is_active = self._loop.create_future()
        self._task = asyncio.ensure_future(self._run())
        # forcing execution of the newly created task
        await asyncio.sleep(0)

    async def stop(self) -> None:
        logger.info("Stopping jobs polling")
        assert self._is_active is not None
        self._is_active.set_result(None)

        assert self._task
        await self._task

        self._task = None
        self._is_active = None

    async def _run(self) -> None:
        assert self._is_active is not None
        while not self._is_active.done():
            await self._run_once()
            await self._wait()

    async def _run_once(self) -> None:
        try:
            await self._jobs_service.update_jobs_statuses()
        except Exception:
            logger.exception("exception when trying update jobs status")

    async def _wait(self) -> None:
        assert self._is_active is not None
        await asyncio.wait((self._is_active,), timeout=self._interval_s)
