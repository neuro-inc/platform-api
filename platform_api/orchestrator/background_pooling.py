import asyncio
import logging
from typing import Optional


from .jobs_service import JobsService, JobError

logger = logging.getLogger(__name__)


class JobsStatusPooling:
    def __init__(self, *, jobs_service: JobsService, interval_s: int = 1, loop: Optional[asyncio.AbstractEventLoop] = None):
        self._loop = loop or asyncio.get_event_loop()

        self._jobs_service = jobs_service
        self._interval_s = interval_s

        self._is_active = None
        self._task = None

    async def start(self):
        logger.info('Start jobs status pooling')
        await self._init_task()

    async def __aenter__(self) -> 'JobsStatusPooling':
        await self.start()
        return self

    async def __aexit__(self, *args) -> None:
        await self.stop()

    async def _init_task(self):
        assert not self._is_active
        assert not self._task

        self._is_active = self._loop.create_future()
        self._task = asyncio.ensure_future(self._run(), loop=self._loop)

    async def stop(self):
        logger.info('Stopping jobs status pooling')
        self._is_active.set_result(None)

        assert self._task
        await self._task

        self._task = None
        self._is_active = None

    async def _run(self):
        while not self._is_active.done():
            await self._run_once()
            await self._wait()

    async def _run_once(self):
        try:
            await self._jobs_service.update_jobs_status()
        except Exception:
            logger.exception("exception when trying update jobs status")

    async def _wait(self):
        await asyncio.wait((self._is_active,), loop=self._loop, timeout=self._interval_s)


