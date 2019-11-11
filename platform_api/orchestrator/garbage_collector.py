import asyncio
import logging
from datetime import timedelta
from typing import Any, AsyncIterator, Iterable, Optional

import aiohttp

from platform_api.config import GarbageCollectorConfig

from .job import JobRecord
from .kube_orchestrator import KubeOrchestrator


logger = logging.getLogger(__name__)


class GarbageCollectorPoller:
    def __init__(
        self, *, config: GarbageCollectorConfig, orchestrator: KubeOrchestrator,
    ) -> None:
        self._loop = asyncio.get_event_loop()

        self._config = config
        self._orchestrator = orchestrator
        self._deletion_delay = timedelta(seconds=config.deletion_delay_s)
        self._interval_s = config.interval_s

        self._is_active: Optional[asyncio.Future[None]] = None
        self._task: Optional[asyncio.Future[None]] = None

        self._platform_api_url = config.platform_api_url
        headers = {"Authorization": f"Bearer {config.token}"}
        self._session = aiohttp.ClientSession(headers=headers)

    async def start(self) -> None:
        logger.info("Starting garbage collector")
        await self._init_task()

    async def __aenter__(self) -> "GarbageCollectorPoller":
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
        logger.info("Stopping garbage collector")
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
            await self._collect_cluster_resources()
        except Exception:
            logger.exception("exception when trying collect resources")

    async def _wait(self) -> None:
        assert self._is_active is not None
        await asyncio.wait((self._is_active,), timeout=self._interval_s)

    async def _get_finished_jobs_by_ids(
        self, job_ids: Iterable[str]
    ) -> AsyncIterator[JobRecord]:
        # TODO (S Storchaka): Implement filtering by status and id:
        # .../jobs?status=succeeded&status=failed&id=...&id=...
        for job_id in job_ids:
            async with self._session.get(
                f"{self._platform_api_url}/jobs?status=succeeded&status=failed"
            ) as resp:
                resp.raise_for_status()
                record = JobRecord.from_primitive(await resp.json())
                if not record.is_finished:
                    yield record

    async def _should_be_collected(self, job_ids: Iterable[str]) -> AsyncIterator[str]:
        async for record in self._get_finished_jobs_by_ids(job_ids):
            if record.should_be_collected(delay=self._deletion_delay):
                yield record.id

    async def _collect_cluster_resources(self) -> None:
        await self._orchestrator.cleanup(should_be_collected=self._should_be_collected)
