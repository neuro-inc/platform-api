import asyncio
import logging
from typing import Any, Optional

import aiohttp

from platform_api.config import JobPolicyEnforcerConfig


logger = logging.getLogger(__name__)


class JobPolicyEnforcer:
    def __init__(self, config: JobPolicyEnforcerConfig) -> None:
        self._loop = asyncio.get_event_loop()

        self._platform_api_url = config.platform_api_url
        self._headers = {"Authorization": f"Bearer {config.token}"}
        self._interval_sec = config.interval_sec

        self._is_active: Optional[asyncio.Future[None]] = None
        self._task: Optional[asyncio.Future[None]] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        logger.info("Starting enforce polling")
        await self._init_task()

    async def __aenter__(self) -> "JobPolicyEnforcer":
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
        print("Stopping JobPolicyEnforcer")
        logger.info("Stopping JobPolicyEnforcer")

        assert self._is_active is not None
        self._is_active.set_result(None)

        assert self._task is not None
        await self._task

        assert self._session is not None
        await self._session.close()

        self._is_active = None
        self._task = None
        self._session = None

    async def _run(self) -> None:
        assert self._is_active is not None
        assert self._loop is not None
        self._session = aiohttp.ClientSession(loop=self._loop)
        while not self._is_active.done():
            await self._run_once()
            await self._wait()

    async def _run_once(self) -> None:
        assert self._session is not None
        try:
            logger.debug("JobPolicyEnforcer._run_once() called")
            async with self._session.get(
                self._platform_api_url / "ping", headers=self._headers
            ) as resp:
                assert resp.status == 200
                logger.debug("Platform API available")
        except Exception:
            logger.exception("Exception when trying to enforce jobs policies")

    async def _wait(self) -> None:
        assert self._is_active is not None
        await asyncio.wait((self._is_active,), timeout=self._interval_sec)
