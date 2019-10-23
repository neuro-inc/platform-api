import asyncio
import logging
from typing import Any, Optional

import aiohttp
from yarl import URL


logger = logging.getLogger(__name__)


class EnforcePoller:
    def __init__(
        self, platform_api_url: URL, *, token: str, interval_s: int = 1
    ) -> None:
        self._loop = asyncio.get_event_loop()

        self._platform_api_url = platform_api_url
        assert token
        self._headers = {"Authorization": f"Bearer {token}"}
        self._interval_s = interval_s

        self._is_active: Optional[asyncio.Future[None]] = None
        self._task: Optional[asyncio.Future[None]] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        logger.info("Starting enforce polling")
        await self._init_task()

    async def __aenter__(self) -> "EnforcePoller":
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
        logger.info("Stopping enforce polling")
        assert self._is_active is not None
        self._is_active.set_result(None)

        assert self._task
        await self._task

        self._task = None
        self._is_active = None

    async def _run(self) -> None:
        assert self._is_active is not None
        self._session = aiohttp.ClientSession()
        while not self._is_active.done():
            await self._run_once()
            await self._wait()

    async def _run_once(self) -> None:
        try:
            logger.debug("EnforcePoller._run_once() called")
            async with self._session.get(
                self._platform_api_url / "ping", headers=self._headers
            ) as resp:
                resp.raise_for_status()
                logger.info("Platform API available")

        except Exception:
            logger.exception("exception when trying to enforce jobs policies")

    async def _wait(self) -> None:
        assert self._is_active is not None
        await asyncio.wait((self._is_active,), timeout=self._interval_s)
