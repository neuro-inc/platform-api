import asyncio
import logging
from typing import Any, Callable, Optional

import aiohttp
import async_timeout

from platform_api.config import JobPolicyEnforcerConfig


logger = logging.getLogger(__name__)


def _nop(*args: Any, **kwargs: Any) -> None:
    pass


class JobPolicyEnforcer:
    def __init__(
        self,
        config: JobPolicyEnforcerConfig,
        exception_handler: Callable[[BaseException], None] = _nop,
    ) -> None:
        self._platform_api_url = config.platform_api_url
        self._headers = {"Authorization": f"Bearer {config.token}"}
        self._interval_sec = config.interval_sec
        self._run_once_timeout_sec = config.run_once_timeout_sec
        self._exception_handler = exception_handler

        self._session = aiohttp.ClientSession()
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        logger.info("Starting enforce polling")
        assert self._task is None

        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._run())

        # forcing execution of the newly created task:
        await asyncio.sleep(0)

    async def stop(self) -> None:
        logger.info("Stopping JobPolicyEnforcer")

        assert self._task is not None
        self._task.cancel()
        self._task = None

        await self._session.close()

    async def _run(self) -> None:
        import sys

        logger.addHandler(logging.StreamHandler(sys.stdout))

        while True:
            await self._run_once()
            await asyncio.sleep(self._interval_sec)

    async def _run_once(self) -> None:
        assert self._session is not None
        try:
            logger.debug("JobPolicyEnforcer._run_once() called")
            with async_timeout.timeout(self._run_once_timeout_sec):
                async with self._session.get(
                    self._platform_api_url / "ping", headers=self._headers
                ) as resp:
                    assert resp.status == 200
                    logger.debug("Platform API available")
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            logger.exception("Exception when trying to enforce jobs policies")
            self._exception_handler(exc)
