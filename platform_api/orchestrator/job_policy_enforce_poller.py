import asyncio
import logging
from typing import Any, Optional

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job_policy_enforcer import JobPolicyEnforcer


logger = logging.getLogger(__name__)


class JobPolicyEnforcePoller:
    def __init__(
        self, policy_enforcer: JobPolicyEnforcer, config: JobPolicyEnforcerConfig
    ) -> None:
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self._policy_enforcer = policy_enforcer
        self._config = config

        self._task: Optional[asyncio.Task[None]] = None

    async def __aenter__(self) -> "JobPolicyEnforcePoller":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        logger.info("Starting job policy enforce polling")
        assert self._task is None
        self._task = self._loop.create_task(self._run())

    async def stop(self) -> None:
        logger.info("Stopping job policy enforce polling")
        assert self._task is not None
        self._task.cancel()
        self._task = None

    async def _run(self) -> None:
        while True:
            start = self._loop.time()
            await self._run_once()
            elapsed = self._loop.time() - start
            delay = self._config.interval_sec - elapsed
            if delay < 0:
                delay = 0
            await asyncio.sleep(delay)

    async def _run_once(self) -> None:
        try:
            await self._policy_enforcer.enforce()
        except asyncio.CancelledError:
            raise
        except BaseException:
            logger.exception("Exception when trying to enforce jobs policies")
