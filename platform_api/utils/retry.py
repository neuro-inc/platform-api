import asyncio
import logging
from collections.abc import Callable, Iterator
from typing import Any


log = logging.getLogger(__name__)


class retries:
    def __init__(
        self,
        msg: str,
        catch: tuple[type[Exception], ...],
        attempts: int = 10,
        logger: Callable[[str], None] = log.info,
    ) -> None:
        self._msg = msg
        self._attempts = attempts
        self._logger = logger
        self._catch = catch
        self.reset()

    def reset(self) -> None:
        self._attempt = 0
        self._sleeptime = 0.0

    def __iter__(self) -> Iterator["retries"]:
        while self._attempt < self._attempts:
            self._sleeptime += 0.1
            self._attempt += 1
            yield self

    async def __aenter__(self) -> None:
        pass

    async def __aexit__(
        self, type: type[BaseException], value: BaseException, tb: Any
    ) -> bool:
        if type is None:
            # Stop iteration
            self._attempt = self._attempts
        elif issubclass(type, self._catch) and self._attempt < self._attempts:
            self._logger(f"{self._msg}: {value}.  Retry...")
            await asyncio.sleep(self._sleeptime)
            return True
        return False
