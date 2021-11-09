import asyncio
import logging
from typing import Any, Callable, Iterator, Tuple, Type


log = logging.getLogger(__name__)


class retries:
    def __init__(
        self,
        msg: str,
        catch: Tuple[Type[Exception], ...],
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
        self, type: Type[BaseException], value: BaseException, tb: Any
    ) -> bool:
        if type is None:
            # Stop iteration
            self._attempt = self._attempts
        elif issubclass(type, self._catch) and self._attempt < self._attempts:
            self._logger(f"{self._msg}: {value}.  Retry...")
            await asyncio.sleep(self._sleeptime)
            return True
        return False
