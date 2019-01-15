import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List


log = logging.getLogger(__name__)


class Executor(ABC):
    @abstractmethod
    async def exec_in_job(self, job: str, command: List[str]) -> int:
        pass


class KubeCTLExecutor(Executor):
    def __init__(self, tty: bool) -> None:
        self._tty = tty

    async def exec_in_job(self, job: str, command: List[str]) -> int:
        log.debug((f"Executing {command} in {job}"))
        if self._tty:
            kubectl_cmd = ["kubectl", "exec", "-it", job, "--"] + command
        else:
            kubectl_cmd = ["kubectl", "exec", "-i", job, "--"] + command
        log.debug(f"Running kubectl with command: {kubectl_cmd}")
        proc = await asyncio.create_subprocess_exec(*kubectl_cmd)
        retcode = await proc.wait()

        return retcode
