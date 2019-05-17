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
    def __init__(self, tty: bool, jobs_namespace: str) -> None:
        self._tty = tty
        self._jobs_namespace = jobs_namespace

    async def exec_in_job(self, job: str, command: List[str]) -> int:
        log.debug(f"Executing {command} in {job}")
        kubectl_exec = ["kubectl", "exec", "--namespace", self._jobs_namespace]
        if self._tty:
            kubectl_cmd = kubectl_exec + ["-it", job, "--"] + command
        else:
            kubectl_cmd = kubectl_exec + ["-i", job, "--"] + command
        log.debug(f"Running kubectl with command: {kubectl_cmd}")
        proc = await asyncio.create_subprocess_exec(*kubectl_cmd)
        retcode = await proc.wait()

        return retcode
