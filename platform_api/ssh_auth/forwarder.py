import asyncio
import logging
import random
from abc import ABC, abstractmethod


log = logging.getLogger(__name__)

MIN_PORT = 49152
MAX_PORT = 65535


class Forwarder(ABC):
    @abstractmethod
    async def forward(self, job_id: str, job_port: int) -> int:
        pass


class NCForwarder(Forwarder):
    def __init__(self, log_fifo: str) -> None:
        self._log_fifo = log_fifo

    async def forward(self, job_id: str, job_port: int) -> int:
        log.debug(f"Forwarding")
        while True:
            port = random.randint(MIN_PORT, MAX_PORT)
            log.debug(f"Trying port: {port}")
            command = [
                "/usr/sbin/sshd",
                "-D",
                "-e",
                "-f",
                "/etc/ssh/sshd_config_portforward",
                "-o",
                f"PermitOpen={job_id}:{job_port}",
                "-o",
                f"PidFile=/nonexistent/sshd.{port}.pid",
                "-p",
                str(port),
            ]
            proc = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            assert proc.stderr
            line = (await proc.stderr.readline()).decode()
            if "listening" in line:
                break
            log.debug(f"Port {port} is not available")
        log.debug(f"Redirecting input/output")
        command = ["nc", "127.0.0.1", str(port)]
        proc = await asyncio.create_subprocess_exec(*command)
        retcode = await proc.wait()
        return retcode
