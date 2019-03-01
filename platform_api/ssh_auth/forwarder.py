import asyncio
import logging
import random
from abc import ABC, abstractmethod


log = logging.getLogger(__name__)

MIN_PORT = 49152
MAX_PORT = 65535
MAX_ATTEMPT = 10


class Forwarder(ABC):
    @abstractmethod
    async def forward(self, job_id: str, job_port: int) -> int:
        pass


def try_kill(proc):
    try:
        proc.kill()
    except ProcessLookupError:
        pass


class NCForwarder(Forwarder):
    async def forward(self, job_id: str, job_port: int) -> int:
        log.debug(f"Forwarding")
        for i in range(MAX_ATTEMPT):
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
            ssh_proc = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            assert ssh_proc.stderr
            line = (await ssh_proc.stderr.readline()).decode()
            if "listening" in line:
                break
            try_kill(ssh_proc)
            log.debug(f"Port {port} is not available")
        else:
            raise OSError("No ports are available")
        log.debug(f"Redirecting input/output")
        command = ["nc", "-q", "3", "127.0.0.1", str(port)]
        nc_proc = await asyncio.create_subprocess_exec(*command)
        try:
            await nc_proc.wait()
        finally:
            log.debug(f"Cleanup")
            try_kill(ssh_proc)
            try_kill(nc_proc)
            return 0
