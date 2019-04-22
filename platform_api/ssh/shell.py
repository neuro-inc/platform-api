import asyncio
import logging
import shlex
from contextlib import suppress
from typing import Any, Awaitable, Callable, Dict, Optional

import asyncssh

from platform_api.orchestrator.kube_client import PodExec
from platform_api.ssh.server import SSHServer


logger = logging.getLogger(__name__)


class ShellSession:
    def __init__(self, server: SSHServer, channel: asyncssh.channel.SSHChannel) -> None:
        self._chan = channel
        self._server = server
        self._subproc: Optional[PodExec] = None
        self._stdin_redirect: Optional[asyncio.Task[Any]] = None
        self._stdout_redirect: Optional[asyncio.Task[Any]] = None
        self._stderr_redirect: Optional[asyncio.Task[Any]] = None

    async def redirect_in(
        self, src: asyncssh.SSHReader, writer: Callable[[bytes], Awaitable[None]]
    ) -> None:
        assert self._subproc is not None
        try:
            while True:
                data = await src.read(8096)
                if data:
                    data = data.encode("utf-8")
                    await writer(data)
                else:
                    self.exit_with_signal("TERM")
                    return
        except asyncssh.BreakReceived:
            await self._subproc.close()
            self.exit_with_signal("INT")
        except asyncssh.SignalReceived as exc:
            await self._subproc.close()
            self.exit_with_signal(exc.signal)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Redirect input error")
            await self._subproc.close()
            self.exit_with_signal("KILL")
            return

    async def redirect_out(
        self, reader: Callable[[], Awaitable[bytes]], dst: asyncssh.SSHWriter
    ) -> None:
        try:
            while True:
                data = await reader()
                sdata = data.decode("utf-8")
                dst.write(sdata)
                await dst.drain()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Redirect output error")
            return

    async def run(
        self,
        stdin: asyncssh.SSHReader,
        stdout: asyncssh.SSHWriter,
        stderr: asyncssh.SSHWriter,
    ) -> None:
        username = self.username
        pod_id = username
        loop = asyncio.get_event_loop()
        env = self._chan.get_environment()
        try:
            command_str = self.command
            if command_str is None:
                command = ["sh", "-i"]
            else:
                command = shlex.split(command_str)
            if env:
                lst = ["env"]
                for name, val in env.items():
                    lst.append(name + "=" + shlex.quote(val))
                lst.extend(command)
                command = lst

            subproc = await self._server.orchestrator.exec_pod(
                pod_id, command, tty=True
            )
            self._subproc = subproc
            self._stdin_redirect = loop.create_task(
                self.redirect_in(stdin, subproc.write_stdin)
            )
            self._server.add_cleanup(self._stdin_redirect)
            self._stdout_redirect = loop.create_task(
                self.redirect_out(subproc.read_stdout, stdout)
            )
            self._server.add_cleanup(self._stdout_redirect)
            self._stderr_redirect = loop.create_task(
                self.redirect_out(subproc.read_stderr, stderr)
            )
            self._server.add_cleanup(self._stderr_redirect)

            retcode = await subproc.wait()
            self.exit(retcode)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unhandled error in ssh server")
            raise
        finally:
            if self._subproc is not None:
                await self._subproc.close()
            await self.cleanup()

    def exit(self, status: int) -> None:
        self._chan.exit(status)

    def exit_with_signal(
        self,
        signal: str,
        core_dumped: bool = False,
        msg: str = "",
        lang: str = asyncssh.DEFAULT_LANG,
    ) -> None:
        return self._chan.exit_with_signal(signal, core_dumped, msg, lang)

    @property
    def username(self) -> str:
        return self._chan.get_extra_info("username")

    @property
    def env(self) -> Dict[str, str]:
        return self._chan.get_environment()

    @property
    def command(self) -> Optional[str]:
        return self._chan.get_command()

    @property
    def subsystem(self) -> Optional[str]:
        return self._chan.get_subsystem()

    async def cleanup(self) -> None:
        if self._stdin_redirect is not None:
            self._stdin_redirect.cancel()
            with suppress(asyncio.CancelledError):
                await self._stdin_redirect
            self._stdin_redirect = None
        if self._stdout_redirect is not None:
            with suppress(asyncio.CancelledError):
                await self._stdout_redirect
            self._stdout_redirect = None
        if self._stderr_redirect is not None:
            with suppress(asyncio.CancelledError):
                await self._stderr_redirect
            self._stderr_redirect = None
