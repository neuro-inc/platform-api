import asyncio
import logging
import pathlib
import signal
from contextlib import suppress
from functools import partial
from typing import Awaitable, List

import asyncssh
from asyncssh.stream import SSHReader, SSHServerSession, SSHStreamSession, SSHWriter

from platform_api.config_factory import EnvironConfigFactory
from platform_api.orchestrator.kube_orchestrator import KubeOrchestrator

from .sftp import SFTPServer


logger = logging.getLogger(__name__)


class SSHServerHandler(asyncssh.SSHServer):
    def __init__(self, orchestrator: KubeOrchestrator) -> None:
        super().__init__()
        self._orchestrator = orchestrator

    def begin_auth(self, username):
        print("LOGIN", username)
        return False  # False for aonymous

    def password_auth_supported(self):
        return True

    async def validate_password(self, username, password):
        # TODO: add validation
        return True

    def session_requested(self):
        return SSHServerSession(self._orchestrator)


class SSHServerSession(SSHStreamSession, SSHServerSession):
    def __init__(self, orchestrator: KubeOrchestrator) -> None:
        super().__init__()
        self._orchestrator = orchestrator
        self._task = None

    def shell_requested(self):
        """Return whether a shell can be requested"""

        return True

    def exec_requested(self, command):
        print("SESSION exec requested", command)
        # command could be 'scp ' for SCP sessions
        return True

    def subsystem_requested(self, subsystem):
        print("SESSION subsystem requested", subsystem)
        # subsystem is either empty or 'sftp'
        return True

    def session_started(self):
        """Start a session for this newly opened server channel"""
        command = self._chan.get_command()

        stdin = SSHReader(self, self._chan)
        stdout = SSHWriter(self, self._chan)
        stderr = SSHWriter(self, self._chan, asyncssh.EXTENDED_DATA_STDERR)

        if self._chan.get_subsystem() == "sftp":
            self._chan.set_encoding(None)
            self._encoding = None

            print("SFTP session")

            sftp = SFTPServer(self._orchestrator, self._chan)
            handler = sftp.run(stdin, stdout, stderr)
        elif command and command.startswith("scp "):
            self._chan.set_encoding(None)
            self._encoding = None
            print("SCP command", command)
            import pdb

            pdb.set_trace()

            handler = run_scp_server(
                self._sftp_factory(self._conn), command, stdin, stdout, stderr
            )
        else:
            print("SHELL session")
            import pdb

            pdb.set_trace()
            handler = self._session_factory(stdin, stdout, stderr)

        self._task = self._conn.create_task(handler, stdin.logger)

    def connection_lost(self, exc):
        print("SERVER CONNECTION LOST", exc)
        if self._task is not None:
            if not self._task.done():
                self._task.cancel()
            self._task = None
        super().connection_lost(exc)

    def break_received(self, msec):
        """Handle an incoming break on the channel"""
        print("Break received", msec)
        self._recv_buf[None].append(asyncssh.BreakReceived(msec))
        self._unblock_read(None)
        return True

    def signal_received(self, signal):
        """Handle an incoming signal on the channel"""

        print("signal received", signal)
        self._recv_buf[None].append(asyncssh.SignalReceived(signal))
        self._unblock_read(None)

    def terminal_size_changed(self, width, height, pixwidth, pixheight):
        """Handle an incoming terminal size change on the channel"""
        print("terminal size changed", width, height, pixwidth, pixheight)

        self._recv_buf[None].append(
            asyncssh.TerminalSizeChanged(width, height, pixwidth, pixheight)
        )
        self._unblock_read(None)


class ShellSession:
    def __init__(self, process, orchestrator):
        self._process = process
        self._orchestrator = orchestrator
        self._subproc = None
        self._stdin_redirect = None
        self._stdout_redirect = None
        self._stderr_redirect = None

    @classmethod
    def run(
        cls, process: asyncssh.SSHServerProcess, orchestrator: KubeOrchestrator
    ) -> Awaitable[None]:
        self = cls(process, orchestrator)
        return self.handle_client()

    async def redirect_in(self, src, writer):
        try:
            while True:
                data = await src.read(8096)
                if data:
                    data = data.encode("utf-8")
                    await writer(data)
                else:
                    self.exit_with_signal(signal.SIGTERM)
                    return
        except asyncssh.BreakReceived:
            await self._subproc.close()
            await self.exit_with_signal(signal.SIGINT)
        except asyncssh.SignalReceived as exc:
            await self._subproc.close()
            await self.exit_with_signal(exc.signal)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Redirect input error")
            await self._subproc.close()
            await self.exit_with_signal(signal.SIGKILL)
            raise

    async def redirect_out(self, reader, dst):
        try:
            while True:
                data = await reader()
                data = data.decode("utf-8")
                dst.write(data)
                await dst.drain()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Redirect output error")
            raise

    async def handle_client(self):
        process = self._process
        username = process.get_extra_info("username")
        pod_id = username
        loop = asyncio.get_event_loop()
        import pdb

        pdb.set_trace()
        try:
            command = process.command
            if command is None:
                command = "sh -i"
            subproc = await self._orchestrator.exec_pod(pod_id, command, tty=True)
            self._subproc = subproc
            self._stdin_redirect = loop.create_task(
                self.redirect_in(process.stdin, subproc.write_stdin)
            )
            self._stdout_redirect = loop.create_task(
                self.redirect_out(subproc.read_stdout, process.stdout)
            )
            self._stderr_redirect = loop.create_task(
                self.redirect_out(subproc.read_stderr, process.stderr)
            )

            retcode = await subproc.wait()
            process.exit(retcode)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unhandled error in ssh server")
            raise
        finally:
            await self.cleanup()

    async def terminate(self, sigcode):
        if self._subproc is not None:
            await self._subproc.close()
            self._subproc = None
        await self.cleanup()
        self._process.exit_with_signal(sigcode)

    async def cleanup(self):
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


class SSHServer:
    def __init__(self, host: str, port: int, orchestrator: KubeOrchestrator) -> None:
        self._orchestrator = orchestrator
        self._host = host
        self._port = port
        self._server = None
        self._ssh_host_keys: List[str] = []
        here = pathlib.Path(__file__).parent
        self._ssh_host_keys.append(str(here / "ssh_host_dsa_key"))
        self._ssh_host_keys.append(str(here / "ssh_host_rsa_key"))

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    async def start(self):
        self._server = await asyncssh.create_server(
            partial(SSHServerHandler, self._orchestrator),
            self._host,
            self._port,
            server_host_keys=self._ssh_host_keys,
            # process_factory=partial(ShellSession.run, orchestrator=self._orchestrator),
            # sftp_factory=partial(SFTPServer.__call__, orchestrator=self._orchestrator),
            # allow_scp=True,
        )
        address = self._server.sockets[0].getsockname()
        self._host, self._port = address

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()


def init_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


async def run():
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)

    logger.info("Initializing Orchestrator")
    async with KubeOrchestrator(config=config.orchestrator) as orchestrator:
        srv = SSHServer("localhost", 8022, orchestrator)
        await srv.start()
        print("Start SSH server on localhost:8022")
        while True:
            await asyncio.sleep(3600)


async def main():
    init_logging()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())


if __name__ == "__main__":
    main()
