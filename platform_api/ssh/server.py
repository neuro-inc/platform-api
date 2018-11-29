import asyncio
import logging
import pathlib
import weakref
from functools import partial
from typing import List

import asyncssh
from asyncssh.stream import SSHReader, SSHServerSession, SSHStreamSession, SSHWriter

from platform_api.config_factory import EnvironConfigFactory
from platform_api.orchestrator.kube_orchestrator import KubeOrchestrator

from .sftp import SFTPServer
from .shell import ShellSession


logger = logging.getLogger(__name__)


class SSHServerHandler(asyncssh.SSHServer):
    def __init__(self, server: "SSHServer") -> None:
        super().__init__()
        self._server = server

    def begin_auth(self, username):
        print("LOGIN", username)
        return False  # False for aonymous

    def password_auth_supported(self):
        return True

    async def validate_password(self, username, password):
        # TODO: add validation
        return True

    def session_requested(self):
        return SSHServerSession(self._server)


class SSHServerSession(SSHStreamSession, SSHServerSession):
    def __init__(self, server: "SSHServer") -> None:
        super().__init__()
        self._server = server
        self._task = None

    def shell_requested(self):
        """Return whether a shell can be requested"""
        print("SESSION shell requested")

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

            sftp = SFTPServer(self._server, self._chan)
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
            shell = ShellSession(self._server, self._chan)
            handler = shell.run(stdin, stdout, stderr)

        self._task = self._conn.create_task(handler, stdin.logger)
        self._server.register_on_close(self._task)

    def connection_lost(self, exc):
        print("SERVER CONNECTION LOST", exc)
        if self._task is not None:
            if not self._task.done():
                print("CANCEL TASK")
                self._task.cancel()
            self._task = None
        super().connection_lost(exc)

    def eof_received(self):
        print("EOF RECEIVED")
        super().eof_received()

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
        self._waiters = weakref.WeakSet()

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def orchestrator(self):
        return self._orchestrator

    async def start(self):
        self._server = await asyncssh.create_server(
            partial(SSHServerHandler, self),
            self._host,
            self._port,
            server_host_keys=self._ssh_host_keys,
        )
        address = self._server.sockets[0].getsockname()
        self._host, self._port = address

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
        # import pdb;pdb.set_trace()
        print("WAIT FOR", list(self._waiters))
        await asyncio.gather(*list(self._waiters))

    async def _wait(self, coro):
        await coro

    def register_on_close(self, task):
        self._waiters.add(task)


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
