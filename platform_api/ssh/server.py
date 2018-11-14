import asyncio
import logging
import pathlib
import shlex
import signal
import subprocess
import traceback
from contextlib import suppress
from functools import partial
from typing import Awaitable

import asyncssh

from platform_api.config_factory import EnvironConfigFactory
from platform_api.orchestrator.kube_orchestrator import KubeOrchestrator


logger = logging.getLogger(__name__)


class SSHServerHandler(asyncssh.SSHServer):
    def __init__(self, orchestrator: KubeOrchestrator) -> None:
        self._orchestrator = orchestrator

    def begin_auth(self, username):
        print("Begin auth")
        return False  # False for aonymous

    def password_auth_supported(self):
        print("Password auth supported")
        return True

    async def validate_password(self, username, password):
        # TODO: add validation
        print("Validate", username, password)
        return True


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
                    print("STDIN close")
                    await self.terminate(signal.SIG_KILL)
                    raise
        except asyncssh.BreakReceived:
            await self.terminate(signal.SIG_INT)
        except asyncssh.SignalReceived as exc:
            await self.terminate(exc.signal)
        except BaseException:
            print("Exc in redirect")
            traceback.print_exc()
            await self.terminate(signal.SIG_KILL)
            raise

    async def redirect_out(self, reader, dst):
        try:
            while True:
                print('BEFORE READ from OUT')
                data = await reader()
                print('READ DATA', data)
                data = data.decode("utf-8")
                dst.write(data)
                await dst.drain()
        except BaseException:
            print("Exc in redirect")
            traceback.print_exc()
            raise

    async def handle_client(self):
        process = self._process
        username = process.get_extra_info("username")
        print(username)
        pod_id = username
        loop = asyncio.get_event_loop()
        try:
            # process.stdout.write(
            #     "Welcome to my SSH server, %s!\n" % process.get_extra_info("username")
            # )
            command = process.command
            if command is None:
                command = "sh -i"
            print("Command", command)
            print("Process", process.subsystem)
            print("Terminal", process.get_terminal_type())
            subproc = await self._orchestrator.exec_pod(pod_id, command)
            self._subproc = subproc
            print("Redirect")
            self._stdin_redirect = loop.create_task(
                self.redirect_in(process.stdin, subproc.write_stdin)
            )
            self._stdout_redirect = loop.create_task(
                self.redirect_out(subproc.read_stdout, process.stdout)
            )
            self._stderr_redirect = loop.create_task(
                self.redirect_out(subproc.read_stderr, process.stderr)
            )

            print("Wait")
            retcode = await subproc.wait()
            print("Exited", retcode)
            process.exit(retcode)
        except BaseException:
            print("Exc in handle_client")
            traceback.print_exc()
            raise
        finally:
            self.cleanup()

    async def terminate(self, sigcode):
        if self._subproc is not None:
            if self._subproc.retcode is None:
                self._subproc.kill(sigcode)
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
            self._stdout_redirect.cancel()
            with suppress(asyncio.CancelledError):
                await self._stdout_redirect
            self._stdout_redirect = None
        if self._stderr_redirect is not None:
            self._stderr_redirect.cancel()
            with suppress(asyncio.CancelledError):
                await self._stderr_redirect
            self._stderr_redirect = None


class SSHServer:
    def __init__(self, host: str, port: int, orchestrator: KubeOrchestrator) -> None:
        self._orchestrator = orchestrator
        self._host = host
        self._port = port
        self._server = None
        self._ssh_host_keys = []
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
            process_factory=partial(ShellSession.run, orchestrator=self._orchestrator),
            sftp_factory=True,
            allow_scp=True,
        )
        address = self._server.sockets[0].getsockname()
        self._host, self._port = address
        # server_host_keys=['ssh_host_key'],
        # process_factory=handle_client)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()


def init_logging():
    logging.basicConfig(
        # TODO (A Danshyn 06/01/18): expose in the Config
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
