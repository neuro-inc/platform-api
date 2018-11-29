import asyncio
import logging
from contextlib import suppress

import asyncssh


logger = logging.getLogger(__name__)


class ShellSession:
    def __init__(self, server, channel):
        self._process = asyncssh.SSHServerProcess(self.handle, None, None)
        self._process._chan = channel
        self._server = server
        self._subproc = None
        self._stdin_redirect = None
        self._stdout_redirect = None
        self._stderr_redirect = None

    def run(self, stdin, stdout, stderr):
        process = self._process
        return process._start_process(stdin, stdout, stderr)

    async def redirect_in(self, src, writer):
        try:
            while True:
                data = await src.read(8096)
                if data:
                    data = data.encode("utf-8")
                    await writer(data)
                else:
                    self._process.exit_with_signal("TERM")
                    return
        except asyncssh.BreakReceived:
            await self._subproc.close()
            await self._process.exit_with_signal("INT")
        except asyncssh.SignalReceived as exc:
            await self._subproc.close()
            await self._process.exit_with_signal(exc.signal)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Redirect input error")
            await self._subproc.close()
            await self._process.exit_with_signal("KILL")
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

    async def handle(self, process):
        username = process.get_extra_info("username")
        pod_id = username
        loop = asyncio.get_event_loop()
        try:
            command = process.command
            if command is None:
                command = "sh -i"
            subproc = await self._server.orchestrator.exec_pod(
                pod_id, command, tty=True
            )
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
