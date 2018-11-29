import asyncio
import logging
from contextlib import suppress

import asyncssh


logger = logging.getLogger(__name__)


class ShellSession:
    def __init__(self, server, channel):
        self._chan = channel
        self._server = server
        self._subproc = None
        self._stdin_redirect = None
        self._stdout_redirect = None
        self._stderr_redirect = None

    async def redirect_in(self, src, writer):
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

    async def redirect_out(self, reader, dst):
        try:
            while True:
                data = await reader()
                data = data.decode("utf-8")
                dst.write(data)
                await dst.drain()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Redirect output error")
            return

    async def run(self, stdin, stdout, stderr):
        username = self.username
        print('USERNAME', username)
        pod_id = username
        loop = asyncio.get_event_loop()
        try:
            command = self.command
            if command is None:
                command = "sh -i"
            print('COMMAND', command)
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

    def exit(self, status):
        self._chan.exit(status)

    def exit_with_signal(
        self, signal, core_dumped=False, msg="", lang=asyncssh.DEFAULT_LANG
    ):
        return self._chan.exit_with_signal(signal, core_dumped, msg, lang)

    @property
    def username(self):
        return self._chan.get_extra_info('username')

    @property
    def env(self):
        return self._chan.get_environment()

    @property
    def command(self):
        return self._chan.get_command()

    @property
    def subsystem(self):
        return self._chan.get_subsystem()

    async def cleanup(self):
        print("PROC CLEANUP")
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
