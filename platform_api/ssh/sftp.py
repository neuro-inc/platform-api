import asyncio
import logging
from contextlib import suppress

import asyncssh


logger = logging.getLogger(__name__)


class SFTPServer:
    def __init__(self, orchestrator, channel):
        self._orchestrator = orchestrator
        self._chan = channel

    async def redirect_in(self, src, writer):
        try:
            while True:
                data = await src.read(8096)
                if data:
                    await writer(data)
                else:
                    self.exit_with_signal("TERM")
                    return
        except asyncssh.BreakReceived:
            await self._subproc.close()
            await self.exit_with_signal("INT")
        except asyncssh.SignalReceived as exc:
            await self._subproc.close()
            await self.exit_with_signal(exc.signal)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Redirect input error")
            await self._subproc.close()
            await self.exit_with_signal("KILL")
            raise

    async def redirect_out(self, reader, dst):
        try:
            while True:
                data = await reader()
                dst.write(data)
                await dst.drain()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Redirect output error")
            raise

    def exit_with_signal(
        self, signal, core_dumped=False, msg="", lang=asyncssh.DEFAULT_LANG
    ):
        return self._chan.exit_with_signal(signal, core_dumped, msg, lang)

    async def run(self, stdin, stdout, stderr):
        loop = asyncio.get_event_loop()
        username = self._chan.get_extra_info("username")
        pod_id = username

        try:
            subproc = await self._orchestrator.exec_pod(
                pod_id, "/usr/lib/openssh/sftp-server", tty=False
            )
            self._subproc = subproc
            self._stdin_redirect = loop.create_task(
                self.redirect_in(stdin, subproc.write_stdin)
            )
            self._stdout_redirect = loop.create_task(
                self.redirect_out(subproc.read_stdout, stdout)
            )
            self._stderr_redirect = loop.create_task(
                self.redirect_out(subproc.read_stderr, stderr)
            )

            retcode = await subproc.wait()
            self._chan.exit(retcode)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unhandled error in sftp server")
            raise
        finally:
            await self.cleanup()

    async def terminate(self, sigcode):
        if self._subproc is not None:
            await self._subproc.close()
            self._subproc = None
        await self.cleanup()
        self._chan.exit(sigcode)

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
