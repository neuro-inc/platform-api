import asyncio
import asyncssh
import pathlib
import shlex
import subprocess
import traceback
import signal
import logging

from contextlib import suppress


class SSHServerHandler(asyncssh.SSHServer):
    def begin_auth(self, username):
        print('Begin auth')
        return False  # False for aonymous

    def password_auth_supported(self):
        print('Password auth supported')
        return True

    async def validate_password(self, username, password):
        # TODO: add validation
        print('Validate', username, password)
        return True


class ShellSession:
    def __init__(self, process):
        self._process = process
        self._subproc = None
        self._stdin_redirect = None
        self._stdout_redirect = None
        self._stderr_redirect = None

    @classmethod
    def run(cls, process):
        self = cls(process)
        return self.handle_client()

    async def redirect_in(self, src, dst):
        try:
            while True:
                data = await src.read(8096)
                if data:
                    data = data.encode('utf-8')
                    dst.write(data)
                    await dst.drain()
                else:
                    print('close')
                    dst.close()
                    break
        except asyncssh.BreakReceived:
            await self.terminate(signal.SIG_INT)
        except asyncssh.SignalReceived as exc:
            await self.terminate(exc.signal)
        except BaseException:
            print("Exc in redirect")
            traceback.print_exc()
            await self.terminate(signal.SIG_KILL)
            raise

    async def redirect_out(self, src, dst):
        try:
            while True:
                data = await src.read(8096)
                if data:
                    data = data.decode('utf-8')
                    dst.write(data)
                    await dst.drain()
                else:
                    print('close')
                    dst.close()
                    break
        except BaseException:
            print("Exc in redirect")
            traceback.print_exc()
            raise

    async def handle_client(self):
        process = self._process
        loop = asyncio.get_event_loop()
        try:
            process.stdout.write('Welcome to my SSH server, %s!\n' %
                                 process.get_extra_info('username'))
            command = process.command
            if command is None:
                command = 'sh -i'
            print('Command', command)
            print('Process', process.subsystem)
            print("Terminal", process.get_terminal_type())
            params = shlex.split(command)
            subproc = await asyncio.create_subprocess_exec(*params,
                                                           stdin=subprocess.PIPE,
                                                           stdout=subprocess.PIPE,
                                                           stderr=subprocess.PIPE)
            self._subproc = subproc
            print('Redirect')
            self._stdin_redirect = loop.create_task(
                self.redirect_in(process.stdin, subproc.stdin))
            self._stdout_redirect = loop.create_task(
                self.redirect_out(subproc.stdout, process.stdout))
            self._stderr_redirect = loop.create_task(
                self.redirect_out(subproc.stderr, process.stderr))

            print('Wait')
            retcode = await subproc.wait()
            print('Exited', retcode)
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
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._handler = SSHServerHandler()
        self._server = None
        self._ssh_host_keys = []
        here = pathlib.Path(__file__).parent
        self._ssh_host_keys.append(str(here / 'ssh_host_dsa_key'))
        self._ssh_host_keys.append(str(here / 'ssh_host_rsa_key'))

    async def start(self):
        self._server = await asyncssh.create_server(
            SSHServerHandler,
            self._host, self._port,
            server_host_keys=self._ssh_host_keys,
            process_factory=ShellSession.run,
            sftp_factory=True,
            allow_scp=True)
        # server_host_keys=['ssh_host_key'],
        # process_factory=handle_client)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()


def main():
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    srv = SSHServer('localhost', 8022)
    loop.run_until_complete(srv.start())
    print('Start SSH server on localhost:8022')
    loop.run_forever()
