import asyncio
import asyncssh
import pathlib
import shlex
import subprocess
import traceback


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


async def redirect(src, dst):
    try:
        while True:
            data = await src.read(8096)
            if data:
                print('data', data)
                if isinstance(data, str):
                    data = data.encode('utf-8')
                else:
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

async def handle_client(process):
    loop = asyncio.get_event_loop()
    try:
        process.stdout.write('Welcome to my SSH server, %s!\n' %
                             process.get_extra_info('username'))
        command = process.command
        if command is None:
            command = 'sh'
        print('Command', command)
        print('Process', process.subsystem)
        params = shlex.split(command)
        proc = await asyncio.create_subprocess_exec(*params,
                                                    stdin=subprocess.PIPE,
                                                    stdout=subprocess.PIPE,
                                                    stderr=subprocess.PIPE)
        print('Redirect')
        stdin_redirect = loop.create_task(redirect(process.stdin, proc.stdin))
        stdout_redirect = loop.create_task(redirect(proc.stdout, process.stdout))
        stderr_redirect = loop.create_task(redirect(proc.stderr, process.stderr))

        # stdin = proc.stdin.get_extra_info('pipe')
        # stdout = proc.stdout._transport.get_extra_info('pipe')
        # stderr = proc.stderr._transport.get_extra_info('pipe')
        # process.redirect(stdin=stdin, stdout=stdout, stderr=stderr)

        print('Wait')
        retcode = await proc.wait()
        print('Exited', retcode)
        process.exit(retcode)
    except BaseException:
        print("Exc in handle_client")
        traceback.print_exc()
        raise
    finally:
        stdin_redirect.cancel()
        stdout_redirect.cancel()
        stderr_redirect.cancel()


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
            process_factory=handle_client,
            sftp_factory=True,
            allow_scp=True)
        # server_host_keys=['ssh_host_key'],
        # process_factory=handle_client)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()


def main():
    loop = asyncio.get_event_loop()
    srv = SSHServer('localhost', 8022)
    loop.run_until_complete(srv.start())
    print('Start SSH server on localhost:8022')
    loop.run_forever()
