import asyncio
import logging
import os
import sys
from typing import IO

from neuro_auth_client import AuthClient

from platform_api.config_factory import EnvironConfigFactory, SSHAuthConfig

from .executor import KubeCTLExecutor
from .forwarder import NCForwarder
from .proxy import (
    AuthenticationError,
    AuthorizationError,
    ExecProxy,
    IllegalArgumentError,
)


log = logging.getLogger(__name__)


async def run(config: SSHAuthConfig) -> int:
    json_request = os.environ.get("SSH_ORIGINAL_COMMAND", "")
    log.info(f"Request: {json_request}")
    if os.environ.get("NP_TTY", "0") == "1":
        tty = True
    else:
        tty = False
    log.info(f"TTY is {tty}")

    async with AuthClient(
        url=config.auth.server_endpoint_url, token=config.auth.service_token
    ) as auth_client:
        forwarder = NCForwarder(config.log_fifo)
        executor = KubeCTLExecutor(tty)
        proxy = ExecProxy(
            auth_client=auth_client,
            platform_url=config.platform.server_endpoint_url,
            executor=executor,
            forwarder=forwarder,
        )
        retcode = await proxy.process(json_request)
        log.info(f"Done, retcode={retcode}")
        return retcode


def init_logging(pipe: IO[str]) -> None:
    logging.basicConfig(
        stream=pipe,
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main() -> None:
    config = EnvironConfigFactory().create_ssh_auth()
    with config.log_fifo.open(mode="w") as pipe:
        init_logging(pipe)
        loop = asyncio.get_event_loop()
        try:
            retcode = loop.run_until_complete(run(config))
            sys.exit(retcode)
        except AuthenticationError as error:
            print("Unauthorized")
            log.error(f"{type(error)}:{error}")
            sys.exit(os.EX_NOPERM)
        except AuthorizationError as error:
            print(f"Permission denied")
            log.error(f"{type(error)}:{error}")
            sys.exit(os.EX_NOPERM)
        except IllegalArgumentError as error:
            print(f"{error}")
            log.error(f"{type(error)}:{error}")
            sys.exit(os.EX_DATAERR)
        except Exception as error:
            print("Unknown exception")
            log.error(f"{type(error)}:{error}")
            sys.exit(os.EX_SOFTWARE)
        finally:
            # Suppess error messages, as aiohttp can send a false warning
            # about unclosed stream
            loop.set_exception_handler(lambda loop, context: None)


if __name__ == "__main__":
    main()
