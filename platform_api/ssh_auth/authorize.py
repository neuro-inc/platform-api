import asyncio
import logging
import os
import sys

from neuro_auth_client import AuthClient

from platform_api.config_factory import EnvironConfigFactory
from platform_api.orchestrator.jobs_storage import RedisJobsStorage
from platform_api.redis import create_redis_client

from .executor import KubeCTLExecutor
from .proxy import (
    AuthenticationError,
    AuthorizationError,
    ExecProxy,
    IllegalArgumentError,
)


log = logging.getLogger(__name__)


async def run() -> int:
    json_request = os.environ.get("SSH_ORIGINAL_COMMAND", "")
    log.info(f"Request: {json_request}")
    if os.environ.get("NP_TTY", "0") == "1":
        tty = True
    else:
        tty = False
    log.info(f"TTY is {tty}")
    config = EnvironConfigFactory().create()
    async with AuthClient(
        url=config.auth.server_endpoint_url, token=config.auth.service_token
    ) as auth_client:
        async with create_redis_client(config.database.redis) as redis_client:
            jobs_storage = RedisJobsStorage(
                redis_client, orchestrator_config=config.orchestrator
            )
            executor = KubeCTLExecutor(tty)
            proxy = ExecProxy(auth_client, jobs_storage, executor)
            retcode = await proxy.process(json_request)
            log.info(f"Done, retcode={retcode}")
            return retcode


def init_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main() -> None:
    init_logging()
    loop = asyncio.get_event_loop()

    try:
        retcode = loop.run_until_complete(run())
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


if __name__ == "__main__":
    main()
