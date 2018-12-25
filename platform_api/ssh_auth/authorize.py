import logging
import os

from platform_api.config_factory import EnvironConfigFactory

from .proxy import (
    AuthenticationError,
    AuthorizationError,
    ExecProxy,
    IllegalArgumentError,
)


log = logging.getLogger(__name__)

ILLEGAL_ARGUMENT = 1
UNAUTHORIZED = 11
PERMISSION_DENIED = 12
UNKNOWN = 100


def run() -> None:
    json_request = os.environ.get("SSH_ORIGINAL_COMMAND", "")
    log.debug(f"SSH_ORIGINAL_COMMAND={json_request}")
    if os.environ.get("NP_TTY", "0") == "1":
        tty = True
    else:
        tty = False
    log.debug(f"TTY is {tty}")
    proxy = ExecProxy(EnvironConfigFactory().create(), tty)
    exit(proxy.process(json_request))


def init_logging() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main() -> None:
    init_logging()
    try:
        run()
        log.debug("Successfull")
    except AuthenticationError as error:
        print("Unauthorized")
        log.error(f"{type(error)}:{error}")
        exit(UNAUTHORIZED)
    except AuthorizationError as error:
        print(f"Permission denied")
        log.error(f"{type(error)}:{error}")
        exit(PERMISSION_DENIED)
    except IllegalArgumentError as error:
        print(f"{error}")
        log.error(f"{type(error)}:{error}")
        exit(ILLEGAL_ARGUMENT)
    except Exception as error:
        print("Unknown exception")
        log.error(f"{type(error)}:{error}")
        exit(UNKNOWN)


if __name__ == "__main__":
    main()
