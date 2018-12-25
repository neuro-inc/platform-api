import logging
import os
import sys

from platform_api.config_factory import EnvironConfigFactory

from .proxy import (
    AuthenticationError,
    AuthorizationError,
    ExecProxy,
    IllegalArgumentError,
)


log = logging.getLogger(__name__)


def run() -> int:
    json_request = os.environ.get("SSH_ORIGINAL_COMMAND", "")
    log.info(f"Request: {json_request}")
    if os.environ.get("NP_TTY", "0") == "1":
        tty = True
    else:
        tty = False
    log.info(f"TTY is {tty}")
    proxy = ExecProxy(EnvironConfigFactory().create(), tty)
    retcode = proxy.process(json_request)
    log.info(f"Done, retcode={retcode}")
    return retcode


def init_logging() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main() -> None:
    init_logging()
    try:
        sys.exit(run())
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
