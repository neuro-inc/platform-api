import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


@contextmanager
def log_time(lvl: int, msg: str, *args: Any, **kwargs: Any) -> Iterator[None]:
    start_time_s = time.monotonic()
    yield
    delta_ms = (time.monotonic() - start_time_s) * 1000.0
    msg += f" [{delta_ms:.3f} ms]"
    logger.log(lvl, msg, *args, **kwargs)


@contextmanager
def log_debug_time(msg: str, *args: Any, **kwargs: Any) -> Iterator[None]:
    with log_time(logging.DEBUG, msg, *args, **kwargs):
        yield
