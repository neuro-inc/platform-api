from collections.abc import Iterator
from contextlib import contextmanager
from uuid import uuid1

import pytest

pytest_plugins = [
    "tests.integration.auth",
    "tests.integration.api",
    "tests.integration.docker",
    "tests.integration.secrets",
    "tests.integration.diskapi",
    "tests.integration.notifications",
    "tests.integration.admin",
    "tests.integration.postgres",
]


@contextmanager
def not_raises(exc_cls: type[Exception]) -> Iterator[None]:
    try:
        yield
    except exc_cls as exc:
        pytest.fail(f"DID RAISE {exc}")


def random_str() -> str:
    return str(uuid1())[:8]
