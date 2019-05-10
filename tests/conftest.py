import os
from contextlib import contextmanager
from typing import Any, Iterator, Type
from uuid import uuid1

import pytest


@contextmanager
def not_raises(exc_cls: Type[Exception]) -> Iterator[None]:
    try:
        yield
    except exc_cls as exc:
        pytest.fail(f"DID RAISE {exc}")


def pytest_configure(config: Any) -> None:
    pytest.not_raises = not_raises


def random_str() -> str:
    return str(uuid1())[:8]


def read_file_binary(ca_path: str) -> bytes:
    try:
        with open(ca_path, "rb") as f:
            return f.read()
    except IOError as e:
        raise IOError(
            f"Could not read CA file '{os.path.abspath(ca_path)}': {e}"
        ) from e
