from contextlib import contextmanager
from typing import Any, Iterator, Type, Callable
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


@pytest.fixture
def random_str_factory() -> Callable[[], str]:
    def _factory() -> str:
        return str(uuid1())[:8]

    return _factory
