import pytest

from tests.conftest import not_raises


def test_not_raises_success() -> None:
    with pytest.raises(AttributeError, match="!"):
        with not_raises(ValueError):
            raise AttributeError("!")


def test_not_raises_failure() -> None:
    with pytest.raises(pytest.fail.Exception):
        with not_raises(ValueError):
            raise ValueError("!")
