import pytest


def test_not_raises_success():
    with pytest.raises(AttributeError, match="!"):
        with pytest.not_raises(ValueError):
            raise AttributeError("!")


def test_not_raises_failure():
    with pytest.raises(pytest.fail.Exception):
        with pytest.not_raises(ValueError):
            raise ValueError("!")
