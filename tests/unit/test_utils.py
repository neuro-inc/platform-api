import pytest
from _pytest.logging import LogCaptureFixture

from platform_api.utils.asyncio import run_and_log_exceptions


@pytest.mark.asyncio
async def test_log_exceptions(caplog: LogCaptureFixture) -> None:
    async def failing_func_name() -> None:
        raise Exception("Foo")

    await run_and_log_exceptions([failing_func_name()])

    assert "failing_func_name" in caplog.text
