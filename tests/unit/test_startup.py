import asyncio
from collections.abc import Coroutine
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import aiohttp.web
import pytest

from platform_api import api, poller_main


@pytest.mark.parametrize("entrypoint", [api, poller_main])
def test_main_without_current_event_loop(
    monkeypatch: pytest.MonkeyPatch, entrypoint: ModuleType
) -> None:
    config = SimpleNamespace(server=SimpleNamespace(host="127.0.0.1", port=8080))
    factory = Mock()
    factory.create.return_value = config
    factory.create_poller.return_value = config
    monkeypatch.setattr(entrypoint, "EnvironConfigFactory", lambda: factory)
    monkeypatch.setattr(entrypoint, "init_logging", Mock())
    monkeypatch.setattr(entrypoint, "setup_sentry", Mock())
    created = False

    async def create_app(received: object) -> aiohttp.web.Application:
        nonlocal created
        assert received is config
        asyncio.get_running_loop()
        created = True
        return aiohttp.web.Application()

    def run_app(
        app: Coroutine[None, None, aiohttp.web.Application], *, host: str, port: int
    ) -> None:
        assert not created
        assert (host, port) == ("127.0.0.1", 8080)
        asyncio.run(app)

    monkeypatch.setattr(entrypoint, "create_app", create_app)
    monkeypatch.setattr(entrypoint.aiohttp.web, "run_app", run_app)
    try:
        previous_loop = asyncio.get_event_loop()
    except RuntimeError:
        previous_loop = None
    asyncio.set_event_loop(None)
    try:
        entrypoint.main()
        assert created
    finally:
        asyncio.set_event_loop(previous_loop)
