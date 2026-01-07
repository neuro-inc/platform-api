from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager

import pytest
from neuro_admin_client import AdminClient
from yarl import URL


@asynccontextmanager
async def create_admin_client(
    url: URL, service_token: str
) -> AsyncGenerator[AdminClient]:
    async with AdminClient(base_url=url, service_token=service_token) as client:
        yield client


@pytest.fixture
async def admin_client_factory(
    k8s_admin_url: URL,
) -> AsyncIterator[Callable[[str], Awaitable[AdminClient]]]:
    exit_stack = AsyncExitStack()

    async def _factory(service_token: str) -> AdminClient:
        return await exit_stack.enter_async_context(
            create_admin_client(
                k8s_admin_url,
                service_token,
            )
        )

    async with exit_stack:
        yield _factory
