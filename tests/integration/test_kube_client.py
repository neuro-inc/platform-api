from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest
from apolo_kube_client import (
    KubeClientSelector,
    V1Node,
    V1NodeCondition,
    V1NodeSpec,
    V1NodeStatus,
    V1ObjectMeta,
)

from platform_api.orchestrator.kube_client import (
    EventHandler,
    NodeWatcher,
    WatchEvent,
)


class MyNodeEventHandler(EventHandler[V1Node]):
    def __init__(self) -> None:
        self.node_names: list[str] = []
        self._events: dict[str, asyncio.Event] = {}

    async def init(self, nodes: list[V1Node]) -> None:
        self.node_names.extend([p.metadata.name for p in nodes if p.metadata.name])

    async def handle(self, event: WatchEvent[V1Node]) -> None:
        pod_name = event.object.metadata.name
        assert pod_name is not None
        self.node_names.append(pod_name)
        waiter = self._events.get(pod_name)
        if waiter:
            del self._events[pod_name]
            waiter.set()

    async def wait_for_node(self, name: str) -> None:
        if name in self.node_names:
            return
        event = asyncio.Event()
        self._events[name] = event
        await event.wait()


class TestNodeWatcher:
    @pytest.fixture
    def handler(self) -> MyNodeEventHandler:
        return MyNodeEventHandler()

    @pytest.fixture
    async def node_watcher(
        self, kube_client_selector: KubeClientSelector, handler: MyNodeEventHandler
    ) -> AsyncIterator[NodeWatcher]:
        watcher = NodeWatcher(kube_client_selector.host_client)
        watcher.subscribe(handler)
        async with watcher:
            yield watcher

    @pytest.mark.usefixtures("node_watcher")
    async def test_handle(
        self, kube_client_selector: KubeClientSelector, handler: MyNodeEventHandler
    ) -> None:
        assert len(handler.node_names) > 0

        node_name = str(uuid.uuid4())
        try:
            await kube_client_selector.host_client.core_v1.node.create(
                V1Node(
                    metadata=V1ObjectMeta(name=node_name),
                    spec=V1NodeSpec(),
                    status=V1NodeStatus(
                        capacity={"pods": "110", "cpu": "1", "memory": "1024Mi"},
                        conditions=[V1NodeCondition(status="True", type="Ready")],
                    ),
                )
            )

            await asyncio.wait_for(handler.wait_for_node(node_name), 5)

            assert node_name in handler.node_names
        finally:
            await kube_client_selector.host_client.core_v1.node.delete(node_name)

    async def test_subscribe_after_start(
        self, node_watcher: NodeWatcher, handler: MyNodeEventHandler
    ) -> None:
        with pytest.raises(
            Exception, match="Subscription is not possible after watcher start"
        ):
            node_watcher.subscribe(handler)
