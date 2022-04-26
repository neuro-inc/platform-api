from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Callable, Iterable
from contextlib import suppress
from typing import Any

from .kube_client import (
    KubeClient,
    KubePreemption,
    Node,
    NodeResources,
    NotFoundException,
    PodDescriptor,
    PodEventHandler,
    PodStatus,
    PodWatcher,
    PodWatchEvent,
    WatchEventType,
)

logger = logging.getLogger(__name__)


class _Pod:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload
        self._status = PodStatus(payload["status"])

    @property
    def payload(self) -> dict[str, Any]:
        return self._payload

    @property
    def name(self) -> str:
        return self._payload["metadata"]["name"]

    @property
    def labels(self) -> dict[str, str]:
        return self._payload["metadata"].get("labels", {})

    @property
    def is_idle(self) -> bool:
        return bool(self.labels.get("platform.neuromation.io/idle"))

    @property
    def is_terminating(self) -> bool:
        return bool(self._payload["metadata"].get("deletionTimestamp"))

    @property
    def status(self) -> PodStatus:
        return self._status

    @property
    def node_name(self) -> str:
        return self._payload["spec"]["nodeName"]

    @property
    def resource_requests(self) -> NodeResources:
        pod_resources = NodeResources()
        for container in self._payload["spec"]["containers"]:
            resources = container.get("resources")
            if not resources:
                continue
            requests = resources.get("requests")
            if not requests:
                continue
            pod_resources += NodeResources.from_primitive(requests)
        return pod_resources


class NodeResourcesHandler(PodEventHandler):
    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client
        self._nodes: dict[str, Node] = {}
        self._node_free_resources: dict[str, NodeResources] = {}
        self._pod_node_names: dict[str, str] = {}

    async def init(self, raw_pods: list[dict[str, Any]]) -> None:
        for raw_pod in raw_pods:
            pod = _Pod(raw_pod)
            if pod.status.is_scheduled and not pod.status.is_terminated:
                await self._add_pod(pod)

    async def handle(self, event: PodWatchEvent) -> None:
        pod = _Pod(event.raw_pod)
        if not pod.status.is_scheduled:
            return
        if event.type == WatchEventType.DELETED or pod.status.is_terminated:
            self._remove_pod(pod)
        else:
            await self._add_pod(pod)

    async def _add_pod(self, pod: _Pod) -> None:
        pod_name = pod.name
        if pod_name in self._pod_node_names:
            return
        node_name = pod.node_name
        # Ignore error in case node was removed/lost but pod was not yet removed
        with suppress(NotFoundException):
            if node_name not in self._nodes:
                node = await self._kube_client.get_node(node_name)
                self._nodes[node_name] = node
                self._node_free_resources[node_name] = node.allocatable_resources
            self._pod_node_names[pod_name] = node_name
            self._node_free_resources[node_name] -= pod.resource_requests

    def _remove_pod(self, pod: _Pod) -> None:
        pod_name = pod.name
        if pod_name not in self._pod_node_names:
            return
        node_name = pod.node_name
        node = self._nodes.get(node_name)
        if node:
            node_free_resources = self._node_free_resources[node_name]
            node_free_resources += pod.resource_requests
            if node.allocatable_resources == node_free_resources:
                self._node_free_resources.pop(node_name, None)
                self._nodes.pop(node_name, None)
            else:
                self._node_free_resources[node_name] = node_free_resources
        self._pod_node_names.pop(pod_name, None)

    def get_nodes(self) -> list[Node]:
        return list(self._nodes.values())

    def get_node_free_resources(self, node_name: str) -> NodeResources:
        return self._node_free_resources.get(node_name) or NodeResources()

    def get_pod_node_name(self, pod_name: str) -> str | None:
        """
        Get name of the node which runs pod.
        Return None if pod is not in a Running state.
        """
        return self._pod_node_names.get(pod_name)

    def _get_node_allocatable_resources(self, node_name: str) -> NodeResources:
        node = self._nodes.get(node_name)
        return node.allocatable_resources if node else NodeResources()


class IdlePodsHandler(PodEventHandler):
    def __init__(self) -> None:
        self._pods: dict[str, dict[str, PodDescriptor]] = defaultdict(dict)

    async def init(self, raw_pods: list[dict[str, Any]]) -> None:
        for raw_pod in raw_pods:
            pod = _Pod(raw_pod)
            if pod.is_idle and pod.status.is_scheduled and not pod.status.is_terminated:
                self._add_pod(pod)

    async def handle(self, event: PodWatchEvent) -> None:
        pod = _Pod(event.raw_pod)
        if not pod.is_idle or not pod.status.is_scheduled:
            return
        if event.type == WatchEventType.DELETED or pod.status.is_terminated:
            self._remove_pod(pod)
        else:
            self._add_pod(pod)

    def _add_pod(self, pod: _Pod) -> None:
        pod_name = pod.name
        node_name = pod.node_name
        # there is an issue in k8s, elements in items don't have kind and version
        pod.payload["kind"] = "Pod"
        self._pods[node_name][pod_name] = PodDescriptor.from_primitive(pod.payload)

    def _remove_pod(self, pod: _Pod) -> None:
        node_name = pod.node_name
        pod_name = pod.name
        pods = self._pods[node_name]
        pods.pop(pod_name, None)
        if not pods:
            self._pods.pop(node_name, None)

    def get_pods(self, node_name: str) -> list[PodDescriptor]:
        pods = self._pods.get(node_name)
        return list(pods.values()) if pods else []


class KubeOrchestratorPreemption:
    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client
        self._kube_preemption = KubePreemption()
        self._node_resources_handler = NodeResourcesHandler(kube_client)
        self._idle_pods_handler = IdlePodsHandler()

    def register(self, pod_watcher: PodWatcher) -> None:
        pod_watcher.subscribe(self._node_resources_handler)
        pod_watcher.subscribe(self._idle_pods_handler)

    async def preempt_pods(
        self,
        pods_to_schedule: list[PodDescriptor],
        preemptible_pods: list[PodDescriptor],
    ) -> None:
        preemptible_pods_by_node = defaultdict(list)
        for pod in preemptible_pods:
            node_name = self._node_resources_handler.get_pod_node_name(pod.name)
            if node_name:
                preemptible_pods_by_node[node_name].append(pod)
        await self._preempt_pods(
            pods_to_schedule, get_preemptible_pods=preemptible_pods_by_node.__getitem__
        )

    async def preempt_idle_pods(self, pods_to_schedule: list[PodDescriptor]) -> None:
        await self._preempt_pods(
            pods_to_schedule, get_preemptible_pods=self._idle_pods_handler.get_pods
        )

    async def _preempt_pods(
        self,
        pods_to_schedule: list[PodDescriptor],
        get_preemptible_pods: Callable[[str], list[PodDescriptor]],
    ) -> None:
        nodes_to_preempt: set[Node] = set()
        pods_to_preempt: list[PodDescriptor] = []
        for pod in self._get_pods_to_schedule(pods_to_schedule):
            # Handle one node per api poller iteration.
            # Exclude nodes preempted in previous steps
            # to avoid node resources tracking complexity.
            node, pods = self._get_pods_to_preempt(
                pod,
                get_preemptible_pods=get_preemptible_pods,
                exclude_nodes=nodes_to_preempt,
            )
            if node:
                nodes_to_preempt.add(node)
                pods_to_preempt.extend(pods)
        await self._delete_pods(pods_to_preempt)

    def _get_pods_to_schedule(
        self, pods: Iterable[PodDescriptor]
    ) -> list[PodDescriptor]:
        def _create_key(pod: PodDescriptor) -> tuple[int, int, float]:
            r = pod.resources
            if not r:
                return (0, 0, 0.0)
            return (r.gpu or 0, r.memory, r.cpu)

        pods_to_schedule = []
        for pod in pods:
            if not self._node_resources_handler.get_pod_node_name(pod.name):
                pods_to_schedule.append(pod)
        pods_to_schedule.sort(
            key=_create_key
        )  # Try to preempt pods for small pods first
        return pods_to_schedule

    def _get_pods_to_preempt(
        self,
        pod_to_schedule: PodDescriptor,
        get_preemptible_pods: Callable[[str], list[PodDescriptor]],
        exclude_nodes: Iterable[Node],
    ) -> tuple[Node | None, list[PodDescriptor]]:
        for node in self._get_nodes(exclude_nodes):
            if not pod_to_schedule.can_be_scheduled(node.labels):
                continue
            preemptible_pods = get_preemptible_pods(node.name)
            if not preemptible_pods:
                logger.debug("Node %r doesn't have pods to preempt", node.name)
                continue
            logger.debug("Find pods to preempt on node %r", node.name)
            resources = self._get_resources_to_preempt(pod_to_schedule, node)
            logger.debug("Resources to preempt on node %r: %s", node.name, resources)
            pods_to_preempt = self._kube_preemption.get_pods_to_preempt(
                resources, preemptible_pods
            )
            if pods_to_preempt:
                logger.info(
                    "Pods to preempt on node %r for pod %r: %r",
                    node.name,
                    pod_to_schedule.name,
                    [p.name for p in pods_to_preempt],
                )
                return node, pods_to_preempt
            logger.debug(
                "Not enough resources on node %r for pod %r",
                node.name,
                pod_to_schedule.name,
            )
        return None, []

    def _get_nodes(self, exclude: Iterable[Node]) -> list[Node]:
        def _create_key(node: Node) -> tuple[int, int, float]:
            r = self._node_resources_handler.get_node_free_resources(node.name)
            if not r:
                return (0, 0, 0.0)
            return (r.gpu or 0, r.memory, r.cpu)

        nodes = self._node_resources_handler.get_nodes()
        nodes = [n for n in nodes if n not in exclude]
        nodes.sort(key=_create_key)  # Try to preempt nodes with less resources first
        return nodes

    def _get_resources_to_preempt(
        self, pod_to_schedule: PodDescriptor, node: Node
    ) -> NodeResources:
        free = self._node_resources_handler.get_node_free_resources(node.name)
        required = pod_to_schedule.resources
        if not required:
            return NodeResources()
        return NodeResources(
            cpu=max(0, required.cpu - free.cpu),
            memory=max(0, required.memory - free.memory),
            gpu=max(0, (required.gpu or 0) - free.gpu),
        )

    async def _delete_pods(self, pods: Iterable[PodDescriptor]) -> None:
        tasks = [
            asyncio.create_task(self._kube_client.delete_pod(pod.name)) for pod in pods
        ]
        if tasks:
            await asyncio.wait(tasks)
