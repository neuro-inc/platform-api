from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Iterable
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
    PodWatcher,
    PodWatchEvent,
    WatchEventType,
)

logger = logging.getLogger(__name__)


class _Pod(dict[str, Any]):
    @property
    def name(self) -> str:
        return self["metadata"]["name"]

    @property
    def labels(self) -> dict[str, str]:
        return self["metadata"].get("labels", {})

    @property
    def is_idle(self) -> bool:
        return bool(self.labels.get("platform.neuromation.io/idle"))

    @property
    def node_name(self) -> str | None:
        return self["spec"].get("nodeName")

    @property
    def is_pending(self) -> bool:
        return self["status"]["phase"] == "Pending"

    @property
    def is_waiting_for_node(self) -> bool:
        return self.is_pending and not bool(self["spec"].get("nodeName"))

    @property
    def is_scheduled(self) -> bool:
        return self.is_pending and bool(self.node_name)

    @property
    def is_running(self) -> bool:
        return self["status"]["phase"] == "Running"

    @property
    def is_bound_to_node(self) -> bool:
        return self.is_scheduled or self.is_running

    @property
    def is_terminating(self) -> bool:
        return bool(self["metadata"].get("deletionTimestamp"))

    @property
    def resource_requests(self) -> NodeResources:
        pod_resources = NodeResources()
        for container in self["spec"]["containers"]:
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
        self._pod_names: set[str] = set()
        self._nodes: dict[str, Node] = {}
        self._node_free_resources: dict[str, NodeResources] = {}

    async def init(self, raw_pods: list[dict[str, Any]]) -> None:
        for raw_pod in raw_pods:
            pod = _Pod(raw_pod)
            if pod.is_bound_to_node:
                await self._add_pod(pod)

    async def handle(self, event: PodWatchEvent) -> None:
        pod = _Pod(event.raw_pod)
        if pod.is_waiting_for_node:
            return
        if event.type == WatchEventType.DELETED:
            self._remove_pod(pod)
        elif pod.is_bound_to_node:
            await self._add_pod(pod)
        else:
            self._remove_pod(pod)

    async def _add_pod(self, pod: _Pod) -> None:
        node_name: str = pod.node_name  # type: ignore
        pod_name = pod.name
        # Ignore error in case node was removed/lost but pod was not yet removed
        with suppress(NotFoundException):
            if node_name not in self._nodes:
                node = await self._kube_client.get_node(node_name)
                self._nodes[node_name] = node
                self._node_free_resources[node_name] = node.allocatable_resources
            self._pod_names.add(pod_name)
            self._node_free_resources[node_name] -= pod.resource_requests

    def _remove_pod(self, pod: _Pod) -> None:
        node_name: str = pod.node_name  # type: ignore
        pod_name = pod.name
        node = self._nodes.get(node_name)
        if node:
            node_free_resources = self._node_free_resources[node_name]
            node_free_resources += pod.resource_requests
            if node.allocatable_resources == node_free_resources:
                self._node_free_resources.pop(node_name, None)
                self._nodes.pop(node_name, None)
            else:
                self._node_free_resources[node_name] = node_free_resources
        with suppress(KeyError):
            self._pod_names.remove(pod_name)

    def get_nodes(self) -> list[Node]:
        return list(self._nodes.values())

    def get_node_free_resources(self, node_name: str) -> NodeResources:
        return self._node_free_resources.get(node_name) or NodeResources()

    def is_pod_bound_to_node(self, pod_name: str) -> bool:
        return pod_name in self._pod_names

    def _get_node_allocatable_resources(self, node_name: str) -> NodeResources:
        node = self._nodes.get(node_name)
        return node.allocatable_resources if node else NodeResources()


class IdlePodsHandler(PodEventHandler):
    def __init__(self) -> None:
        self._pod_names: set[str] = set()
        self._pods: dict[str, dict[str, PodDescriptor]] = defaultdict(dict)
        self._terminating_pod_names: set[str] = set()
        self._terminating_pod_events: dict[str, list[asyncio.Event]] = defaultdict(list)

    async def init(self, raw_pods: list[dict[str, Any]]) -> None:
        for raw_pod in raw_pods:
            pod = _Pod(raw_pod)
            if pod.is_idle and pod.is_bound_to_node:
                self._add_pod(pod)

    async def handle(self, event: PodWatchEvent) -> None:
        pod = _Pod(event.raw_pod)
        if not pod.is_idle or pod.is_waiting_for_node:
            return
        if event.type == WatchEventType.DELETED:
            self._notify_pod_terminating(pod)  # in case it's force delete
            self._remove_pod(pod)
        elif pod.is_bound_to_node:
            self._add_pod(pod)
            if pod.is_terminating:
                self._notify_pod_terminating(pod)
        else:
            self._remove_pod(pod)

    def _add_pod(self, pod: _Pod) -> None:
        node_name: str = pod.node_name  # type: ignore
        pod_name = pod.name
        # there is an issue in k8s, elements in items don't have kind and version
        pod["kind"] = "Pod"
        self._pod_names.add(pod_name)
        self._pods[node_name][pod_name] = PodDescriptor.from_primitive(pod)

    def _remove_pod(self, pod: _Pod) -> None:
        node_name: str = pod.node_name  # type: ignore
        pod_name = pod.name
        pods = self._pods[node_name]
        pods.pop(pod_name, None)
        if not pods:
            self._pods.pop(node_name, None)
        with suppress(KeyError):
            self._pod_names.remove(pod_name)
        with suppress(KeyError):
            self._terminating_pod_names.remove(pod_name)

    def _notify_pod_terminating(self, pod: _Pod) -> None:
        pod_name = pod.name
        self._terminating_pod_names.add(pod_name)
        events = self._terminating_pod_events.get(pod_name)
        if events is None:
            return
        for event in events:
            event.set()
        self._terminating_pod_events.pop(pod_name, None)

    def get_pods(self, node_name: str) -> list[PodDescriptor]:
        pods = self._pods.get(node_name)
        return list(pods.values()) if pods else []

    async def wait_for_pod_terminating(
        self, pod_name: str, timeous_s: float = 60
    ) -> None:
        if pod_name not in self._pod_names or pod_name in self._terminating_pod_names:
            return
        event = asyncio.Event()
        events = self._terminating_pod_events[pod_name]
        events.append(event)
        try:
            await asyncio.wait_for(event.wait(), timeous_s)
        except asyncio.TimeoutError:
            if len(events) > 1:
                events.remove(event)
            else:
                self._terminating_pod_events.pop(pod_name, None)
            raise


class KubeOrchestratorPreemption:
    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client
        self._kube_preemption = KubePreemption()
        self._node_resources_handler = NodeResourcesHandler(kube_client)
        self._idle_pods_handler = IdlePodsHandler()

    def register(self, pod_watcher: PodWatcher) -> None:
        pod_watcher.subscribe(self._node_resources_handler)
        pod_watcher.subscribe(self._idle_pods_handler)

    async def preempt_idle_pods(self, job_pods: list[PodDescriptor]) -> None:
        nodes_to_preempt: set[Node] = set()
        pods_to_preempt: list[PodDescriptor] = []
        for job_pod in self._get_jobs_for_preemption(job_pods):
            # Handle one node per api poller iteration.
            # Exclude nodes preempted in previous steps
            # to avoid node resources tracking complexity.
            node, pods = self._get_pods_to_preempt(job_pod, nodes_to_preempt)
            if node:
                nodes_to_preempt.add(node)
                pods_to_preempt.extend(pods)
        await self._delete_idle_pods(pods_to_preempt)

    def _get_pods_to_preempt(
        self, job_pod: PodDescriptor, exclude_nodes: Iterable[Node]
    ) -> tuple[Node | None, list[PodDescriptor]]:
        for node in self._get_nodes_for_preemption(exclude_nodes):
            if not job_pod.can_be_scheduled(node.labels):
                continue
            idle_pods = self._idle_pods_handler.get_pods(node.name)
            if not idle_pods:
                logger.debug("Node %r doesn't have idle pods", node.name)
                continue
            logger.debug("Find pods to preempt on node %r", node.name)
            resources = self._get_resources_to_preempt(job_pod, node)
            logger.debug("Resources to preempt on node %r: %s", node.name, resources)
            pods_to_preempt = self._kube_preemption.get_pods_to_preempt(
                resources, idle_pods
            )
            if pods_to_preempt:
                logger.info(
                    "Pods to preempt on node %r for pod %r: %r",
                    node.name,
                    job_pod.name,
                    [p.name for p in pods_to_preempt],
                )
                return node, pods_to_preempt
            logger.debug(
                "Not enough resources on node %r for pod %r", node.name, job_pod.name
            )
        return None, []

    def _get_jobs_for_preemption(
        self, job_pods: Iterable[PodDescriptor]
    ) -> list[PodDescriptor]:
        def _create_key(pod: PodDescriptor) -> tuple[int, int, float]:
            r = pod.resources
            if not r:
                return (0, 0, 0.0)
            return (r.gpu or 0, r.memory, r.cpu)

        pods = []
        for pod in job_pods:
            if not self._node_resources_handler.is_pod_bound_to_node(pod.name):
                pods.append(pod)
        pods.sort(key=_create_key)  # Try to preempt pods for small jobs first
        return pods

    def _get_nodes_for_preemption(self, exclude: Iterable[Node]) -> list[Node]:
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
        self, pod: PodDescriptor, node: Node
    ) -> NodeResources:
        free = self._node_resources_handler.get_node_free_resources(node.name)
        required = pod.resources
        if not required:
            return NodeResources()
        return NodeResources(
            cpu=max(0, required.cpu - free.cpu),
            memory=max(0, required.memory - free.memory),
            gpu=max(0, (required.gpu or 0) - free.gpu),
        )

    def _get_idle_pods(
        self, node_name: str, exclude: Iterable[PodDescriptor]
    ) -> list[PodDescriptor]:
        exclude_names = {pod.name for pod in exclude}
        idle_pods = self._idle_pods_handler.get_pods(node_name)
        idle_pods = [p for p in idle_pods if p.name not in exclude_names]
        return idle_pods

    async def _delete_idle_pods(self, pods: Iterable[PodDescriptor]) -> None:
        tasks = [asyncio.create_task(self._delete_idle_pod(pod.name)) for pod in pods]
        if tasks:
            await asyncio.wait(tasks)

    async def _delete_idle_pod(self, pod_name: str) -> None:
        await self._kube_client.delete_pod(pod_name)
        await self._idle_pods_handler.wait_for_pod_terminating(pod_name, timeous_s=15)
