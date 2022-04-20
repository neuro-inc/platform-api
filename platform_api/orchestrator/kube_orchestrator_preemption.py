from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from contextlib import suppress
from typing import Any

from .kube_client import (
    KubeClient,
    KubePreemption,
    Node,
    NodeResources,
    PodDescriptor,
    PodEventHandler,
    PodWatcher,
    PodWatchEvent,
    WatchEventType,
)

logger = logging.getLogger(__name__)


def _is_pod_pending(pod: dict[str, Any]) -> bool:
    return pod["status"]["phase"] == "Pending"


def _is_pod_running(pod: dict[str, Any]) -> bool:
    return pod["status"]["phase"] == "Running"


def _is_pod_terminating(pod: dict[str, Any]) -> bool:
    return bool(pod["metadata"].get("deletionTimestamp"))


def _is_idle_pod(pod: dict[str, Any]) -> bool:
    pod_labels = pod["metadata"].get("labels", {})
    return bool(pod_labels.get("platform.neuromation.io/idle"))


class NodeResourcesHandler(PodEventHandler):
    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client
        self._pod_names: set[str] = set()
        self._nodes: dict[str, Node] = {}
        self._node_resources: dict[str, dict[str, NodeResources]] = defaultdict(dict)

    async def init(self, raw_pods: list[dict[str, Any]]) -> None:
        for pod in raw_pods:
            if _is_pod_running(pod):
                await self._add_pod(pod)

    async def handle(self, event: PodWatchEvent) -> None:
        pod = event.raw_pod
        if _is_pod_pending(pod):
            return
        if event.type == WatchEventType.DELETED:
            self._remove_pod(pod)
        elif _is_pod_running(pod):
            await self._add_pod(pod)
        else:
            self._remove_pod(pod)

    async def _add_pod(self, pod: dict[str, Any]) -> None:
        node_name = pod["spec"]["nodeName"]
        pod_name = pod["metadata"]["name"]
        self._pod_names.add(pod_name)
        self._node_resources[node_name][pod_name] = self._get_pod_resources(pod)
        if node_name not in self._nodes:
            self._nodes[node_name] = await self._kube_client.get_node(node_name)

    def _remove_pod(self, pod: dict[str, Any]) -> None:
        node_name = pod["spec"]["nodeName"]
        pod_name = pod["metadata"]["name"]
        node_resources = self._node_resources[node_name]
        node_resources.pop(pod_name, None)
        if not node_resources:
            self._node_resources.pop(node_name, None)
            self._nodes.pop(node_name, None)
        with suppress(KeyError):
            self._pod_names.remove(pod_name)

    @classmethod
    def _get_pod_resources(cls, pod: dict[str, Any]) -> NodeResources:
        pod_resources = NodeResources(0, 0)
        for container in pod["spec"]["containers"]:
            resources = container.get("resources")
            if not resources:
                continue
            requests = resources.get("requests")
            if not requests:
                continue
            pod_resources += NodeResources.from_primitive(requests)
        return pod_resources

    def get_nodes(self) -> list[Node]:
        return list(self._nodes.values())

    def get_node_free_resources(self, node_name: str) -> NodeResources:
        total = self._get_node_allocatable_resources(node_name)
        used = self._get_node_requested_resources(node_name)
        return NodeResources(
            cpu=total.cpu - used.cpu,
            memory=total.memory - used.memory,
            gpu=total.gpu - used.gpu,
        )

    def is_pod_running(self, pod_name: str) -> bool:
        return pod_name in self._pod_names

    def _get_node_allocatable_resources(self, node_name: str) -> NodeResources:
        node = self._nodes.get(node_name)
        if not node:
            return NodeResources(0.0, 0)
        return node.allocatable_resources

    def _get_node_requested_resources(self, node_name: str) -> NodeResources:
        node_resources = self._node_resources.get(node_name)
        if not node_resources:
            return NodeResources(0.0, 0)
        cpu, memory, gpu = 0.0, 0, 0
        for resources in self._node_resources[node_name].values():
            cpu += resources.cpu
            memory += resources.memory
            gpu += resources.gpu or 0
        return NodeResources(cpu=cpu, memory=memory, gpu=gpu)


class IdlePodsHandler(PodEventHandler):
    def __init__(self) -> None:
        self._pod_names: set[str] = set()
        self._pods: dict[str, dict[str, PodDescriptor]] = defaultdict(dict)
        self._terminating_pod_names: set[str] = set()
        self._terminating_pod_events: dict[str, list[asyncio.Event]] = defaultdict(list)

    async def init(self, raw_pods: list[dict[str, Any]]) -> None:
        for pod in raw_pods:
            if _is_idle_pod(pod) and _is_pod_running(pod):
                self._add_pod(pod)

    async def handle(self, event: PodWatchEvent) -> None:
        pod = event.raw_pod
        if not _is_idle_pod(pod) or _is_pod_pending(pod):
            return
        if event.type == WatchEventType.DELETED:
            self._notify_pod_terminating(pod)  # in case it's force delete
            self._remove_pod(pod)
        elif _is_pod_running(pod):
            self._add_pod(pod)
            if _is_pod_terminating(pod):
                self._notify_pod_terminating(pod)
        else:
            self._remove_pod(pod)

    def _add_pod(self, pod: dict[str, Any]) -> None:
        node_name = pod["spec"]["nodeName"]
        pod_name = pod["metadata"]["name"]
        pod["kind"] = "Pod"
        self._pod_names.add(pod_name)
        self._pods[node_name][pod_name] = PodDescriptor.from_primitive(pod)

    def _remove_pod(self, pod: dict[str, Any]) -> None:
        node_name = pod["spec"]["nodeName"]
        pod_name = pod["metadata"]["name"]
        pods = self._pods[node_name]
        pods.pop(pod_name, None)
        if not pods:
            self._pods.pop(node_name, None)
        with suppress(KeyError):
            self._pod_names.remove(pod_name)
        with suppress(KeyError):
            self._terminating_pod_names.remove(pod_name)

    def _notify_pod_terminating(self, pod: dict[str, Any]) -> None:
        pod_name = pod["metadata"]["name"]
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
        job_pods = job_pods.copy()
        self._sort_pods_for_preemption(job_pods)

        pods_to_preempt = []
        for job_pod in job_pods:
            pods_to_preempt.extend(self._get_pods_to_preempt(job_pod))

        await self._delete_idle_pods(pods_to_preempt)

    def _get_pods_to_preempt(self, job_pod: PodDescriptor) -> list[PodDescriptor]:
        if self._node_resources_handler.is_pod_running(job_pod.name):
            return []

        for node in self._node_resources_handler.get_nodes():
            if not job_pod.can_be_scheduled(node.labels):
                continue
            idle_pods = self._idle_pods_handler.get_pods(node.name)
            if not idle_pods:
                continue
            logger.debug("Find pods to preempt on node %r", node.name)
            resources = self._get_resources_to_preempt(job_pod, node)
            logger.debug(
                "Resources to preempt on node %r: cpu=%dm, memory=%dMi, gpu=%d",
                node.name,
                resources.cpu_mcores,
                resources.memory,
                resources.gpu,
            )
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
                return pods_to_preempt
            logger.debug(
                "Not enough resources on node %r for pod %r", node.name, job_pod.name
            )
        return []

    @classmethod
    def _sort_pods_for_preemption(self, job_pods: list[PodDescriptor]) -> None:
        def _create_key(pod: PodDescriptor) -> tuple[int, int, float]:
            r = pod.resources
            if not r:
                return (0, 0, 0.0)
            return (r.gpu or 0, r.memory, r.cpu)

        job_pods.sort(key=lambda n: _create_key(n))

    def _get_resources_to_preempt(
        self, pod: PodDescriptor, node: Node
    ) -> NodeResources:
        free = self._node_resources_handler.get_node_free_resources(node.name)
        required = pod.resources
        if not required:
            return NodeResources(0, 0)
        return NodeResources(
            cpu=max(0, required.cpu - free.cpu),
            memory=max(0, required.memory - free.memory),
            gpu=max(0, (required.gpu or 0) - free.gpu),
        )

    async def _delete_idle_pods(self, pods: list[PodDescriptor]) -> None:
        if not pods:
            return
        await asyncio.wait(
            [asyncio.create_task(self._delete_idle_pod(pod)) for pod in pods]
        )

    async def _delete_idle_pod(self, pod: PodDescriptor) -> None:
        await self._kube_client.delete_pod(pod.name)
        await self._idle_pods_handler.wait_for_pod_terminating(pod.name, timeous_s=15)
