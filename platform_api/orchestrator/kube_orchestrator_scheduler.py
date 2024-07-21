from __future__ import annotations

import logging
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from typing import Any

from .kube_client import (
    EventHandler,
    KubeClient,
    KubePreemption,
    Node,
    NodeResources,
    PodDescriptor,
    PodStatus,
    WatchEvent,
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


class NodesHandler(EventHandler):
    def __init__(self) -> None:
        self._nodes: dict[str, Node] = {}

    async def init(self, raw_nodes: list[dict[str, Any]]) -> None:
        for raw_node in raw_nodes:
            node = Node.from_primitive(raw_node)
            if node.status.is_ready:
                self._add_node(node)

    async def handle(self, event: WatchEvent) -> None:
        node = Node.from_primitive(event.resource)
        if event.type == WatchEventType.DELETED or not node.status.is_ready:
            self._remove_node(node)
        else:
            self._add_node(node)

    def _add_node(self, node: Node) -> None:
        self._nodes[node.name] = node

    def _remove_node(self, node: Node) -> None:
        self._nodes.pop(node.name, None)

    def get_ready_nodes(self) -> Iterable[Node]:
        return self._nodes.values()


class NodeResourcesHandler(EventHandler):
    def __init__(self) -> None:
        self._resource_requests: dict[str, NodeResources] = defaultdict(NodeResources)
        self._pod_counts: Counter[str] = Counter()
        self._pod_node_names: dict[str, str] = {}

    async def init(self, raw_pods: list[dict[str, Any]]) -> None:
        for raw_pod in raw_pods:
            pod = _Pod(raw_pod)
            if (
                not pod.is_idle
                and pod.status.is_scheduled
                and not pod.status.is_terminated
                and not pod.status.is_phase_failed
            ):
                self._add_pod(pod)

    async def handle(self, event: WatchEvent) -> None:
        pod = _Pod(event.resource)
        if pod.is_idle or not pod.status.is_scheduled:
            return
        if (
            event.type == WatchEventType.DELETED
            or pod.status.is_terminated
            or pod.status.is_phase_failed
        ):
            self._remove_pod(pod)
        else:
            self._add_pod(pod)

    def _add_pod(self, pod: _Pod) -> None:
        pod_name = pod.name
        if pod_name in self._pod_node_names:
            return
        node_name = pod.node_name
        self._resource_requests[node_name] += pod.resource_requests
        self._pod_counts[node_name] += 1
        self._pod_node_names[pod_name] = node_name

    def _remove_pod(self, pod: _Pod) -> None:
        pod_name = pod.name
        if pod_name not in self._pod_node_names:
            return
        node_name = pod.node_name
        resource_requests = self._resource_requests[node_name]
        resource_requests -= pod.resource_requests
        pod_counts = self._pod_counts[node_name]
        pod_counts -= 1
        if pod_counts == 0:
            self._resource_requests.pop(node_name, None)
            self._pod_counts.pop(node_name, None)
        else:
            self._resource_requests[node_name] = resource_requests
            self._pod_counts[node_name] = pod_counts
        self._pod_node_names.pop(pod_name, None)

    def get_resource_requests(self, node_name: str) -> NodeResources:
        return self._resource_requests.get(node_name) or NodeResources()

    def get_pod_node_name(self, pod_name: str) -> str | None:
        """
        Get name of the node which runs pod.
        Return None if pod is not scheduled.
        """
        return self._pod_node_names.get(pod_name)


class KubeOrchestratorScheduler:
    def __init__(
        self, nodes_handler: NodesHandler, node_resources_handler: NodeResourcesHandler
    ) -> None:
        self._nodes_handler = nodes_handler
        self._node_resources_handler = node_resources_handler

    def is_pod_scheduled(self, pod_name: str) -> bool:
        return bool(self._node_resources_handler.get_pod_node_name(pod_name))

    def get_schedulable_pods(
        self, pods: Iterable[PodDescriptor]
    ) -> list[PodDescriptor]:
        schedulable_pods: list[PodDescriptor] = []
        scheduled: dict[str, NodeResources] = defaultdict(NodeResources)
        for pod in pods:
            logger.debug("Check pod %r can be scheduled", pod.name)
            if self.is_pod_scheduled(pod.name):
                logger.debug("Pod %r has already been scheduled", pod.name)
                schedulable_pods.append(pod)
                continue
            for node in self._nodes_handler.get_ready_nodes():
                if not pod.can_be_scheduled(node.labels):
                    logger.debug(
                        "Pod %r cannot be scheduled to node %r due to labels mismatch",
                        pod.name,
                        node.name,
                    )
                    continue
                requested = self._node_resources_handler.get_resource_requests(
                    node.name
                )
                requested += scheduled[node.name]
                free = node.get_free_resources(requested)
                if free.are_sufficient(pod):
                    logger.debug(
                        "Pod %r can be scheduled onto node %r", pod.name, node.name
                    )
                    schedulable_pods.append(pod)
                    if pod.resources:
                        scheduled[node.name] += NodeResources(
                            cpu=pod.resources.cpu,
                            memory=pod.resources.memory,
                            nvidia_gpu=pod.resources.nvidia_gpu or 0,
                            amd_gpu=pod.resources.amd_gpu or 0,
                        )
                    break
                logger.debug(
                    "Node %r has not enough resources for pod %r. Resources left: %s",
                    node.name,
                    pod.name,
                    free,
                )
        return schedulable_pods


class KubeOrchestratorPreemption:
    def __init__(
        self,
        kube_client: KubeClient,
        nodes_handler: NodesHandler,
        node_resources_handler: NodeResourcesHandler,
    ) -> None:
        self._kube_client = kube_client
        self._kube_preemption = KubePreemption()
        self._nodes_handler = nodes_handler
        self._node_resources_handler = node_resources_handler

    def get_pods_to_preempt(
        self,
        pods_to_schedule: list[PodDescriptor],
        preemptible_pods: list[PodDescriptor],
    ) -> list[PodDescriptor]:
        preemptible_pods_by_node = defaultdict(list)
        for pod in preemptible_pods:
            node_name = self._node_resources_handler.get_pod_node_name(pod.name)
            if node_name:
                preemptible_pods_by_node[node_name].append(pod)
        return self._get_pods_to_preempt(
            pods_to_schedule, preemptible_pods_by_node.__getitem__
        )

    def _get_pods_to_preempt(
        self,
        pods_to_schedule: list[PodDescriptor],
        get_preemptible_pods: Callable[[str], list[PodDescriptor]],
    ) -> list[PodDescriptor]:
        nodes_to_preempt: set[Node] = set()
        pods_to_preempt: list[PodDescriptor] = []
        for pod in self._get_pods_to_schedule(pods_to_schedule):
            # Handle one node per api poller iteration.
            # Exclude nodes preempted in previous steps
            # to avoid node resources tracking complexity.
            node, pods = self._get_pods_to_preempt_for_pod(
                pod,
                get_preemptible_pods=get_preemptible_pods,
                exclude_nodes=nodes_to_preempt,
            )
            if node:
                nodes_to_preempt.add(node)
                pods_to_preempt.extend(pods)
        return pods_to_preempt

    def _get_pods_to_schedule(
        self, pods: Iterable[PodDescriptor]
    ) -> list[PodDescriptor]:
        def _create_key(pod: PodDescriptor) -> tuple[int, int, float]:
            r = pod.resources
            if not r:
                return (0, 0, 0.0)
            return ((r.nvidia_gpu or 0) + (r.amd_gpu or 0), r.memory, r.cpu)

        pods_to_schedule = []
        for pod in pods:
            if not self._node_resources_handler.get_pod_node_name(pod.name):
                pods_to_schedule.append(pod)
        pods_to_schedule.sort(
            key=_create_key
        )  # Try to preempt pods for small pods first
        return pods_to_schedule

    def _get_pods_to_preempt_for_pod(
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
        def _create_key(node: Node) -> tuple[int, int, int, float]:
            requested = self._node_resources_handler.get_resource_requests(node.name)
            free = node.get_free_resources(requested)
            if not free:
                return (0, 0, 0, 0.0)
            return (free.nvidia_gpu or 0, free.amd_gpu or 0, free.memory, free.cpu)

        nodes = self._nodes_handler.get_ready_nodes()
        nodes = [n for n in nodes if n not in exclude]
        nodes.sort(key=_create_key)  # Try to preempt nodes with less resources first
        return nodes

    def _get_resources_to_preempt(
        self, pod_to_schedule: PodDescriptor, node: Node
    ) -> NodeResources:
        requested = self._node_resources_handler.get_resource_requests(node.name)
        free = node.get_free_resources(requested)
        required = pod_to_schedule.resources
        if not required:
            return NodeResources()
        return NodeResources(
            cpu=max(0, required.cpu - free.cpu),
            memory=max(0, required.memory - free.memory),
            nvidia_gpu=max(0, (required.nvidia_gpu or 0) - free.nvidia_gpu),
            amd_gpu=max(0, (required.amd_gpu or 0) - free.amd_gpu),
        )
