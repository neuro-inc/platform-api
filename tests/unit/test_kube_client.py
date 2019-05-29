from typing import Any, Dict

import pytest

from platform_api.orchestrator.kube_client import (
    NodeAffinity,
    NodePreferredSchedulingTerm,
    NodeSelectorOperator,
    NodeSelectorRequirement,
    NodeSelectorTerm,
    PodContainerStats,
    StatsSummary,
)


class TestNodeSelectorRequirement:
    def test_blank_key(self) -> None:
        with pytest.raises(ValueError, match="blank key"):
            NodeSelectorRequirement("", operator=NodeSelectorOperator.EXISTS)

    def test_non_empty_values_with_exists(self) -> None:
        with pytest.raises(ValueError, match="values must be empty"):
            NodeSelectorRequirement(
                "key", operator=NodeSelectorOperator.EXISTS, values=["value"]
            )

    def test_create_exists(self) -> None:
        req = NodeSelectorRequirement.create_exists("testkey")
        assert req == NodeSelectorRequirement(
            key="testkey", operator=NodeSelectorOperator.EXISTS
        )
        assert req.to_primitive() == {"key": "testkey", "operator": "Exists"}

    def test_create_does_not_exist(self) -> None:
        req = NodeSelectorRequirement.create_does_not_exist("testkey")
        assert req == NodeSelectorRequirement(
            key="testkey", operator=NodeSelectorOperator.DOES_NOT_EXIST
        )
        assert req.to_primitive() == {"key": "testkey", "operator": "DoesNotExist"}


class TestNodeSelectorTerm:
    def test_empty(self) -> None:
        with pytest.raises(ValueError, match="no expressions"):
            NodeSelectorTerm([])


class TestNodeAffinity:
    def test_empty(self) -> None:
        with pytest.raises(ValueError, match="no terms"):
            NodeAffinity()

    def test_to_primitive(self) -> None:
        node_affinity = NodeAffinity(
            required=[
                NodeSelectorTerm([NodeSelectorRequirement.create_exists("testkey")])
            ],
            preferred=[
                NodePreferredSchedulingTerm(
                    NodeSelectorTerm(
                        [NodeSelectorRequirement.create_does_not_exist("anotherkey")]
                    )
                )
            ],
        )
        assert node_affinity.to_primitive() == {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {"matchExpressions": [{"key": "testkey", "operator": "Exists"}]}
                ]
            },
            "preferredDuringSchedulingIgnoredDuringExecution": [
                {
                    "preference": {
                        "matchExpressions": [
                            {"key": "anotherkey", "operator": "DoesNotExist"}
                        ]
                    },
                    "weight": 100,
                }
            ],
        }


class TestPodContainerStats:
    def test_from_primitive_no_cpu(self) -> None:
        payload: Dict[str, Any] = {"memory": {}}
        stats = PodContainerStats.from_primitive(payload)
        assert stats == PodContainerStats(cpu=0.0, memory=0.0)

    def test_from_primitive_empty(self) -> None:
        payload: Dict[str, Any] = {"cpu": {}, "memory": {}}
        stats = PodContainerStats.from_primitive(payload)
        assert stats == PodContainerStats(cpu=0.0, memory=0.0)

    def test_from_primitive(self) -> None:
        payload = {
            "cpu": {"usageNanoCores": 1000},
            "memory": {"workingSetBytes": 1024 * 1024},
            "accelerators": [
                {"dutyCycle": 20, "memoryUsed": 2 * 1024 * 1024},
                {"dutyCycle": 30, "memoryUsed": 4 * 1024 * 1024},
            ],
        }
        stats = PodContainerStats.from_primitive(payload)
        assert stats == PodContainerStats(
            cpu=0.000001, memory=1.0, gpu_duty_cycle=25, gpu_memory=6.0
        )


class TestStatsSummary:
    def test_get_pod_container_stats_no_pod(self) -> None:
        payload: Dict[str, Any] = {"pods": []}
        stats = StatsSummary(payload).get_pod_container_stats(
            "namespace", "pod", "container"
        )
        assert stats is None

    def test_get_pod_container_stats_no_containers(self) -> None:
        payload = {"pods": [{"podRef": {"namespace": "namespace", "name": "pod"}}]}
        stats = StatsSummary(payload).get_pod_container_stats(
            "namespace", "pod", "container"
        )
        assert stats is None

    def test_get_pod_container_stats(self) -> None:
        payload = {
            "pods": [
                {
                    "podRef": {"namespace": "namespace", "name": "pod"},
                    "containers": [{"name": "container", "cpu": {}, "memory": {}}],
                }
            ]
        }
        stats = StatsSummary(payload).get_pod_container_stats(
            "namespace", "pod", "container"
        )
        assert stats
