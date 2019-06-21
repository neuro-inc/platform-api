from datetime import datetime, timezone
from typing import Any, Dict

import pytest

from platform_api.orchestrator.kube_client import (
    NodeAffinity,
    NodePreferredSchedulingTerm,
    NodeSelectorOperator,
    NodeSelectorRequirement,
    NodeSelectorTerm,
    PodConditionType,
    PodContainerStats,
    PodStatus,
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
    def test_from_primitive_no_keys(self) -> None:
        payload: Dict[str, Any] = {"memory": {}}
        stats = PodContainerStats.from_primitive(payload)
        empty_stats = PodContainerStats(cpu=0.0, memory=0.0)
        assert stats == empty_stats
        payload = {"cpu": {}}
        stats = PodContainerStats.from_primitive(payload)
        assert stats == empty_stats
        payload = {}
        stats = PodContainerStats.from_primitive(payload)
        assert stats == empty_stats

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


class TestPodStatus:
    def test_from_primitive(self) -> None:
        payload = {"phase": "Running", "containerStatuses": [{"ready": True}]}
        status = PodStatus.from_primitive(payload)
        assert status.phase == "Running"
        assert len(status.conditions) == 0

    def test_from_primitive_with_conditions(self) -> None:
        payload = {
            "conditions": [
                {
                    "lastProbeTime": None,
                    "lastTransitionTime": "2019-06-20T11:03:32Z",
                    "reason": "PodCompleted",
                    "status": "True",
                    "type": "Initialized",
                },
                {
                    "lastProbeTime": None,
                    "lastTransitionTime": "2019-06-20T11:13:37Z",
                    "reason": "PodCompleted",
                    "status": "False",
                    "type": "Ready",
                },
                {
                    "lastProbeTime": None,
                    "lastTransitionTime": "2019-06-20T11:03:32Z",
                    "status": "True",
                    "type": "PodScheduled",
                },
            ],
            "containerStatuses": [
                {
                    "containerID": "docker://cf4061683a6d7",
                    "image": "ubuntu:latest",
                    "imageID": "docker-pullable://ubuntu@sha256:eb70667a8016",
                    "lastState": {},
                    "name": "job-fce70f73-4a6e-45f6-ba20-b338ea9a5609",
                    "ready": False,
                    "restartCount": 0,
                    "state": {
                        "terminated": {
                            "containerID": "docker://cf4061683a6d7",
                            "exitCode": 0,
                            "finishedAt": "2019-06-20T11:13:36Z",
                            "reason": "Completed",
                            "startedAt": "2019-06-20T11:03:36Z",
                        }
                    },
                }
            ],
            "hostIP": "10.128.0.27",
            "phase": "Succeeded",
            "podIP": "10.44.4.175",
            "qosClass": "Guaranteed",
            "startTime": "2019-06-20T11:03:32Z",
        }

        status = PodStatus.from_primitive(payload)
        assert len(status.conditions) == 3
        cond = status.conditions[1]
        assert cond.transition_time == datetime(
            2019, 6, 20, 11, 13, 37, tzinfo=timezone.utc
        )
        assert cond.reason == "PodCompleted"
        assert cond.message == ""
        assert cond.status == False
        assert cond.type == PodConditionType.READY
