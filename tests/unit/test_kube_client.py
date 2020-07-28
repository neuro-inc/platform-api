from datetime import datetime, timezone

import pytest

from platform_api.orchestrator.kube_client import (
    KubernetesEvent,
    NodeAffinity,
    NodePreferredSchedulingTerm,
    NodeSelectorOperator,
    NodeSelectorRequirement,
    NodeSelectorTerm,
    PodCondition,
    PodConditionType,
    PodStatus,
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
            "phase": "Succeeded",
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
        assert cond.status is False
        assert cond.type == PodConditionType.READY

    def test_is_sceduled_true_1(self) -> None:
        payload = {
            "conditions": [
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
            "phase": "Running",
            "startTime": "2019-06-20T11:03:32Z",
        }

        status = PodStatus.from_primitive(payload)
        assert status.is_scheduled

    def test_is_sceduled_true_2(self) -> None:
        payload = {
            "conditions": [
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
            "phase": "Pending",
            "startTime": "2019-06-20T11:03:32Z",
        }

        status = PodStatus.from_primitive(payload)
        assert status.is_scheduled

    def test_is_sceduled_false_1(self) -> None:
        payload = {"phase": "Pending", "startTime": "2019-06-20T11:03:32Z"}

        status = PodStatus.from_primitive(payload)
        assert not status.is_scheduled

    def test_is_sceduled_false_2(self) -> None:
        payload = {
            "conditions": [
                {
                    "lastProbeTime": None,
                    "lastTransitionTime": "2019-06-20T11:03:32Z",
                    "status": "False",
                    "type": "PodScheduled",
                }
            ],
            "phase": "Pending",
            "startTime": "2019-06-20T11:03:32Z",
        }

        status = PodStatus.from_primitive(payload)
        assert not status.is_scheduled


class TestPodCondition:
    def test_unknown_type(self) -> None:
        payload = {"lastTransitionTime": "2019-06-20T11:03:32Z", "type": "Invalid"}
        cond = PodCondition(payload)
        assert cond.type == PodConditionType.UNKNOWN

    def test_status_unknown(self) -> None:
        cond = PodCondition({"status": "Unknown"})
        assert cond.status is None

    def test_status_true(self) -> None:
        cond = PodCondition({"status": "True"})
        assert cond.status is True

    def test_status_false(self) -> None:
        cond = PodCondition({"status": "False"})
        assert cond.status is False

    def test_status_invalid(self) -> None:
        cond = PodCondition({"status": "123"})
        with pytest.raises(ValueError):
            cond.status


class TestKubernetesEvent:
    def test_first_timestamp(self) -> None:
        data = {
            "apiVersion": "v1",
            "count": 12,
            "eventTime": None,
            "firstTimestamp": "2019-06-20T11:03:32Z",
            "involvedObject": {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": "job-cd109c3b-c36e-47d4-b3d6-8bb05a5e63ab",
                "namespace": "namespace",
                "resourceVersion": "48102193",
                "uid": "eddfe678-86e9-11e9-9d65-42010a800018",
            },
            "kind": "Event",
            "lastTimestamp": "2019-06-20T11:03:33Z",
            "message": "TriggeredScaleUp",
            "metadata": {
                "creationTimestamp": "2019-06-20T11:03:32Z",
                "name": "job-cd109c3b-c36e-47d4-b3d6-8bb05a5e63ab.15a870d7e2bb228b",
                "namespace": "namespace",
                "selfLink": (
                    "/api/v1/namespaces/namespace/events/{pod_id}.15a870d7e2bb228b"
                ),
                "uid": "cb886f64-8f96-11e9-9251-42010a800038",
            },
            "reason": "TriggeredScaleUp",
            "reportingComponent": "",
            "reportingInstance": "",
            "source": {"component": "cluster-autoscaler"},
            "type": "Normal",
        }
        event = KubernetesEvent(data)
        assert event.first_timestamp == datetime(
            2019, 6, 20, 11, 3, 32, tzinfo=timezone.utc
        )
        assert event.last_timestamp == datetime(
            2019, 6, 20, 11, 3, 33, tzinfo=timezone.utc
        )
        assert event.count == 12
