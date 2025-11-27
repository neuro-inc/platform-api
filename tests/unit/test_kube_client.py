import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from apolo_kube_client import (
    V1LabelSelector,
    V1LabelSelectorRequirement,
    V1Node,
    V1NodeAffinity,
    V1NodeCondition,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
    V1NodeStatus,
    V1ObjectMeta,
    V1PodAffinity,
    V1PodAffinityTerm,
    V1PreferredSchedulingTerm,
    V1WeightedPodAffinityTerm,
)

from platform_api.orchestrator.kube_client import (
    KubePreemption,
    KubernetesEvent,
    LabelSelectorMatchExpression,
    LabelSelectorTerm,
    Node,
    NodeAffinity,
    NodeCondition,
    NodeConditionType,
    NodePreferredSchedulingTerm,
    NodeResources,
    NodeStatus,
    PodAffinity,
    PodAffinityTerm,
    PodCondition,
    PodConditionType,
    PodDescriptor,
    PodPreferredSchedulingTerm,
    PodStatus,
    Resources,
    SelectorOperator,
)


class TestLabelSelectorMatchExpression:
    def test_blank_key(self) -> None:
        with pytest.raises(ValueError, match="blank key"):
            LabelSelectorMatchExpression("", operator=SelectorOperator.EXISTS)

    def test_non_empty_values_with_exists(self) -> None:
        with pytest.raises(ValueError, match="values must be empty"):
            LabelSelectorMatchExpression(
                "key", operator=SelectorOperator.EXISTS, values=["value"]
            )

    def test_create_in(self) -> None:
        req = LabelSelectorMatchExpression.create_in("testkey", "testvalue")
        assert req == LabelSelectorMatchExpression(
            key="testkey", operator=SelectorOperator.IN, values=["testvalue"]
        )
        assert req.to_model() == V1LabelSelectorRequirement(
            key="testkey",
            operator="In",
            values=["testvalue"],
        )

    def test_create_exists(self) -> None:
        req = LabelSelectorMatchExpression.create_exists("testkey")
        assert req == LabelSelectorMatchExpression(
            key="testkey", operator=SelectorOperator.EXISTS
        )
        assert req.to_model() == V1LabelSelectorRequirement(
            key="testkey", operator="Exists"
        )

    def test_create_does_not_exist(self) -> None:
        req = LabelSelectorMatchExpression.create_does_not_exist("testkey")
        assert req == LabelSelectorMatchExpression(
            key="testkey", operator=SelectorOperator.DOES_NOT_EXIST
        )
        assert req.to_model() == V1LabelSelectorRequirement(
            key="testkey", operator="DoesNotExist"
        )

    def test_create_gt(self) -> None:
        req = LabelSelectorMatchExpression.create_gt("testkey", 1)
        assert req == LabelSelectorMatchExpression(
            key="testkey", operator=SelectorOperator.GT, values=["1"]
        )
        assert req.to_model() == V1LabelSelectorRequirement(
            key="testkey",
            operator="Gt",
            values=["1"],
        )

    def test_create_lt(self) -> None:
        req = LabelSelectorMatchExpression.create_lt("testkey", 1)
        assert req == LabelSelectorMatchExpression(
            key="testkey", operator=SelectorOperator.LT, values=["1"]
        )
        assert req.to_model() == V1LabelSelectorRequirement(
            key="testkey",
            operator="Lt",
            values=["1"],
        )

    def test_in_requirement_is_satisfied(self) -> None:
        req = LabelSelectorMatchExpression.create_in("testkey", "testvalue")

        assert req.is_satisfied({"testkey": "testvalue"}) is True
        assert req.is_satisfied({"testkey": "testvalue2"}) is False

    def test_exists_requirement_is_satisfied(self) -> None:
        req = LabelSelectorMatchExpression.create_exists("testkey")

        assert req.is_satisfied({"testkey": "testvalue"}) is True
        assert req.is_satisfied({"testkey2": "testvalue"}) is False

    def test_does_not_exist_requirement_is_satisfied(self) -> None:
        req = LabelSelectorMatchExpression.create_does_not_exist("testkey2")

        assert req.is_satisfied({"testkey": "testvalue"}) is True
        assert req.is_satisfied({"testkey2": "testvalue"}) is False

    def test_gt_requirement_is_satisfied(self) -> None:
        req = LabelSelectorMatchExpression.create_gt("testkey", 1)

        assert req.is_satisfied({"testkey": "2"}) is True
        assert req.is_satisfied({"testkey2": "1"}) is False

    def test_lt_requirement_is_satisfied(self) -> None:
        req = LabelSelectorMatchExpression.create_lt("testkey", 1)

        assert req.is_satisfied({"testkey": "0"}) is True
        assert req.is_satisfied({"testkey2": "1"}) is False


class TestLabelSelectorTerm:
    def test_empty(self) -> None:
        with pytest.raises(ValueError, match="no expressions"):
            LabelSelectorTerm([])

    def test_is_satisfied(self) -> None:
        term = LabelSelectorTerm(
            [
                LabelSelectorMatchExpression.create_exists("job"),
                LabelSelectorMatchExpression.create_in("zone", "us-east-1a"),
            ]
        )

        assert term.is_satisfied({"job": "id", "zone": "us-east-1a"}) is True
        assert term.is_satisfied({"job": "id", "zone": "us-east-1b"}) is False


class TestNodeAffinity:
    def test_empty(self) -> None:
        with pytest.raises(ValueError, match="no terms"):
            NodeAffinity()

    def test_is_satisfied(self) -> None:
        term1 = LabelSelectorTerm(
            [LabelSelectorMatchExpression.create_in("zone", "us-east-1a")]
        )
        term2 = LabelSelectorTerm(
            [LabelSelectorMatchExpression.create_in("zone", "us-east-1b")]
        )
        node_affinity = NodeAffinity(required=[term1, term2])

        assert node_affinity.is_satisfied({"zone": "us-east-1a"}) is True
        assert node_affinity.is_satisfied({"zone": "us-east-1b"}) is True
        assert node_affinity.is_satisfied({"zone": "us-east-1c"}) is False

    def test_to_model(self) -> None:
        node_affinity = NodeAffinity(
            required=[
                LabelSelectorTerm(
                    [LabelSelectorMatchExpression.create_exists("testkey")]
                )
            ],
            preferred=[
                NodePreferredSchedulingTerm(
                    LabelSelectorTerm(
                        [
                            LabelSelectorMatchExpression.create_does_not_exist(
                                "anotherkey"
                            )
                        ]
                    )
                )
            ],
        )
        assert node_affinity.to_model() == V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=V1NodeSelector(
                node_selector_terms=[
                    V1NodeSelectorTerm(
                        match_expressions=[
                            V1NodeSelectorRequirement(key="testkey", operator="Exists")
                        ]
                    )
                ]
            ),
            preferred_during_scheduling_ignored_during_execution=[
                V1PreferredSchedulingTerm(
                    preference=V1NodeSelectorTerm(
                        match_expressions=[
                            V1NodeSelectorRequirement(
                                key="anotherkey", operator="DoesNotExist"
                            )
                        ]
                    ),
                    weight=100,
                )
            ],
        )


class TestPodAffinity:
    def test_to_model(self) -> None:
        pod_affinity = PodAffinity(
            preferred=[
                PodPreferredSchedulingTerm(
                    PodAffinityTerm(
                        LabelSelectorTerm(
                            [
                                LabelSelectorMatchExpression.create_exists("mylabel"),
                            ]
                        )
                    )
                )
            ]
        )

        assert pod_affinity.to_model() == V1PodAffinity(
            preferred_during_scheduling_ignored_during_execution=[
                V1WeightedPodAffinityTerm(
                    pod_affinity_term=V1PodAffinityTerm(
                        label_selector=V1LabelSelector(
                            match_expressions=[
                                V1LabelSelectorRequirement(
                                    key="mylabel", operator="Exists"
                                )
                            ],
                        ),
                        topology_key="kubernetes.io/hostname",
                    ),
                    weight=100,
                )
            ]
        )

    def test_to_model_empty(self) -> None:
        pod_affinity = PodAffinity()
        assert pod_affinity.to_model() == V1PodAffinity()


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
        assert cond.transition_time == datetime(2019, 6, 20, 11, 13, 37, tzinfo=UTC)
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

    def test_is_waiting_true(self) -> None:
        payload: dict[str, Any] = {
            "phase": "Pending",
            "containerStatuses": [
                {"state": {"waiting": {}}},
                {"state": {"running": {}}},
            ],
        }

        status = PodStatus.from_primitive(payload)
        assert status.is_waiting

    def test_is_waiting_false(self) -> None:
        payload: dict[str, Any] = {
            "phase": "Pending",
            "containerStatuses": [
                {"state": {"running": {}}},
                {"state": {"running": {}}},
            ],
        }

        status = PodStatus.from_primitive(payload)
        assert status.is_waiting is False

    def test_is_terminated_true(self) -> None:
        payload: dict[str, Any] = {
            "phase": "Pending",
            "containerStatuses": [
                {"state": {"terminated": {}}},
                {"state": {"terminated": {}}},
            ],
        }

        status = PodStatus.from_primitive(payload)
        assert status.is_terminated

    def test_is_terminated_false(self) -> None:
        payload: dict[str, Any] = {
            "phase": "Pending",
            "containerStatuses": [
                {"state": {"running": {}}},
                {"state": {"terminated": {}}},
            ],
        }

        status = PodStatus.from_primitive(payload)
        assert status.is_terminated is False


class TestPodCondition:
    @pytest.fixture
    def pod_condition_payload(self) -> dict[str, str]:
        return {
            "lastTransitionTime": "2019-06-20T11:03:32Z",
            "type": "Invalid",
            "status": "True",
            "reason": "reason",
            "message": "message",
        }

    def test_unknown_type(
        self,
        pod_condition_payload: dict[str, str],
    ) -> None:
        pod_condition_payload.update(
            {"lastTransitionTime": "2019-06-20T11:03:32Z", "type": "Invalid"}
        )
        cond = PodCondition.from_primitive(pod_condition_payload)
        assert cond.type == PodConditionType.UNKNOWN

    def test_status_unknown(
        self,
        pod_condition_payload: dict[str, str],
    ) -> None:
        pod_condition_payload["status"] = "Unknown"
        cond = PodCondition.from_primitive(pod_condition_payload)
        assert cond.status is None

    def test_status_true(
        self,
        pod_condition_payload: dict[str, str],
    ) -> None:
        pod_condition_payload["status"] = "True"
        cond = PodCondition.from_primitive(pod_condition_payload)
        assert cond.status is True

    def test_status_false(
        self,
        pod_condition_payload: dict[str, str],
    ) -> None:
        pod_condition_payload["status"] = "False"
        cond = PodCondition.from_primitive(pod_condition_payload)
        assert cond.status is False

    def test_status_invalid(
        self,
        pod_condition_payload: dict[str, str],
    ) -> None:
        pod_condition_payload["status"] = "123"
        with pytest.raises(ValueError):
            PodCondition.from_primitive(pod_condition_payload)


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
        event = KubernetesEvent.from_primitive(data)
        assert event.first_timestamp == datetime(2019, 6, 20, 11, 3, 32, tzinfo=UTC)
        assert event.last_timestamp == datetime(2019, 6, 20, 11, 3, 33, tzinfo=UTC)
        assert event.count == 12


class TestNodeCondition:
    def test_from_model_status_true(self) -> None:
        now = datetime.now(UTC)
        condition = NodeCondition.from_model(
            V1NodeCondition(
                type="Ready",
                status="True",
                last_transition_time=now,
            )
        )

        assert condition == NodeCondition(
            type=NodeConditionType.READY, status=True, transition_time=now
        )

    def test_from_model_status_false(self) -> None:
        now = datetime.now(UTC)
        condition = NodeCondition.from_model(
            V1NodeCondition(
                type="Ready",
                status="False",
                last_transition_time=now,
            )
        )

        assert condition == NodeCondition(
            type=NodeConditionType.READY, status=False, transition_time=now
        )

    def test_from_model_status_unknown(self) -> None:
        now = datetime.now(UTC)
        condition = NodeCondition.from_model(
            V1NodeCondition(
                type="Ready",
                status="Unknown",
                last_transition_time=now,
            )
        )

        assert condition == NodeCondition(
            type=NodeConditionType.READY, status=None, transition_time=now
        )

    def test_from_model_status_invalid(self) -> None:
        with pytest.raises(ValueError, match="Invalid status 'Invalid'"):
            NodeCondition.from_model(
                V1NodeCondition(
                    type="Ready",
                    status="Invalid",
                    last_transition_time=datetime.now(),
                )
            )


class TestNodeStatus:
    def test_from_model(self) -> None:
        now = datetime.now(UTC)
        status = NodeStatus.from_model(
            V1NodeStatus(
                allocatable={},
                conditions=[
                    V1NodeCondition(
                        type="Ready",
                        status="True",
                        last_transition_time=now,
                    )
                ],
            )
        )

        assert status == NodeStatus(
            allocatable_resources=NodeResources(),
            conditions=[
                NodeCondition(
                    type=NodeConditionType.READY, status=True, transition_time=now
                )
            ],
        )

    def test_is_ready_true(self) -> None:
        status = NodeStatus(
            allocatable_resources=NodeResources(1, 4096),
            conditions=[
                NodeCondition(
                    type=NodeConditionType.READY,
                    status=True,
                    transition_time=datetime.now(),
                )
            ],
        )

        assert status.is_ready is True

    def test_is_ready_false(self) -> None:
        status = NodeStatus(
            allocatable_resources=NodeResources(1, 4096),
            conditions=[
                NodeCondition(
                    type=NodeConditionType.READY,
                    status=False,
                    transition_time=datetime.now(),
                )
            ],
        )

        assert status.is_ready is False

    def test_is_ready_unknown(self) -> None:
        status = NodeStatus(
            allocatable_resources=NodeResources(1, 4096),
            conditions=[
                NodeCondition(
                    type=NodeConditionType.READY,
                    status=None,
                    transition_time=datetime.now(),
                )
            ],
        )

        assert status.is_ready is False


class TestNodeResources:
    def test_from_primitive(self) -> None:
        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4096Mi"})

        assert resources == NodeResources(cpu=1, memory=4096 * 2**20)

    def test_from_primitive_default(self) -> None:
        resources = NodeResources.from_primitive({})

        assert resources == NodeResources(cpu=0, memory=0)

    def test_from_primitive_cpu(self) -> None:
        resources = NodeResources.from_primitive({"cpu": "1000m", "memory": "4096Mi"})

        assert resources == NodeResources(cpu=1, memory=4096 * 2**20)

    def test_from_primitive_memory(self) -> None:
        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4294967296"})
        assert resources == NodeResources(cpu=1, memory=4294967296)

        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4194304Ki"})
        assert resources == NodeResources(cpu=1, memory=4194304 * 2**10)

        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4096Mi"})
        assert resources == NodeResources(cpu=1, memory=4096 * 2**20)

        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4Gi"})
        assert resources == NodeResources(cpu=1, memory=4 * 2**30)

        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4Ti"})
        assert resources == NodeResources(cpu=1, memory=4 * 2**40)

        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4000000k"})
        assert resources == NodeResources(cpu=1, memory=4000000 * 10**3)

        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4000M"})
        assert resources == NodeResources(cpu=1, memory=4000 * 10**6)

        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4G"})
        assert resources == NodeResources(cpu=1, memory=4 * 10**9)

        resources = NodeResources.from_primitive({"cpu": "1", "memory": "4T"})
        assert resources == NodeResources(cpu=1, memory=4 * 10**12)

        with pytest.raises(ValueError, match="'4Pi' memory format is not supported"):
            NodeResources.from_primitive({"cpu": "1", "memory": "4Pi"})

    def test_from_primitive_with_gpu(self) -> None:
        resources = NodeResources.from_primitive(
            {"cpu": "1", "memory": "4096Mi", "nvidia.com/gpu": "1"}
        )

        assert resources == NodeResources(cpu=1, memory=4096 * 2**20, nvidia_gpu=1)

    def test_invalid_cpu(self) -> None:
        with pytest.raises(ValueError, match="Invalid cpu: -1"):
            NodeResources(cpu=-1, memory=4096, nvidia_gpu=1)

    def test_invalid_memory(self) -> None:
        with pytest.raises(ValueError, match="Invalid memory: -4096"):
            NodeResources(cpu=1, memory=-4096, nvidia_gpu=1)

    def test_invalid_gpu(self) -> None:
        with pytest.raises(ValueError, match="Invalid nvidia gpu: -1"):
            NodeResources(cpu=1, memory=4096, nvidia_gpu=-1)

    def test_are_sufficient(self) -> None:
        r = NodeResources(cpu=1, memory=4096, nvidia_gpu=1)

        pod = PodDescriptor(name="job", image="job")
        assert r.are_sufficient(pod) is True

        pod = PodDescriptor(
            name="job",
            image="job",
            resources=Resources(cpu=1, memory=4096, nvidia_gpu=1),
        )
        assert r.are_sufficient(pod) is True

        pod = PodDescriptor(
            name="job", image="job", resources=Resources(cpu=0.1, memory=128)
        )
        assert r.are_sufficient(pod) is True

        pod = PodDescriptor(
            name="job", image="job", resources=Resources(cpu=1.1, memory=128)
        )
        assert r.are_sufficient(pod) is False

        pod = PodDescriptor(
            name="job", image="job", resources=Resources(cpu=0.1, memory=4097)
        )
        assert r.are_sufficient(pod) is False

        pod = PodDescriptor(
            name="job",
            image="job",
            resources=Resources(cpu=0.1, memory=128, nvidia_gpu=2),
        )
        assert r.are_sufficient(pod) is False


class TestNode:
    def test_from_model(self) -> None:
        node = Node.from_model(
            V1Node(
                metadata=V1ObjectMeta(name="minikube"),
                status=V1NodeStatus(allocatable={"cpu": "1", "memory": "4096Mi"}),
            )
        )

        assert node == Node(
            name="minikube",
            status=NodeStatus(allocatable_resources=NodeResources(cpu=1, memory=4096)),
        )

    def test_from_model_with_labels(self) -> None:
        node = Node.from_model(
            V1Node(
                metadata=V1ObjectMeta(name="minikube", labels={"job": "true"}),
                status=V1NodeStatus(allocatable={"cpu": "1", "memory": "4096Mi"}),
            )
        )

        assert node == Node(
            name="minikube",
            labels={"job": "true"},
            status=NodeStatus(allocatable_resources=NodeResources(cpu=1, memory=4096)),
        )

    def test_get_free_resources(self) -> None:
        node = Node(
            name="minikube",
            status=NodeStatus(
                allocatable_resources=NodeResources(cpu=1, memory=1024, nvidia_gpu=1)
            ),
        )

        free = node.get_free_resources(NodeResources(cpu=0.1, memory=128))

        assert free == NodeResources(cpu=0.9, memory=896, nvidia_gpu=1)

    def test_get_free_resources_error(self) -> None:
        node = Node(
            name="minikube",
            status=NodeStatus(
                allocatable_resources=NodeResources(cpu=1, memory=1024, nvidia_gpu=1)
            ),
        )

        with pytest.raises(ValueError, match="Invalid cpu"):
            node.get_free_resources(NodeResources(cpu=1.1, memory=128))


PodFactory = Callable[..., PodDescriptor]


@pytest.fixture
async def pod_factory() -> PodFactory:
    def _create(
        name: str | None = None,
        labels: dict[str, str] | None = None,
        cpu: float = 0.1,
        memory: int = 128,
        gpu: int = 1,
        idle: bool = True,
    ) -> PodDescriptor:
        labels = labels or {}
        if idle:
            labels["platform.neuromation.io/idle"] = "true"
        return PodDescriptor(
            name=name or f"pod-{uuid.uuid4()}",
            labels=labels,
            image="gcr.io/google_containers/pause:3.1",
            resources=Resources(cpu=cpu, memory=memory, nvidia_gpu=gpu),
        )

    return _create


class TestKubePreemption:
    @pytest.fixture
    def preemption(self) -> KubePreemption:
        return KubePreemption()

    def test_single_pod(
        self, preemption: KubePreemption, pod_factory: PodFactory
    ) -> None:
        idle_pod = pod_factory()
        assert idle_pod.resources
        resources = NodeResources(
            cpu=idle_pod.resources.cpu, memory=idle_pod.resources.memory
        )

        pods = preemption.get_pods_to_preempt(resources, [idle_pod])

        assert pods == [idle_pod]

    async def test_multiple_pods(
        self, preemption: KubePreemption, pod_factory: PodFactory
    ) -> None:
        idle_pod1 = pod_factory()
        idle_pod2 = pod_factory()
        assert idle_pod1.resources
        assert idle_pod2.resources
        resources = NodeResources(
            cpu=idle_pod1.resources.cpu + idle_pod2.resources.cpu,
            memory=idle_pod1.resources.memory + idle_pod2.resources.memory,
        )

        pods = preemption.get_pods_to_preempt(resources, [idle_pod1, idle_pod2])

        assert pods == [idle_pod1, idle_pod2]

    async def test_cpu_part_of_single_pod(
        self, preemption: KubePreemption, pod_factory: PodFactory
    ) -> None:
        idle_pod = pod_factory()
        assert idle_pod.resources
        resources = NodeResources(
            cpu=idle_pod.resources.cpu / 2, memory=idle_pod.resources.memory
        )

        pods = preemption.get_pods_to_preempt(resources, [idle_pod])

        assert pods == [idle_pod]

    async def test_memory_part_of_single_pod(
        self, preemption: KubePreemption, pod_factory: PodFactory
    ) -> None:
        idle_pod = pod_factory()
        assert idle_pod.resources
        resources = NodeResources(
            cpu=idle_pod.resources.cpu, memory=idle_pod.resources.memory // 2
        )

        pods = preemption.get_pods_to_preempt(resources, [idle_pod])

        assert pods == [idle_pod]

    async def test_part_of_multiple_pods(
        self, preemption: KubePreemption, pod_factory: PodFactory
    ) -> None:
        idle_pod1 = pod_factory()
        idle_pod2 = pod_factory()
        idle_pod3 = pod_factory()
        assert idle_pod1.resources
        assert idle_pod2.resources
        resources = NodeResources(
            cpu=idle_pod1.resources.cpu + idle_pod2.resources.cpu / 2,
            memory=idle_pod1.resources.memory + idle_pod2.resources.memory // 2,
        )

        pods = preemption.get_pods_to_preempt(
            resources, [idle_pod1, idle_pod2, idle_pod3]
        )

        assert pods == [idle_pod1, idle_pod2]

    async def test_pods_ordered_by_distance(
        self, preemption: KubePreemption, pod_factory: PodFactory
    ) -> None:
        idle_pod1 = pod_factory(cpu=0.1, memory=64)
        idle_pod2 = pod_factory(cpu=0.1, memory=192)
        assert idle_pod1.resources
        assert idle_pod2.resources
        resources = NodeResources(cpu=0.2, memory=256)

        pods = preemption.get_pods_to_preempt(resources, [idle_pod1, idle_pod2])

        assert pods == [idle_pod2, idle_pod1]

    async def test_pod_with_least_resources(
        self, preemption: KubePreemption, pod_factory: PodFactory
    ) -> None:
        idle_pod1 = pod_factory(cpu=0.1)
        idle_pod2 = pod_factory(cpu=0.2)
        assert idle_pod1.resources
        resources = NodeResources(cpu=0.09, memory=idle_pod1.resources.memory)

        pods = preemption.get_pods_to_preempt(resources, [idle_pod1, idle_pod2])

        assert pods == [idle_pod1]

    async def test_no_pods(self, preemption: KubePreemption) -> None:
        resources = NodeResources(cpu=0.1, memory=128)

        pods = preemption.get_pods_to_preempt(resources, [])

        assert pods == []

    async def test_not_enough_pods(
        self, preemption: KubePreemption, pod_factory: PodFactory
    ) -> None:
        idle_pod = pod_factory(cpu=0.1, memory=128)
        resources = NodeResources(cpu=0.2, memory=256)

        pods = preemption.get_pods_to_preempt(resources, [idle_pod])

        assert pods == []
