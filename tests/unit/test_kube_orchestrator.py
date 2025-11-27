from __future__ import annotations

from datetime import datetime
from pathlib import PurePath
from typing import Any

import pytest
from apolo_kube_client import (
    V1Affinity,
    V1Container,
    V1ContainerPort,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1HTTPGetAction,
    V1HTTPIngressPath,
    V1HTTPIngressRuleValue,
    V1Ingress,
    V1IngressBackend,
    V1IngressRule,
    V1IngressServiceBackend,
    V1IngressSpec,
    V1LabelSelector,
    V1LabelSelectorRequirement,
    V1LocalObjectReference,
    V1NodeAffinity,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
    V1ObjectMeta,
    V1Pod,
    V1PodAffinity,
    V1PodAffinityTerm,
    V1PodSpec,
    V1PodStatus,
    V1Probe,
    V1ResourceRequirements,
    V1SecretKeySelector,
    V1SecretVolumeSource,
    V1SecurityContext,
    V1Service,
    V1ServiceBackendPort,
    V1ServicePort,
    V1ServiceSpec,
    V1Toleration,
    V1Volume,
    V1VolumeMount,
    V1WeightedPodAffinityTerm,
)
from neuro_config_client import OrchestratorConfig
from yarl import URL

from platform_api.config import RegistryConfig
from platform_api.orchestrator.job import JobStatusItem, JobStatusReason
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    ContainerTPUResource,
    ContainerVolume,
    JobRequest,
    JobStatus,
    Secret,
    SecretContainerVolume,
)
from platform_api.orchestrator.kube_client import (
    ContainerStatus,
    Ingress,
    Resources,
    SecretEnvVar,
    SecretRef,
    SecretVolume,
    ServiceType,
    SharedMemoryVolume,
    Volume,
    VolumeMount,
)
from platform_api.orchestrator.kube_config import KubeConfig
from platform_api.orchestrator.kube_orchestrator import (
    IngressRule,
    JobStatusItemFactory,
    KubeOrchestrator,
    LabelSelectorMatchExpression,
    LabelSelectorTerm,
    NodeAffinity,
    PodAffinity,
    PodAffinityTerm,
    PodDescriptor,
    PodPreferredSchedulingTerm,
    PodStatus,
    Service,
    Toleration,
)


class TestVolume:
    def test_create_mount_shared_with_mount_sub_path(self) -> None:
        volume = SharedMemoryVolume("testvolume")
        container_volume = ContainerVolume(
            uri=URL("storage://"),
            dst_path=PurePath("/dev/shm"),
        )
        mount = volume.create_mount(container_volume, PurePath("sub/dir"))
        assert mount.volume == volume
        assert mount.mount_path == PurePath("/dev/shm/sub/dir")
        assert mount.sub_path == PurePath()
        assert not mount.read_only


class TestAbstractVolume:
    def test_create_mount_for_abstract_volume_should_fail(self) -> None:
        with pytest.raises(
            NotImplementedError, match="Cannot create mount for abstract Volume type."
        ):
            container_volume = ContainerVolume(
                uri=URL("storage://host/path/to/dir"),
                dst_path=PurePath("/container/path/to/dir"),
            )
            Volume("testvolume").create_mount(container_volume)


class TestSecretVolume:
    def test_to_model_no_items(self) -> None:
        secret_name = "project--alice--secrets"
        volume = SecretVolume("testvolume", k8s_secret_name=secret_name)
        assert volume.to_model() == V1Volume(
            name="testvolume",
            secret=V1SecretVolumeSource(secret_name=secret_name, default_mode=0o400),
        )

    def test_create_secret_mounts(self) -> None:
        secret_name = "project--alice--secrets"
        volume = SecretVolume("testvolume", k8s_secret_name=secret_name)
        container_volumes = [
            SecretContainerVolume.create(
                "secret://clustername/alice/sec1", PurePath("/etc/foo/file1.txt")
            ),
            SecretContainerVolume.create(
                "secret://clustername/alice/sec2", PurePath("/etc/foo/file2.txt")
            ),
        ]
        mounts = [volume.create_secret_mount(vol) for vol in container_volumes]

        assert mounts == [
            VolumeMount(
                volume=volume,
                mount_path=PurePath("/etc/foo/file1.txt"),
                sub_path=PurePath("sec1"),
                read_only=True,
            ),
            VolumeMount(
                volume=volume,
                mount_path=PurePath("/etc/foo/file2.txt"),
                sub_path=PurePath("sec2"),
                read_only=True,
            ),
        ]


class TestSecretEnvVar:
    def test_to_model(self) -> None:
        sec = Secret.create("secret://test-cluster/test-user/sec1")
        sec_env_var = SecretEnvVar.create("sec-name", secret=sec)
        assert sec_env_var.to_model() == V1EnvVar(
            name="sec-name",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name="project--test-user--secrets",
                    key="sec1",
                )
            ),
        )


class TestVolumeMount:
    def test_to_model(self) -> None:
        volume = SharedMemoryVolume(name="testvolume")
        mount = VolumeMount(
            volume=volume,
            mount_path=PurePath("/dst"),
            sub_path=PurePath("/src"),
            read_only=True,
        )
        assert mount.to_model() == V1VolumeMount(
            name="testvolume",
            mount_path="/dst",
            sub_path="/src",
            read_only=True,
        )


class TestPodDescriptor:
    def test_can_be_scheduled(self) -> None:
        pod = PodDescriptor(name="testname", image="testimage")

        assert pod.can_be_scheduled({"node-pool": "cpu"}) is True

    def test_can_be_scheduled_with_node_affinity(self) -> None:
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            node_affinity=NodeAffinity(
                required=[
                    LabelSelectorTerm(
                        [LabelSelectorMatchExpression.create_in("node-pool", "cpu")]
                    )
                ]
            ),
        )

        assert pod.can_be_scheduled({"node-pool": "cpu"}) is True
        assert pod.can_be_scheduled({"node-pool": "gpu"}) is False

    def test_can_be_scheduled_with_node_selector(self) -> None:
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            node_selector={"job": "true", "node-pool": "cpu"},
        )

        assert pod.can_be_scheduled({"job": "true", "node-pool": "cpu"}) is True
        assert pod.can_be_scheduled({"node-pool": "cpu"}) is False
        assert pod.can_be_scheduled({"job": "true"}) is False

    def test_can_be_scheduled_with_node_affinity_and_selector(self) -> None:
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            node_selector={"job": "true"},
            node_affinity=NodeAffinity(
                required=[
                    LabelSelectorTerm(
                        [LabelSelectorMatchExpression.create_in("node-pool", "cpu")]
                    )
                ]
            ),
        )

        assert pod.can_be_scheduled({"job": "true", "node-pool": "cpu"}) is True
        assert pod.can_be_scheduled({"node-pool": "cpu"}) is False
        assert pod.can_be_scheduled({"job": "true"}) is False

    def test_to_model_defaults(self) -> None:
        pod = PodDescriptor(name="testname", image="testimage")
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_model() == V1Pod(
            metadata=V1ObjectMeta(name="testname"),
            spec=V1PodSpec(
                automount_service_account_token=False,
                containers=[
                    V1Container(
                        name="testname",
                        image="testimage",
                        image_pull_policy="Always",
                        env=[],
                        volume_mounts=[],
                        termination_message_policy="FallbackToLogsOnError",
                        stdin=True,
                    )
                ],
                volumes=[],
                restart_policy="Never",
                image_pull_secrets=[],
                tolerations=[],
            ),
        )

    def test_to_model_privileged(self) -> None:
        pod = PodDescriptor(name="testname", image="testimage", privileged=True)
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_model() == V1Pod(
            metadata=V1ObjectMeta(name="testname"),
            spec=V1PodSpec(
                automount_service_account_token=False,
                containers=[
                    V1Container(
                        name="testname",
                        image="testimage",
                        image_pull_policy="Always",
                        env=[],
                        volume_mounts=[],
                        termination_message_policy="FallbackToLogsOnError",
                        stdin=True,
                        security_context=V1SecurityContext(
                            privileged=True,
                        ),
                    )
                ],
                volumes=[],
                restart_policy="Never",
                image_pull_secrets=[],
                tolerations=[],
            ),
        )

    def test_to_model(self) -> None:
        tolerations = [
            Toleration(key="testkey", value="testvalue", effect="NoSchedule")
        ]
        node_affinity = NodeAffinity(
            required=[
                LabelSelectorTerm(
                    [LabelSelectorMatchExpression.create_exists("testkey")]
                )
            ]
        )
        pod_affinity = PodAffinity(
            preferred=[
                PodPreferredSchedulingTerm(
                    weight=150,
                    pod_affinity_term=PodAffinityTerm(
                        topologyKey="sometopologykey",
                        namespaces=["some", "namespace"],
                        label_selector=LabelSelectorTerm(
                            match_expressions=[
                                LabelSelectorMatchExpression.create_exists("keya"),
                                LabelSelectorMatchExpression.create_in(
                                    "keyb", "v1", "v2"
                                ),
                            ]
                        ),
                    ),
                )
            ]
        )
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            env={"TESTVAR": "testvalue"},
            resources=Resources(cpu=0.5, memory=1024 * 10**6, nvidia_gpu=1),
            port=1234,
            tty=True,
            node_selector={"label": "value"},
            tolerations=tolerations,
            node_affinity=node_affinity,
            pod_affinity=pod_affinity,
            labels={"testlabel": "testvalue"},
            annotations={"testa": "testv"},
            working_dir="/working/dir",
        )
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_model() == V1Pod(
            metadata=V1ObjectMeta(
                name="testname",
                labels={"testlabel": "testvalue"},
                annotations={"testa": "testv"},
            ),
            spec=V1PodSpec(
                automount_service_account_token=False,
                containers=[
                    V1Container(
                        name="testname",
                        image="testimage",
                        image_pull_policy="Always",
                        env=[V1EnvVar(name="TESTVAR", value="testvalue")],
                        volume_mounts=[],
                        resources=V1ResourceRequirements(
                            requests={
                                "cpu": "500m",
                                "memory": "1024000000",
                                "nvidia.com/gpu": "1",
                            },
                            limits={
                                "cpu": "500m",
                                "memory": "1024000000",
                                "nvidia.com/gpu": "1",
                            },
                        ),
                        ports=[V1ContainerPort(container_port=1234)],
                        termination_message_policy="FallbackToLogsOnError",
                        stdin=True,
                        tty=True,
                        working_dir="/working/dir",
                    )
                ],
                volumes=[],
                restart_policy="Never",
                image_pull_secrets=[],
                node_selector={"label": "value"},
                tolerations=[
                    V1Toleration(
                        key="testkey",
                        operator="Equal",
                        value="testvalue",
                        effect="NoSchedule",
                    ),
                ],
                affinity=V1Affinity(
                    node_affinity=V1NodeAffinity(
                        required_during_scheduling_ignored_during_execution=V1NodeSelector(
                            node_selector_terms=[
                                V1NodeSelectorTerm(
                                    match_expressions=[
                                        V1NodeSelectorRequirement(
                                            key="testkey", operator="Exists", values=[]
                                        )
                                    ]
                                )
                            ]
                        )
                    ),
                    pod_affinity=V1PodAffinity(
                        preferred_during_scheduling_ignored_during_execution=[
                            V1WeightedPodAffinityTerm(
                                weight=150,
                                pod_affinity_term=V1PodAffinityTerm(
                                    topology_key="sometopologykey",
                                    namespaces=["some", "namespace"],
                                    label_selector=V1LabelSelector(
                                        match_expressions=[
                                            V1LabelSelectorRequirement(
                                                key="keya", operator="Exists"
                                            ),
                                            V1LabelSelectorRequirement(
                                                key="keyb",
                                                operator="In",
                                                values=["v1", "v2"],
                                            ),
                                        ]
                                    ),
                                ),
                            )
                        ],
                    ),
                ),
            ),
        )

    def test_to_model_readiness_probe_http(self) -> None:
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            env={"TESTVAR": "testvalue"},
            resources=Resources(cpu=0.5, memory=1024 * 10**6, nvidia_gpu=1),
            port=1234,
            readiness_probe=True,
        )
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_model() == V1Pod(
            metadata=V1ObjectMeta(name="testname"),
            spec=V1PodSpec(
                automount_service_account_token=False,
                containers=[
                    V1Container(
                        name="testname",
                        image="testimage",
                        image_pull_policy="Always",
                        env=[V1EnvVar(name="TESTVAR", value="testvalue")],
                        volume_mounts=[],
                        resources=V1ResourceRequirements(
                            requests={
                                "cpu": "500m",
                                "memory": "1024000000",
                                "nvidia.com/gpu": "1",
                            },
                            limits={
                                "cpu": "500m",
                                "memory": "1024000000",
                                "nvidia.com/gpu": "1",
                            },
                        ),
                        ports=[V1ContainerPort(container_port=1234)],
                        readiness_probe=V1Probe(
                            http_get=V1HTTPGetAction(port=1234, path="/"),
                            initial_delay_seconds=1,
                            period_seconds=1,
                        ),
                        termination_message_policy="FallbackToLogsOnError",
                        stdin=True,
                    )
                ],
                volumes=[],
                restart_policy="Never",
                image_pull_secrets=[],
                tolerations=[],
            ),
        )

    def test_to_model_no_ports(self) -> None:
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            env={"TESTVAR": "testvalue"},
            resources=Resources(cpu=0.5, memory=1024 * 10**6, nvidia_gpu=1),
        )
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_model() == V1Pod(
            metadata=V1ObjectMeta(name="testname"),
            spec=V1PodSpec(
                automount_service_account_token=False,
                containers=[
                    V1Container(
                        name="testname",
                        image="testimage",
                        image_pull_policy="Always",
                        env=[V1EnvVar(name="TESTVAR", value="testvalue")],
                        volume_mounts=[],
                        resources=V1ResourceRequirements(
                            requests={
                                "cpu": "500m",
                                "memory": "1024000000",
                                "nvidia.com/gpu": "1",
                            },
                            limits={
                                "cpu": "500m",
                                "memory": "1024000000",
                                "nvidia.com/gpu": "1",
                            },
                        ),
                        termination_message_policy="FallbackToLogsOnError",
                        stdin=True,
                    )
                ],
                volumes=[],
                restart_policy="Never",
                image_pull_secrets=[],
                tolerations=[],
            ),
        )

    def test_to_model_with_dev_shm(self) -> None:
        dev_shm = SharedMemoryVolume(name="dshm")
        container_volume = ContainerVolume(
            dst_path=PurePath("/dev/shm"),
            uri=URL("storage://"),
        )
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            env={"TESTVAR": "testvalue"},
            resources=Resources(cpu=0.5, memory=1024 * 10**6, nvidia_gpu=1),
            port=1234,
            volumes=[dev_shm],
            volume_mounts=[dev_shm.create_mount(container_volume)],
        )
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_model() == V1Pod(
            metadata=V1ObjectMeta(name="testname"),
            spec=V1PodSpec(
                automount_service_account_token=False,
                containers=[
                    V1Container(
                        name="testname",
                        image="testimage",
                        image_pull_policy="Always",
                        env=[V1EnvVar(name="TESTVAR", value="testvalue")],
                        volume_mounts=[
                            V1VolumeMount(
                                name="dshm",
                                mount_path="/dev/shm",
                                read_only=False,
                                sub_path=".",
                            )
                        ],
                        resources=V1ResourceRequirements(
                            requests={
                                "cpu": "500m",
                                "memory": "1024000000",
                                "nvidia.com/gpu": "1",
                            },
                            limits={
                                "cpu": "500m",
                                "memory": "1024000000",
                                "nvidia.com/gpu": "1",
                            },
                        ),
                        ports=[V1ContainerPort(container_port=1234)],
                        termination_message_policy="FallbackToLogsOnError",
                        stdin=True,
                    )
                ],
                volumes=[
                    V1Volume(
                        name="dshm", empty_dir=V1EmptyDirVolumeSource(medium="Memory")
                    )
                ],
                restart_policy="Never",
                image_pull_secrets=[],
                tolerations=[],
            ),
        )

    def test_from_job_request(self) -> None:
        container = Container(
            image="testimage",
            command="testcommand 123",
            working_dir="/working/dir",
            env={"TESTVAR": "testvalue"},
            resources=ContainerResources(
                cpu=1, memory=128 * 10**6, nvidia_gpu=1, amd_gpu=2
            ),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            job_request,
        )
        assert pod.name == job_request.job_id
        assert pod.image == "testimage"
        assert pod.args == ["testcommand", "123"]
        assert pod.env == {"TESTVAR": "testvalue"}
        assert pod.env_list == [{"name": "TESTVAR", "value": "testvalue"}]
        assert pod.resources == Resources(
            cpu=1, memory=128 * 10**6, nvidia_gpu=1, amd_gpu=2
        )
        assert pod.working_dir == "/working/dir"
        assert not pod.node_affinity
        assert not pod.pod_affinity

    def test_from_job_request_tpu(self) -> None:
        container = Container(
            image="testimage",
            resources=ContainerResources(
                cpu=1,
                memory=128 * 10**6,
                tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
            ),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(job_request)
        assert pod.annotations == {"tf-version.cloud-tpus.google.com": "1.14"}
        assert pod.priority_class_name is None

    def test_from_model_defaults(self) -> None:
        model = V1Pod(
            metadata=V1ObjectMeta(
                name="testname",
                creation_timestamp=datetime.fromisoformat("2019-06-20T11:03:32Z"),
            ),
            spec=V1PodSpec(
                containers=[V1Container(name="testname", image="testimage")]
            ),
        )
        pod = PodDescriptor.from_model(model)
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.status is not None
        assert pod.status.phase == ""
        assert pod.tolerations == []
        assert pod.priority_class_name is None
        assert pod.image_pull_secrets == []
        assert pod.node_name is None
        assert pod.command == []
        assert pod.args == []
        assert pod.tty is False
        assert pod.labels == {}

    def test_from_model(self) -> None:
        model = V1Pod(
            metadata=V1ObjectMeta(
                name="testname",
                creation_timestamp=datetime.fromisoformat("2019-06-20T11:03:32Z"),
                labels={"testlabel": "testvalue"},
            ),
            spec=V1PodSpec(
                containers=[
                    V1Container(
                        name="testname",
                        image="testimage",
                        tty=True,
                        stdin=True,
                        working_dir="/working/dir",
                    )
                ],
                tolerations=[
                    V1Toleration(
                        key="key1",
                        value="value1",
                        operator="Equals",
                        effect="NoSchedule",
                    ),
                    V1Toleration(key="key2", operator="Exists"),
                    V1Toleration(operator="Exists"),
                    V1Toleration(key="key3"),
                ],
                image_pull_secrets=[V1LocalObjectReference(name="secret")],
            ),
            status=V1PodStatus(phase="Running"),
        )
        pod = PodDescriptor.from_model(model)
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.status is not None
        assert pod.status.phase == "Running"
        assert pod.tolerations == [
            Toleration(
                key="key1", operator="Equals", value="value1", effect="NoSchedule"
            ),
            Toleration(key="key2", operator="Exists", value="", effect=""),
            Toleration(key="", operator="Exists", value="", effect=""),
            Toleration(key="key3", operator="Equal", value="", effect=""),
        ]
        assert pod.image_pull_secrets == [SecretRef("secret")]
        assert pod.node_name is None
        assert pod.command == []
        assert pod.args == []
        assert pod.tty is True
        assert pod.labels == {"testlabel": "testvalue"}
        assert pod.working_dir == "/working/dir"


class TestJobStatusItemFactory:
    @pytest.mark.parametrize(
        "phase, container_statuses, expected_status",
        (
            ("Succeeded", [], JobStatus.SUCCEEDED),
            ("Failed", [], JobStatus.FAILED),
            ("Unknown", [], JobStatus.FAILED),
            ("Running", [{"state": {"running": {}}}], JobStatus.RUNNING),
            ("NewPhase", [], JobStatus.PENDING),
        ),
    )
    def test_status(
        self,
        phase: str,
        container_statuses: list[dict[str, Any]],
        expected_status: JobStatus,
    ) -> None:
        payload: dict[str, Any] = {"phase": phase}
        if container_statuses:
            payload["containerStatuses"] = container_statuses
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(expected_status)

    def test_status_pending(self) -> None:
        payload = {"phase": "Pending"}
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(JobStatus.PENDING)

    def test_status_pending_creating(self) -> None:
        payload = {
            "phase": "Pending",
            "containerStatuses": [
                {"state": {"waiting": {"reason": "ContainerCreating"}}}
            ],
        }
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(
            JobStatus.PENDING, reason=JobStatusReason.CONTAINER_CREATING
        )

    def test_status_pending_running_no_reason(self) -> None:
        payload = {
            "phase": "Pending",
            "containerStatuses": [{"state": {"waiting": {}}}],
        }
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(JobStatus.PENDING)

    def test_status_pending_failure(self) -> None:
        payload = {
            "phase": "Pending",
            "containerStatuses": [
                {"state": {"waiting": {"reason": "SomeWeirdReason"}}}
            ],
        }

        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(
            JobStatus.PENDING, reason="SomeWeirdReason"
        )

    def test_status_running_restarting(self) -> None:
        payload = {
            "phase": "Running",
            "containerStatuses": [
                {"state": {"waiting": {"reason": "SomeWeirdReason"}}}
            ],
        }

        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(
            JobStatus.RUNNING, reason="Restarting"
        )

    def test_status_failure(self) -> None:
        payload = {
            "phase": "Failed",
            "containerStatuses": [
                {
                    "state": {
                        "terminated": {
                            "reason": "Error",
                            "message": "Failed!",
                            "exitCode": 123,
                        }
                    }
                }
            ],
        }
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(
            JobStatus.FAILED,
            reason=JobStatusReason.ERROR,
            description="Failed!",
            exit_code=123,
        )

    def test_status_failure_no_message(self) -> None:
        payload = {
            "phase": "Failed",
            "containerStatuses": [
                {"state": {"terminated": {"reason": "Error", "exitCode": 1}}}
            ],
        }

        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(
            JobStatus.FAILED,
            reason=JobStatusReason.ERROR,
            description=None,
            exit_code=1,
        )

    def test_status_success(self) -> None:
        payload = {
            "phase": "Succeeded",
            "containerStatuses": [
                {
                    "state": {
                        "terminated": {
                            "reason": "Succeeded",
                            "message": "Everything is ok!",
                            "exitCode": 0,
                        }
                    }
                }
            ],
        }
        pod_status = PodStatus.from_primitive(payload)
        job_status_item = JobStatusItemFactory(pod_status).create()
        assert job_status_item == JobStatusItem.create(JobStatus.SUCCEEDED, exit_code=0)


class TestResources:
    def test_invalid_tpu(self) -> None:
        with pytest.raises(ValueError, match="invalid TPU configuration"):
            Resources(cpu=0.5, memory=1024 * 10**6, tpu_version="v2")

    def test_to_model(self) -> None:
        resources = Resources(cpu=0.5, memory=1024000000)
        assert resources.to_model() == V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1024000000"},
            limits={"cpu": "500m", "memory": "1024000000"},
        )

    def test_to_model_gpu(self) -> None:
        resources = Resources(cpu=0.5, memory=1024 * 10**6, nvidia_gpu=2)
        assert resources.to_model() == V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1024000000", "nvidia.com/gpu": "2"},
            limits={"cpu": "500m", "memory": "1024000000", "nvidia.com/gpu": "2"},
        )

    def test_to_model_tpu(self) -> None:
        resources = Resources(
            cpu=0.5, memory=1024 * 10**6, tpu_version="v2", tpu_cores=8
        )
        assert resources.to_model() == V1ResourceRequirements(
            requests={
                "cpu": "500m",
                "memory": "1024000000",
                "cloud-tpus.google.com/v2": "8",
            },
            limits={
                "cpu": "500m",
                "memory": "1024000000",
                "cloud-tpus.google.com/v2": "8",
            },
        )

    def test_to_model_with_memory_request(self) -> None:
        resources = Resources(
            cpu=1,
            memory=1024 * 10**6,
            memory_request=128 * 10**6,
        )

        assert resources.to_model() == V1ResourceRequirements(
            requests={
                "cpu": "1000m",
                "memory": "128000000",
            },
            limits={
                "cpu": "1000m",
                "memory": "1024000000",
            },
        )

    def test_from_container_resources(self) -> None:
        container_resources = ContainerResources(
            cpu=1, memory=128 * 10**6, nvidia_gpu=1, amd_gpu=2
        )
        resources = Resources.from_container_resources(container_resources)
        assert resources == Resources(
            cpu=1, memory=128 * 10**6, nvidia_gpu=1, amd_gpu=2
        )

    def test_from_container_resources_tpu(self) -> None:
        container_resources = ContainerResources(
            cpu=1,
            memory=128 * 10**6,
            tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
        )
        resources = Resources.from_container_resources(container_resources)
        assert resources == Resources(
            cpu=1, memory=128 * 10**6, tpu_version="v2", tpu_cores=8
        )

    @pytest.mark.parametrize("type_", ("v2", "v2-nan", "v2-", "-"))
    def test_from_container_resources_tpu_invalid_type(self, type_: str) -> None:
        container_resources = ContainerResources(
            cpu=1,
            memory=128 * 10**6,
            tpu=ContainerTPUResource(type=type_, software_version="1.14"),
        )
        with pytest.raises(ValueError, match=f"invalid TPU type format: '{type_}'"):
            Resources.from_container_resources(container_resources)

    def test_from_model(self) -> None:
        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1", "memory": "4096Mi"})
        )

        assert resources == Resources(cpu=1, memory=4096 * 2**20)

    def test_from_model_cpu(self) -> None:
        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1000m", "memory": "4096Mi"})
        )

        assert resources == Resources(cpu=1, memory=4096 * 2**20)

    def test_from_model_memory(self) -> None:
        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1", "memory": "4194304Ki"})
        )
        assert resources == Resources(cpu=1, memory=4194304 * 2**10)

        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1", "memory": "4096Mi"})
        )
        assert resources == Resources(cpu=1, memory=4096 * 2**20)

        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1", "memory": "4Gi"})
        )
        assert resources == Resources(cpu=1, memory=4 * 2**30)

        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1", "memory": "4Ti"})
        )
        assert resources == Resources(cpu=1, memory=4 * 2**40)

        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1", "memory": "4000000k"})
        )
        assert resources == Resources(cpu=1, memory=4000000 * 10**3)

        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1", "memory": "4000M"})
        )
        assert resources == Resources(cpu=1, memory=4000 * 10**6)

        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1", "memory": "4G"})
        )
        assert resources == Resources(cpu=1, memory=4 * 10**9)

        resources = Resources.from_model(
            V1ResourceRequirements(requests={"cpu": "1", "memory": "4T"})
        )
        assert resources == Resources(cpu=1, memory=4 * 10**12)

        with pytest.raises(ValueError, match="'4Pi' memory format is not supported"):
            Resources.from_model(
                V1ResourceRequirements(requests={"cpu": "1", "memory": "4Pi"})
            )

    def test_from_model_gpu(self) -> None:
        resources = Resources.from_model(
            V1ResourceRequirements(
                requests={"cpu": "1", "memory": "4096Mi", "nvidia.com/gpu": "1"}
            )
        )

        assert resources == Resources(cpu=1, memory=4096 * 2**20, nvidia_gpu=1)

    def test_from_model_tpu(self) -> None:
        resources = Resources.from_model(
            V1ResourceRequirements(
                requests={
                    "cpu": "1",
                    "memory": "4096Mi",
                    "cloud-tpus.google.com/v2": "8",
                }
            )
        )

        assert resources == Resources(
            cpu=1, memory=4096 * 2**20, tpu_cores=8, tpu_version="v2"
        )

    def test_from_model_default(self) -> None:
        resources = Resources.from_model(V1ResourceRequirements())

        assert resources == Resources(cpu=0, memory=0)


class TestIngressV1Rule:
    def test_from_model_host(self) -> None:
        rule = IngressRule.from_model(V1IngressRule(host="testhost"))
        assert rule == IngressRule(host="testhost")

    def test_from_model_no_paths(self) -> None:
        rule = IngressRule.from_model(
            V1IngressRule(host="testhost", http=V1HTTPIngressRuleValue(paths=[]))
        )
        assert rule == IngressRule(host="testhost")

    def test_from_model_no_service(self) -> None:
        rule = IngressRule.from_model(
            V1IngressRule(
                host="testhost",
                http=V1HTTPIngressRuleValue(
                    paths=[
                        V1HTTPIngressPath(backend=V1IngressBackend(), path_type="Exact")
                    ]
                ),
            )
        )
        assert rule == IngressRule(host="testhost")

    def test_from_model(self) -> None:
        rule = IngressRule.from_model(
            V1IngressRule(
                host="testhost",
                http=V1HTTPIngressRuleValue(
                    paths=[
                        V1HTTPIngressPath(
                            backend=V1IngressBackend(
                                service=V1IngressServiceBackend(
                                    name="testname",
                                    port=V1ServiceBackendPort(number=1234),
                                ),
                            ),
                            path_type="Exact",
                        )
                    ]
                ),
            )
        )
        assert rule == IngressRule(
            host="testhost", service_name="testname", service_port=1234
        )

    def test_to_model_no_service(self) -> None:
        rule = IngressRule(host="testhost")
        assert rule.to_model() == V1IngressRule(host="testhost")

    def test_to_primitive(self) -> None:
        rule = IngressRule(host="testhost", service_name="testname", service_port=1234)
        assert rule.to_model() == V1IngressRule(
            host="testhost",
            http=V1HTTPIngressRuleValue(
                paths=[
                    V1HTTPIngressPath(
                        path_type="ImplementationSpecific",
                        backend=V1IngressBackend(
                            service=V1IngressServiceBackend(
                                name="testname", port=V1ServiceBackendPort(number=1234)
                            )
                        ),
                    )
                ]
            ),
        )


class TestIngressV1:
    def test_from_model_no_rules(self) -> None:
        ingress = Ingress.from_model(
            V1Ingress(
                metadata=V1ObjectMeta(name="testingress"),
                spec=V1IngressSpec(rules=[]),
            )
        )
        assert ingress == Ingress(name="testingress", rules=[])

    def test_from_model_with_ingress_class_annotation(self) -> None:
        ingress = Ingress.from_model(
            V1Ingress(
                metadata=V1ObjectMeta(
                    name="testingress",
                    annotations={"kubernetes.io/ingress.class": "traefik"},
                ),
                spec=V1IngressSpec(rules=[V1IngressRule(host="testhost")]),
            )
        )
        assert ingress == Ingress(
            name="testingress",
            ingress_class="traefik",
            rules=[IngressRule(host="testhost")],
            annotations={"kubernetes.io/ingress.class": "traefik"},
        )

    def test_from_model_with_ingress_class(self) -> None:
        ingress = Ingress.from_model(
            V1Ingress(
                metadata=V1ObjectMeta(name="testingress"),
                spec=V1IngressSpec(
                    ingress_class_name="traefik",
                    rules=[V1IngressRule(host="testhost")],
                ),
            )
        )
        assert ingress == Ingress(
            name="testingress",
            ingress_class="traefik",
            rules=[IngressRule(host="testhost")],
        )

    def test_from_model(self) -> None:
        ingress = Ingress.from_model(
            V1Ingress(
                metadata=V1ObjectMeta(name="testingress"),
                spec=V1IngressSpec(rules=[V1IngressRule(host="testhost")]),
            )
        )
        assert ingress == Ingress(
            name="testingress", rules=[IngressRule(host="testhost")]
        )

    def test_to_model_no_rules(self) -> None:
        ingress = Ingress(name="testingress")
        assert ingress.to_model() == V1Ingress(
            metadata=V1ObjectMeta(name="testingress", annotations={}),
            spec=V1IngressSpec(rules=[]),
        )

    def test_to_model_with_ingress_class(self) -> None:
        ingress = Ingress(name="testingress", ingress_class="traefik")
        assert ingress.to_model() == V1Ingress(
            metadata=V1ObjectMeta(name="testingress", annotations={}),
            spec=V1IngressSpec(ingress_class_name="traefik", rules=[]),
        )

    def test_to_model_with_ingress_class_annotation_removed(self) -> None:
        ingress = Ingress(
            name="testingress",
            ingress_class="traefik",
            annotations={"kubernetes.io/ingress.class": "traefik"},
        )
        assert ingress.to_model() == V1Ingress(
            metadata=V1ObjectMeta(name="testingress", annotations={}),
            spec=V1IngressSpec(ingress_class_name="traefik", rules=[]),
        )

    def test_to_model(self) -> None:
        ingress = Ingress(
            name="testingress",
            rules=[
                IngressRule(host="host1", service_name="testservice", service_port=1234)
            ],
        )
        assert ingress.to_model() == V1Ingress(
            metadata=V1ObjectMeta(name="testingress", annotations={}),
            spec=V1IngressSpec(
                rules=[
                    V1IngressRule(
                        host="host1",
                        http=V1HTTPIngressRuleValue(
                            paths=[
                                V1HTTPIngressPath(
                                    path_type="ImplementationSpecific",
                                    backend=V1IngressBackend(
                                        service=V1IngressServiceBackend(
                                            name="testservice",
                                            port=V1ServiceBackendPort(number=1234),
                                        )
                                    ),
                                )
                            ]
                        ),
                    )
                ]
            ),
        )

    def test_to_model_with_annotations(self) -> None:
        ingress = Ingress(
            name="testingress",
            rules=[
                IngressRule(host="host1", service_name="testservice", service_port=1234)
            ],
            annotations={"key1": "value1"},
        )
        assert ingress.to_model() == V1Ingress(
            metadata=V1ObjectMeta(name="testingress", annotations={"key1": "value1"}),
            spec=V1IngressSpec(
                rules=[
                    V1IngressRule(
                        host="host1",
                        http=V1HTTPIngressRuleValue(
                            paths=[
                                V1HTTPIngressPath(
                                    path_type="ImplementationSpecific",
                                    backend=V1IngressBackend(
                                        service=V1IngressServiceBackend(
                                            name="testservice",
                                            port=V1ServiceBackendPort(number=1234),
                                        )
                                    ),
                                )
                            ]
                        ),
                    )
                ]
            ),
        )

    def test_to_model_with_labels(self) -> None:
        ingress = Ingress(
            name="testingress",
            rules=[
                IngressRule(host="host1", service_name="testservice", service_port=1234)
            ],
            labels={"test-label-1": "test-value-1", "test-label-2": "test-value-2"},
        )
        assert ingress.to_model() == V1Ingress(
            metadata=V1ObjectMeta(
                name="testingress",
                annotations={},
                labels={
                    "test-label-1": "test-value-1",
                    "test-label-2": "test-value-2",
                },
            ),
            spec=V1IngressSpec(
                rules=[
                    V1IngressRule(
                        host="host1",
                        http=V1HTTPIngressRuleValue(
                            paths=[
                                V1HTTPIngressPath(
                                    path_type="ImplementationSpecific",
                                    backend=V1IngressBackend(
                                        service=V1IngressServiceBackend(
                                            name="testservice",
                                            port=V1ServiceBackendPort(number=1234),
                                        )
                                    ),
                                )
                            ]
                        ),
                    )
                ]
            ),
        )


class TestService:
    @pytest.fixture
    def service_model(self) -> V1Service:
        return V1Service(
            metadata=V1ObjectMeta(name="testservice"),
            spec=V1ServiceSpec(
                type="ClusterIP",
                ports=[V1ServicePort(port=80, target_port=8080, name="http")],
                selector={"job": "testservice"},
            ),
        )

    @pytest.fixture
    def service_model_with_uid(self, service_model: V1Service) -> V1Service:
        ret = service_model.copy(deep=True)
        ret.metadata.uid = "test-uid"
        return ret

    def test_to_model(self, service_model: V1Service) -> None:
        service = Service(
            name="testservice",
            selector=service_model.spec.selector,
            target_port=8080,
        )
        assert service.to_model() == service_model

    def test_to_model_with_labels(self, service_model: V1Service) -> None:
        labels = {"label-name": "label-value"}
        expected_model = service_model.copy(deep=True)
        expected_model.metadata.labels = labels
        service = Service(
            name="testservice",
            selector=expected_model.spec.selector,
            target_port=8080,
            labels=labels,
        )
        assert service.to_model() == expected_model

    def test_to_model_load_balancer(self, service_model: V1Service) -> None:
        service = Service(
            name="testservice",
            selector=service_model.spec.selector,
            target_port=8080,
            service_type=ServiceType.LOAD_BALANCER,
        )
        service_model.spec.type = "LoadBalancer"
        assert service.to_model() == service_model

    def test_to_model_headless(self, service_model: V1Service) -> None:
        service = Service(
            name="testservice",
            selector=service_model.spec.selector,
            target_port=8080,
            cluster_ip="None",
        )
        service_model.spec.cluster_ip = "None"
        assert service.to_model() == service_model

    def test_from_model(self, service_model_with_uid: V1Service) -> None:
        service = Service.from_model(service_model_with_uid)
        assert service == Service(
            name="testservice",
            uid="test-uid",
            selector=service_model_with_uid.spec.selector,
            target_port=8080,
        )

    def test_from_model_with_labels(self, service_model_with_uid: V1Service) -> None:
        labels = {"label-name": "label-value"}
        input_model = service_model_with_uid.copy(deep=True)
        input_model.metadata.labels = labels
        service = Service.from_model(input_model)
        assert service == Service(
            name="testservice",
            uid="test-uid",
            selector=service_model_with_uid.spec.selector,
            target_port=8080,
            labels=labels,
        )

    def test_from_primitive_node_port(self, service_model_with_uid: V1Service) -> None:
        service_model_with_uid.spec.type = "NodePort"
        service = Service.from_model(service_model_with_uid)
        assert service == Service(
            name="testservice",
            uid="test-uid",
            selector=service_model_with_uid.spec.selector,
            target_port=8080,
            service_type=ServiceType.NODE_PORT,
        )

    def test_from_primitive_headless(self, service_model_with_uid: V1Service) -> None:
        service_model_with_uid.spec.cluster_ip = "None"
        service = Service.from_model(service_model_with_uid)
        assert service == Service(
            name="testservice",
            uid="test-uid",
            selector=service_model_with_uid.spec.selector,
            cluster_ip="None",
            target_port=8080,
        )

    def test_create_for_pod(self) -> None:
        pod = PodDescriptor(name="testpod", image="testimage", port=1234)
        service = Service.create_for_pod(pod=pod)
        assert service == Service(name="testpod", target_port=1234)

    def test_create_headless_for_pod(self) -> None:
        pod = PodDescriptor(name="testpod", image="testimage", port=1234)
        service = Service.create_headless_for_pod(pod=pod)
        assert service == Service(name="testpod", cluster_ip="None", target_port=1234)


class TestContainerStatus:
    def test_no_state(self) -> None:
        payload: dict[str, Any] = {"state": {}}
        status = ContainerStatus(payload)
        assert status.is_waiting
        assert status.reason is None
        assert status.message is None

    @pytest.mark.parametrize(
        "payload",
        (
            None,
            {},
            {"state": {}},
            {"state": {"waiting": {}}},
            {"state": {"waiting": {"reason": "ContainerCreating"}}},
        ),
    )
    def test_is_waiting_creating(self, payload: Any) -> None:
        status = ContainerStatus(payload)
        assert status.is_waiting
        assert status.is_creating
        assert not status.is_terminated

        with pytest.raises(AssertionError):
            _ = status.exit_code

    @pytest.mark.parametrize(
        "payload", ({"state": {"waiting": {"reason": "NOT CREATING"}}},)
    )
    def test_is_waiting_not_creating(self, payload: Any) -> None:
        status = ContainerStatus(payload)
        assert status.is_waiting
        assert not status.is_creating
        assert not status.is_terminated
        assert status.reason == "NOT CREATING"
        assert status.message is None

    @pytest.mark.parametrize(
        "payload", ({"state": {"running": {}}}, {"state": {"terminated": {}}})
    )
    def test_is_not_waiting(self, payload: Any) -> None:
        status = ContainerStatus(payload)
        assert not status.is_waiting
        assert not status.is_creating

    def test_is_terminated(self) -> None:
        payload = {
            "state": {
                "terminated": {"reason": "Error", "message": "Failed!", "exitCode": 123}
            }
        }
        status = ContainerStatus(payload)
        assert status.is_terminated
        assert status.reason == "Error"
        assert status.message == "Failed!"
        assert status.exit_code == 123


class TestKubeOrchestrator:
    @pytest.fixture
    def orchestrator(self) -> KubeOrchestrator:
        return KubeOrchestrator(
            cluster_name="default",
            registry_config=RegistryConfig(username="username", password="password"),
            orchestrator_config=OrchestratorConfig(
                job_hostname_template="{job_id}.default.org.neu.ro",
                job_fallback_hostname="default.org.neu.ro",
                job_schedule_timeout_s=300,
                job_schedule_scale_up_timeout_s=900,
                resource_pool_types=[],
                resource_presets=[],
            ),
            kube_config=KubeConfig(endpoint_url="https://kuberrnetes.svc"),
        )
