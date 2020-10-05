from pathlib import PurePath
from typing import Any, Dict, List
from unittest import mock

import pytest
from yarl import URL

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
    AlreadyExistsException,
    ContainerStatus,
    Ingress,
    Resources,
    SecretEnvVar,
    SecretRef,
    SecretVolume,
    ServiceType,
    SharedMemoryVolume,
    VolumeMount,
)
from platform_api.orchestrator.kube_orchestrator import (
    HostVolume,
    IngressRule,
    JobStatusItemFactory,
    NfsVolume,
    NodeAffinity,
    NodeSelectorRequirement,
    NodeSelectorTerm,
    PodDescriptor,
    PodStatus,
    PVCVolume,
    Service,
    Toleration,
    Volume,
)


class TestVolume:
    @pytest.mark.parametrize(
        "volume",
        (
            HostVolume("testvolume", path=PurePath("/host")),
            NfsVolume("testvolume", server="1.2.3.4", path=PurePath("/host")),
            PVCVolume("testvolume", claim_name="testclaim", path=PurePath("/host")),
        ),
    )
    def test_create_mount(self, volume: Volume) -> None:
        container_volume = ContainerVolume(
            uri=URL("storage://host"),
            src_path=PurePath("/host/path/to/dir"),
            dst_path=PurePath("/container/path/to/dir"),
        )
        mount = volume.create_mount(container_volume)
        assert mount.volume == volume
        assert mount.mount_path == PurePath("/container/path/to/dir")
        assert mount.sub_path == PurePath("path/to/dir")
        assert not mount.read_only


class TestAbstractVolume:
    def test_create_mount_for_abstract_volume_should_fail(self) -> None:
        with pytest.raises(NotImplementedError, match=""):
            container_volume = ContainerVolume(
                uri=URL("storage://host"),
                src_path=PurePath("/host/path/to/dir"),
                dst_path=PurePath("/container/path/to/dir"),
            )
            Volume("testvolume").create_mount(container_volume)


class TestHostVolume:
    def test_to_primitive(self) -> None:
        volume = HostVolume("testvolume", path=PurePath("/tmp"))
        assert volume.to_primitive() == {
            "name": "testvolume",
            "hostPath": {"path": "/tmp", "type": "Directory"},
        }


class TestNfsVolume:
    def test_to_primitive(self) -> None:
        volume = NfsVolume("testvolume", server="1.2.3.4", path=PurePath("/tmp"))
        assert volume.to_primitive() == {
            "name": "testvolume",
            "nfs": {"server": "1.2.3.4", "path": "/tmp"},
        }


class TestPVCVolume:
    def test_to_primitive(self) -> None:
        volume = PVCVolume("testvolume", claim_name="testclaim", path=PurePath("/tmp"))
        assert volume.to_primitive() == {
            "name": "testvolume",
            "persistentVolumeClaim": {"claimName": "testclaim"},
        }


class TestSecretVolume:
    def test_to_primitive_no_items(self) -> None:
        secret_name = "user--alice--secrets"
        volume = SecretVolume("testvolume", k8s_secret_name=secret_name)
        assert volume.to_primitive() == {
            "name": "testvolume",
            "secret": {"secretName": secret_name, "defaultMode": 0o400},
        }

    def test_create_secret_mounts(self) -> None:
        secret_name = "user--alice--secrets"
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
    def test_to_primitive(self) -> None:
        sec = Secret.create("secret://test-cluster/test-user/sec1")
        sec_env_var = SecretEnvVar.create("sec-name", secret=sec)
        assert sec_env_var.to_primitive() == {
            "name": "sec-name",
            "valueFrom": {
                "secretKeyRef": {"name": "user--test-user--secrets", "key": "sec1"}
            },
        }


class TestVolumeMount:
    def test_to_primitive(self) -> None:
        volume = HostVolume(name="testvolume", path=PurePath("/tmp"))
        mount = VolumeMount(
            volume=volume,
            mount_path=PurePath("/dst"),
            sub_path=PurePath("/src"),
            read_only=True,
        )
        assert mount.to_primitive() == {
            "name": "testvolume",
            "mountPath": "/dst",
            "subPath": "/src",
            "readOnly": True,
        }


class TestPodDescriptor:
    def test_to_primitive_defaults(self) -> None:
        pod = PodDescriptor(name="testname", image="testimage")
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_primitive() == {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": "testname"},
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [
                    {
                        "name": "testname",
                        "image": "testimage",
                        "imagePullPolicy": "Always",
                        "env": [],
                        "volumeMounts": [],
                        "terminationMessagePolicy": "FallbackToLogsOnError",
                        "stdin": True,
                    }
                ],
                "volumes": [],
                "restartPolicy": "Never",
                "imagePullSecrets": [],
                "tolerations": [],
            },
        }

    def test_to_primitive(self) -> None:
        tolerations = [
            Toleration(key="testkey", value="testvalue", effect="NoSchedule")
        ]
        node_affinity = NodeAffinity(
            required=[
                NodeSelectorTerm([NodeSelectorRequirement.create_exists("testkey")])
            ]
        )
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            env={"TESTVAR": "testvalue"},
            resources=Resources(cpu=0.5, memory=1024, gpu=1),
            port=1234,
            ssh_port=2222,
            tty=True,
            node_selector={"label": "value"},
            tolerations=tolerations,
            node_affinity=node_affinity,
            labels={"testlabel": "testvalue"},
            annotations={"testa": "testv"},
            priority_class_name="testpriority",
            working_dir="/working/dir",
        )
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_primitive() == {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": "testname",
                "labels": {"testlabel": "testvalue"},
                "annotations": {"testa": "testv"},
            },
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [
                    {
                        "name": "testname",
                        "image": "testimage",
                        "imagePullPolicy": "Always",
                        "env": [{"name": "TESTVAR", "value": "testvalue"}],
                        "volumeMounts": [],
                        "resources": {
                            "limits": {
                                "cpu": "500m",
                                "memory": "1024Mi",
                                "nvidia.com/gpu": 1,
                            }
                        },
                        "ports": [{"containerPort": 1234}, {"containerPort": 2222}],
                        "terminationMessagePolicy": "FallbackToLogsOnError",
                        "stdin": True,
                        "tty": True,
                        "workingDir": "/working/dir",
                    }
                ],
                "volumes": [],
                "restartPolicy": "Never",
                "imagePullSecrets": [],
                "nodeSelector": {"label": "value"},
                "tolerations": [
                    {
                        "key": "testkey",
                        "operator": "Equal",
                        "value": "testvalue",
                        "effect": "NoSchedule",
                    },
                    {
                        "key": "nvidia.com/gpu",
                        "operator": "Exists",
                        "value": "",
                        "effect": "NoSchedule",
                    },
                ],
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": mock.ANY
                    }
                },
                "priorityClassName": "testpriority",
            },
        }

    def test_to_primitive_readiness_probe_http(self) -> None:
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            env={"TESTVAR": "testvalue"},
            resources=Resources(cpu=0.5, memory=1024, gpu=1),
            port=1234,
            ssh_port=4321,
            readiness_probe=True,
        )
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_primitive() == {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": "testname"},
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [
                    {
                        "name": "testname",
                        "image": "testimage",
                        "imagePullPolicy": "Always",
                        "env": [{"name": "TESTVAR", "value": "testvalue"}],
                        "volumeMounts": [],
                        "resources": {
                            "limits": {
                                "cpu": "500m",
                                "memory": "1024Mi",
                                "nvidia.com/gpu": 1,
                            }
                        },
                        "ports": [{"containerPort": 1234}, {"containerPort": 4321}],
                        "readinessProbe": {
                            "httpGet": {"port": 1234, "path": "/"},
                            "initialDelaySeconds": 1,
                            "periodSeconds": 1,
                        },
                        "terminationMessagePolicy": "FallbackToLogsOnError",
                        "stdin": True,
                    }
                ],
                "volumes": [],
                "restartPolicy": "Never",
                "imagePullSecrets": [],
                "tolerations": [
                    {
                        "key": "nvidia.com/gpu",
                        "operator": "Exists",
                        "value": "",
                        "effect": "NoSchedule",
                    }
                ],
            },
        }

    def test_to_primitive_readiness_probe_ssh(self) -> None:
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            env={"TESTVAR": "testvalue"},
            resources=Resources(cpu=0.5, memory=1024, gpu=1),
            ssh_port=4321,
            readiness_probe=True,
        )
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_primitive() == {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": "testname"},
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [
                    {
                        "name": "testname",
                        "image": "testimage",
                        "imagePullPolicy": "Always",
                        "env": [{"name": "TESTVAR", "value": "testvalue"}],
                        "volumeMounts": [],
                        "resources": {
                            "limits": {
                                "cpu": "500m",
                                "memory": "1024Mi",
                                "nvidia.com/gpu": 1,
                            }
                        },
                        "ports": [{"containerPort": 4321}],
                        "readinessProbe": {
                            "tcpSocket": {"port": 4321},
                            "initialDelaySeconds": 1,
                            "periodSeconds": 1,
                        },
                        "terminationMessagePolicy": "FallbackToLogsOnError",
                        "stdin": True,
                    }
                ],
                "volumes": [],
                "restartPolicy": "Never",
                "imagePullSecrets": [],
                "tolerations": [
                    {
                        "key": "nvidia.com/gpu",
                        "operator": "Exists",
                        "value": "",
                        "effect": "NoSchedule",
                    }
                ],
            },
        }

    def test_to_primitive_no_ports(self) -> None:
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            env={"TESTVAR": "testvalue"},
            resources=Resources(cpu=0.5, memory=1024, gpu=1),
        )
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_primitive() == {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": "testname"},
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [
                    {
                        "name": "testname",
                        "image": "testimage",
                        "imagePullPolicy": "Always",
                        "env": [{"name": "TESTVAR", "value": "testvalue"}],
                        "volumeMounts": [],
                        "resources": {
                            "limits": {
                                "cpu": "500m",
                                "memory": "1024Mi",
                                "nvidia.com/gpu": 1,
                            }
                        },
                        "terminationMessagePolicy": "FallbackToLogsOnError",
                        "stdin": True,
                    }
                ],
                "volumes": [],
                "restartPolicy": "Never",
                "imagePullSecrets": [],
                "tolerations": [
                    {
                        "key": "nvidia.com/gpu",
                        "operator": "Exists",
                        "value": "",
                        "effect": "NoSchedule",
                    }
                ],
            },
        }

    def test_to_primitive_with_dev_shm(self) -> None:
        dev_shm = SharedMemoryVolume(name="dshm")
        container_volume = ContainerVolume(
            dst_path=PurePath("/dev/shm"),
            src_path=PurePath("/host"),
            uri=URL("storage://"),
        )
        pod = PodDescriptor(
            name="testname",
            image="testimage",
            env={"TESTVAR": "testvalue"},
            resources=Resources(cpu=0.5, memory=1024, gpu=1),
            port=1234,
            volumes=[dev_shm],
            volume_mounts=[dev_shm.create_mount(container_volume)],
        )
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.to_primitive() == {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": "testname"},
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [
                    {
                        "name": "testname",
                        "image": "testimage",
                        "imagePullPolicy": "Always",
                        "env": [{"name": "TESTVAR", "value": "testvalue"}],
                        "volumeMounts": [
                            {
                                "name": "dshm",
                                "mountPath": "/dev/shm",
                                "readOnly": False,
                                "subPath": ".",
                            }
                        ],
                        "resources": {
                            "limits": {
                                "cpu": "500m",
                                "memory": "1024Mi",
                                "nvidia.com/gpu": 1,
                            }
                        },
                        "ports": [{"containerPort": 1234}],
                        "terminationMessagePolicy": "FallbackToLogsOnError",
                        "stdin": True,
                    }
                ],
                "volumes": [{"name": "dshm", "emptyDir": {"medium": "Memory"}}],
                "restartPolicy": "Never",
                "imagePullSecrets": [],
                "tolerations": [
                    {
                        "key": "nvidia.com/gpu",
                        "operator": "Exists",
                        "value": "",
                        "effect": "NoSchedule",
                    }
                ],
            },
        }

    def test_from_job_request(self) -> None:
        container = Container(
            image="testimage",
            command="testcommand 123",
            working_dir="/working/dir",
            env={"TESTVAR": "testvalue"},
            volumes=[
                ContainerVolume(
                    uri=URL("storage://src"),
                    src_path=PurePath("/tmp/src"),
                    dst_path=PurePath("/dst"),
                )
            ],
            resources=ContainerResources(cpu=1, memory_mb=128, gpu=1),
        )
        volume = HostVolume(name="testvolume", path=PurePath("/tmp"))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            volume, job_request, priority_class_name="testpriority"
        )
        assert pod.name == job_request.job_id
        assert pod.image == "testimage"
        assert pod.args == ["testcommand", "123"]
        assert pod.env == {"TESTVAR": "testvalue"}
        assert pod.env_list == [{"name": "TESTVAR", "value": "testvalue"}]
        assert pod.volume_mounts == [
            VolumeMount(
                volume=volume, mount_path=PurePath("/dst"), sub_path=PurePath("src")
            )
        ]
        assert pod.volumes == [volume]
        assert pod.resources == Resources(cpu=1, memory=128, gpu=1)
        assert pod.priority_class_name == "testpriority"
        assert pod.working_dir == "/working/dir"

    def test_from_job_request_tpu(self) -> None:
        container = Container(
            image="testimage",
            resources=ContainerResources(
                cpu=1,
                memory_mb=128,
                tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
            ),
        )
        volume = HostVolume(name="testvolume", path=PurePath("/tmp"))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(volume, job_request)
        assert pod.annotations == {"tf-version.cloud-tpus.google.com": "1.14"}
        assert pod.priority_class_name is None

    def test_from_primitive_defaults(self) -> None:
        payload = {
            "kind": "Pod",
            "metadata": {
                "name": "testname",
                "creationTimestamp": "2019-06-20T11:03:32Z",
            },
            "spec": {"containers": [{"name": "testname", "image": "testimage"}]},
        }
        pod = PodDescriptor.from_primitive(payload)
        assert pod.name == "testname"
        assert pod.image == "testimage"
        assert pod.status is None
        assert pod.tolerations == []
        assert pod.priority_class_name is None
        assert pod.image_pull_secrets == []
        assert pod.node_name is None
        assert pod.command is None
        assert pod.args is None
        assert pod.tty is False
        assert pod.labels == {}

    def test_from_primitive(self) -> None:
        payload = {
            "kind": "Pod",
            "metadata": {
                "name": "testname",
                "creationTimestamp": "2019-06-20T11:03:32Z",
                "labels": {"testlabel": "testvalue"},
            },
            "spec": {
                "containers": [
                    {
                        "name": "testname",
                        "image": "testimage",
                        "tty": True,
                        "stdin": True,
                        "workingDir": "/working/dir",
                    }
                ],
                "tolerations": [
                    {
                        "key": "key1",
                        "value": "value1",
                        "operator": "Equals",
                        "effect": "NoSchedule",
                    },
                    {"key": "key2", "operator": "Exists"},
                    {"operator": "Exists"},
                    {"key": "key3"},
                ],
                "priorityClassName": "testpriority",
                "imagePullSecrets": [{"name": "secret"}],
            },
            "status": {"phase": "Running"},
        }
        pod = PodDescriptor.from_primitive(payload)
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
        assert pod.priority_class_name == "testpriority"
        assert pod.image_pull_secrets == [SecretRef("secret")]
        assert pod.node_name is None
        assert pod.command is None
        assert pod.args is None
        assert pod.tty is True
        assert pod.labels == {"testlabel": "testvalue"}
        assert pod.working_dir == "/working/dir"

    def test_from_primitive_failure(self) -> None:
        payload = {"kind": "Status", "code": 409}
        with pytest.raises(AlreadyExistsException, match="already exist"):
            PodDescriptor.from_primitive(payload)

    def test_from_primitive_unknown_kind(self) -> None:
        payload = {"kind": "Unknown"}
        with pytest.raises(ValueError, match="unknown kind: Unknown"):
            PodDescriptor.from_primitive(payload)


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
        container_statuses: List[Dict[str, Any]],
        expected_status: JobStatus,
    ) -> None:
        payload: Dict[str, Any] = {"phase": phase}
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
            Resources(cpu=0.5, memory=1024, tpu_version="v2")

    def test_to_primitive(self) -> None:
        resources = Resources(cpu=0.5, memory=1024)
        assert resources.to_primitive() == {
            "limits": {"cpu": "500m", "memory": "1024Mi"}
        }

    def test_to_primitive_gpu(self) -> None:
        resources = Resources(cpu=0.5, memory=1024, gpu=2)
        assert resources.to_primitive() == {
            "limits": {"cpu": "500m", "memory": "1024Mi", "nvidia.com/gpu": 2}
        }

    def test_to_primitive_tpu(self) -> None:
        resources = Resources(cpu=0.5, memory=1024, tpu_version="v2", tpu_cores=8)
        assert resources.to_primitive() == {
            "limits": {"cpu": "500m", "memory": "1024Mi", "cloud-tpus.google.com/v2": 8}
        }

    def test_from_container_resources(self) -> None:
        container_resources = ContainerResources(cpu=1, memory_mb=128, gpu=1)
        resources = Resources.from_container_resources(container_resources)
        assert resources == Resources(cpu=1, memory=128, gpu=1)

    def test_from_container_resources_tpu(self) -> None:
        container_resources = ContainerResources(
            cpu=1,
            memory_mb=128,
            tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
        )
        resources = Resources.from_container_resources(container_resources)
        assert resources == Resources(cpu=1, memory=128, tpu_version="v2", tpu_cores=8)

    @pytest.mark.parametrize("type_", ("v2", "v2-nan", "v2-", "-"))
    def test_from_container_resources_tpu_invalid_type(self, type_: str) -> None:
        container_resources = ContainerResources(
            cpu=1,
            memory_mb=128,
            tpu=ContainerTPUResource(type=type_, software_version="1.14"),
        )
        with pytest.raises(ValueError, match=f"invalid TPU type format: '{type_}'"):
            Resources.from_container_resources(container_resources)


class TestIngressRule:
    def test_from_primitive_host(self) -> None:
        rule = IngressRule.from_primitive({"host": "testhost"})
        assert rule == IngressRule(host="testhost")

    def test_from_primitive_no_paths(self) -> None:
        rule = IngressRule.from_primitive({"host": "testhost", "http": {"paths": []}})
        assert rule == IngressRule(host="testhost")

    def test_from_primitive_no_backend(self) -> None:
        rule = IngressRule.from_primitive({"host": "testhost", "http": {"paths": [{}]}})
        assert rule == IngressRule(host="testhost")

    def test_from_primitive_no_service(self) -> None:
        rule = IngressRule.from_primitive(
            {"host": "testhost", "http": {"paths": [{"backend": {}}]}}
        )
        assert rule == IngressRule(host="testhost")

    def test_from_primitive(self) -> None:
        rule = IngressRule.from_primitive(
            {
                "host": "testhost",
                "http": {
                    "paths": [
                        {"backend": {"serviceName": "testname", "servicePort": 1234}}
                    ]
                },
            }
        )
        assert rule == IngressRule(
            host="testhost", service_name="testname", service_port=1234
        )

    def test_to_primitive_no_service(self) -> None:
        rule = IngressRule(host="testhost")
        assert rule.to_primitive() == {"host": "testhost"}

    def test_to_primitive(self) -> None:
        rule = IngressRule(host="testhost", service_name="testname", service_port=1234)
        assert rule.to_primitive() == {
            "host": "testhost",
            "http": {
                "paths": [{"backend": {"serviceName": "testname", "servicePort": 1234}}]
            },
        }

    def test_from_service(self) -> None:
        service = Service(name="testname", target_port=1234)
        rule = IngressRule.from_service(host="testname.testdomain", service=service)
        assert rule == IngressRule(
            host="testname.testdomain", service_name="testname", service_port=80
        )


class TestIngress:
    def test_from_primitive_no_rules(self) -> None:
        ingress = Ingress.from_primitive(
            {
                "kind": "Ingress",
                "metadata": {"name": "testingress"},
                "spec": {"rules": []},
            }
        )
        assert ingress == Ingress(name="testingress", rules=[])

    def test_from_primitive(self) -> None:
        ingress = Ingress.from_primitive(
            {
                "kind": "Ingress",
                "metadata": {"name": "testingress"},
                "spec": {"rules": [{"host": "testhost"}]},
            }
        )
        assert ingress == Ingress(
            name="testingress", rules=[IngressRule(host="testhost")]
        )

    def test_find_rule_index_by_host(self) -> None:
        ingress = Ingress(
            name="testingress",
            rules=[
                IngressRule(host="host1"),
                IngressRule(host="host2"),
                IngressRule(host="host3"),
            ],
        )
        assert ingress.find_rule_index_by_host("host1") == 0
        assert ingress.find_rule_index_by_host("host2") == 1
        assert ingress.find_rule_index_by_host("host4") == -1

    def test_to_primitive_no_rules(self) -> None:
        ingress = Ingress(name="testingress")
        assert ingress.to_primitive() == {
            "metadata": {"name": "testingress", "annotations": {}},
            "spec": {"rules": [None]},
        }

    def test_to_primitive(self) -> None:
        ingress = Ingress(
            name="testingress",
            rules=[
                IngressRule(host="host1", service_name="testservice", service_port=1234)
            ],
        )
        assert ingress.to_primitive() == {
            "metadata": {"name": "testingress", "annotations": {}},
            "spec": {
                "rules": [
                    {
                        "host": "host1",
                        "http": {
                            "paths": [
                                {
                                    "backend": {
                                        "serviceName": "testservice",
                                        "servicePort": 1234,
                                    }
                                }
                            ]
                        },
                    }
                ]
            },
        }

    def test_to_primitive_with_annotations(self) -> None:
        ingress = Ingress(
            name="testingress",
            rules=[
                IngressRule(host="host1", service_name="testservice", service_port=1234)
            ],
            annotations={"key1": "value1"},
        )
        assert ingress.to_primitive() == {
            "metadata": {"name": "testingress", "annotations": {"key1": "value1"}},
            "spec": {
                "rules": [
                    {
                        "host": "host1",
                        "http": {
                            "paths": [
                                {
                                    "backend": {
                                        "serviceName": "testservice",
                                        "servicePort": 1234,
                                    }
                                }
                            ]
                        },
                    }
                ]
            },
        }

    def test_to_primitive_with_labels(self) -> None:
        ingress = Ingress(
            name="testingress",
            rules=[
                IngressRule(host="host1", service_name="testservice", service_port=1234)
            ],
            labels={"test-label-1": "test-value-1", "test-label-2": "test-value-2"},
        )
        assert ingress.to_primitive() == {
            "metadata": {
                "name": "testingress",
                "annotations": {},
                "labels": {
                    "test-label-1": "test-value-1",
                    "test-label-2": "test-value-2",
                },
            },
            "spec": {
                "rules": [
                    {
                        "host": "host1",
                        "http": {
                            "paths": [
                                {
                                    "backend": {
                                        "serviceName": "testservice",
                                        "servicePort": 1234,
                                    }
                                }
                            ]
                        },
                    }
                ]
            },
        }


class TestService:
    @pytest.fixture
    def service_payload(self) -> Dict[str, Any]:
        return {
            "metadata": {"name": "testservice"},
            "spec": {
                "type": "ClusterIP",
                "ports": [{"port": 80, "targetPort": 8080, "name": "http"}],
                "selector": {"job": "testservice"},
            },
        }

    def test_to_primitive(self, service_payload: Dict[str, Dict[str, Any]]) -> None:
        service = Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            target_port=8080,
        )
        assert service.to_primitive() == service_payload

    def test_to_primitive_with_labels(
        self, service_payload: Dict[str, Dict[str, Any]]
    ) -> None:
        labels = {"label-name": "label-value"}
        expected_payload = service_payload.copy()
        expected_payload["metadata"]["labels"] = labels
        service = Service(
            name="testservice",
            selector=expected_payload["spec"]["selector"],
            target_port=8080,
            labels=labels,
        )
        assert service.to_primitive() == expected_payload

    def test_to_primitive_load_balancer(
        self, service_payload: Dict[str, Dict[str, Any]]
    ) -> None:
        service = Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            target_port=8080,
            service_type=ServiceType.LOAD_BALANCER,
        )
        service_payload["spec"]["type"] = "LoadBalancer"
        assert service.to_primitive() == service_payload

    def test_to_primitive_headless(
        self, service_payload: Dict[str, Dict[str, Any]]
    ) -> None:
        service = Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            target_port=8080,
            cluster_ip="None",
        )
        service_payload["spec"]["clusterIP"] = "None"
        assert service.to_primitive() == service_payload

    def test_from_primitive(self, service_payload: Dict[str, Dict[str, Any]]) -> None:
        service = Service.from_primitive(service_payload)
        assert service == Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            target_port=8080,
        )

    def test_from_primitive_with_labels(
        self, service_payload: Dict[str, Dict[str, Any]]
    ) -> None:
        labels = {"label-name": "label-value"}
        input_payload = service_payload.copy()
        input_payload["metadata"]["labels"] = labels
        service = Service.from_primitive(input_payload)
        assert service == Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            target_port=8080,
            labels=labels,
        )

    def test_from_primitive_node_port(
        self, service_payload: Dict[str, Dict[str, Any]]
    ) -> None:
        service_payload["spec"]["type"] = "NodePort"
        service = Service.from_primitive(service_payload)
        assert service == Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            target_port=8080,
            service_type=ServiceType.NODE_PORT,
        )

    def test_from_primitive_headless(
        self, service_payload: Dict[str, Dict[str, Any]]
    ) -> None:
        service_payload["spec"]["clusterIP"] = "None"
        service = Service.from_primitive(service_payload)
        assert service == Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            cluster_ip="None",
            target_port=8080,
        )

    def test_create_for_pod(self) -> None:
        pod = PodDescriptor(name="testpod", image="testimage", port=1234)
        service = Service.create_for_pod(pod)
        assert service == Service(name="testpod", target_port=1234)

    def test_create_headless_for_pod(self) -> None:
        pod = PodDescriptor(name="testpod", image="testimage", port=1234)
        service = Service.create_headless_for_pod(pod)
        assert service == Service(name="testpod", cluster_ip="None", target_port=1234)


class TestServiceWithSSHOnly:
    @pytest.fixture(scope="function")
    def service_payload(self) -> Dict[str, Any]:
        return {
            "metadata": {"name": "testservice"},
            "spec": {
                "type": "ClusterIP",
                "ports": [{"port": 89, "targetPort": 8181, "name": "ssh"}],
                "selector": {"job": "testservice"},
            },
        }

    def test_to_primitive(self, service_payload: Dict[str, Dict[str, Any]]) -> None:
        service = Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            target_port=None,
            ssh_port=89,
            ssh_target_port=8181,
        )
        assert service.to_primitive() == service_payload

    def test_to_primitive_default_port(
        self, service_payload: Dict[str, Dict[str, Any]]
    ) -> None:
        service_payload["spec"]["ports"][0]["port"] = 22
        service = Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            target_port=None,
            ssh_target_port=8181,
        )
        assert service.to_primitive() == service_payload

    def test_from_primitive(self, service_payload: Dict[str, Dict[str, Any]]) -> None:
        service = Service.from_primitive(service_payload)
        assert service == Service(
            name="testservice",
            selector=service_payload["spec"]["selector"],
            target_port=None,
            port=80,
            ssh_target_port=8181,
            ssh_port=89,
        )

    def test_create_for_pod(self) -> None:
        pod = PodDescriptor(name="testpod", image="testimage", ssh_port=89)
        service = Service.create_for_pod(pod)
        assert service == Service(name="testpod", target_port=None, ssh_target_port=89)


class TestContainerStatus:
    def test_no_state(self) -> None:
        payload: Dict[str, Any] = {"state": {}}
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
            status.exit_code

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
