import dataclasses
import hashlib
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import PurePath
from typing import Any, Optional
from unittest import mock

import pytest
from yarl import URL

from platform_api.config import NO_ORG
from platform_api.handlers.job_request_builder import create_container_from_payload
from platform_api.orchestrator.job import (
    Job,
    JobPriority,
    JobRecord,
    JobRestartPolicy,
    JobStatusHistory,
    JobStatusItem,
    JobStatusReason,
    maybe_job_id,
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerTPUResource,
    ContainerVolume,
    Disk,
    DiskContainerVolume,
    JobError,
    JobRequest,
    JobStatus,
    Secret,
    SecretContainerVolume,
)

from .conftest import MockOrchestrator


class TestContainer:
    def test_command_entrypoint_list_command_list_empty(self) -> None:
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory=128 * 10**6)
        )
        assert container.entrypoint_list == []
        assert container.command_list == []

    def test_command_list_non_empty(self) -> None:
        container = Container(
            image="testimage",
            command="bash -c date",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        assert container.command_list == ["bash", "-c", "date"]

    def test_command_list_invalid(self) -> None:
        container = Container(
            image="testimage",
            command='"',
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        with pytest.raises(JobError, match="invalid command format"):
            container.command_list

    def test_entrypoint_list_invalid(self) -> None:
        container = Container(
            image="testimage",
            entrypoint='"',
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        with pytest.raises(JobError, match="invalid command format"):
            container.entrypoint_list

    def test_entrypoint_list_non_empty(self) -> None:
        container = Container(
            image="testimage",
            entrypoint="bash -c date",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        assert container.entrypoint_list == ["bash", "-c", "date"]

    def test_command_list_entrypoint_list_non_empty(self) -> None:
        container = Container(
            image="testimage",
            entrypoint="/script.sh",
            command="arg1 arg2 arg3",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        assert container.entrypoint_list == ["/script.sh"]
        assert container.command_list == ["arg1", "arg2", "arg3"]

    def test_belongs_to_registry_no_host(self) -> None:
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory=128 * 10**6)
        )
        assert not container.belongs_to_registry("example.com")

    def test_belongs_to_registry_different_host(self) -> None:
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        assert not container.belongs_to_registry("example.com")

    def test_belongs_to_registry(self) -> None:
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        assert container.belongs_to_registry("example.com")

    def test_to_image_uri_failure(self) -> None:
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        with pytest.raises(AssertionError, match="Unknown registry"):
            container.to_image_uri("example.com", "test-cluster")

    def test_to_image_uri(self) -> None:
        container = Container(
            image="example.com/project/testimage%2d",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        uri = container.to_image_uri("example.com", "test-cluster")
        assert uri == URL("image://test-cluster/project/testimage%252d")

    def test_to_image_uri_registry_with_custom_port(self) -> None:
        container = Container(
            image="example.com:5000/project/testimage",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        uri = container.to_image_uri("example.com:5000", "test-cluster")
        assert uri == URL("image://test-cluster/project/testimage")

    def test_to_image_uri_ignore_tag(self) -> None:
        container = Container(
            image="example.com/project/testimage:latest",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
        )
        uri = container.to_image_uri("example.com", "test-cluster")
        assert uri == URL("image://test-cluster/project/testimage")


class TestContainerVolumeCreate:
    @pytest.mark.parametrize(
        "uri",
        (
            "storage://test-cluster/path/to/dir",
            "storage://test-cluster/path/to//dir",
            "storage://test-cluster/path/to/./dir",
            "storage://test-cluster/path/to/%2e/dir",
            "storage://test-cluster/path/to%2fdir",
        ),
    )
    def test_create(self, uri: str) -> None:
        volume = ContainerVolume.create(
            uri,
            dst_path=PurePath("/container/mnt"),
            read_only=True,
        )
        assert volume.src_path == PurePath("/path/to/dir")
        assert volume.dst_path == PurePath("/container/mnt")
        assert volume.read_only


class TestDisk:
    def test_create(self) -> None:
        uri = "disk://test-cluster/test-user/test-disk%252d"
        disk = Disk.create(uri)
        assert disk.cluster_name == "test-cluster"
        assert disk.path == "test-user"
        assert disk.disk_id == "test-disk%2d"

    def test_create_with_org(self) -> None:
        uri = "disk://test-cluster/org/test-user/test-disk"
        disk = Disk.create(uri)
        assert disk.cluster_name == "test-cluster"
        assert disk.path == "org/test-user"
        assert disk.disk_id == "test-disk"

    def test_create_uri_eq_str(self) -> None:
        uri = "disk://test-cluster/test-user/test-disk%252d"
        assert Disk.create(URL(uri)) == Disk.create(uri)

    def test_to_uri(self) -> None:
        uri = "disk://test-cluster/test-user/test-disk%252d"
        disk = Disk.create(uri)
        assert disk.to_uri() == URL(uri)


class TestDiskContainerVolume:
    def test_create(self) -> None:
        uri = "disk://test-cluster/test-user/test-disk"
        volume = DiskContainerVolume.create(
            uri, dst_path=PurePath("/container"), read_only=False
        )
        assert volume.disk.cluster_name == "test-cluster"
        assert volume.disk.path == "test-user"
        assert volume.disk.disk_id == "test-disk"
        assert volume.dst_path == PurePath("/container")
        assert not volume.read_only

    def test_to_and_from_primitive(self) -> None:
        primitive = {
            "src_disk_uri": "disk://test-cluster/test-user/test-disk",
            "dst_path": "/container",
            "read_only": True,
        }
        volume = DiskContainerVolume.from_primitive(primitive)
        assert volume.to_primitive() == primitive


class TestSecret:
    def test_create(self) -> None:
        uri = "secret://test-cluster/test-user/test-secret%252d"
        sec = Secret.create(uri)
        assert sec.cluster_name == "test-cluster"
        assert sec.path == "test-user"
        assert sec.secret_key == "test-secret%2d"

    def test_create_with_org(self) -> None:
        uri = "secret://test-cluster/org/test-user/test-secret"
        sec = Secret.create(uri)
        assert sec.cluster_name == "test-cluster"
        assert sec.path == "org/test-user"
        assert sec.secret_key == "test-secret"

    def test_create_uri_eq_str(self) -> None:
        uri = "secret://test-cluster/test-user/test-secret%252d"
        assert Secret.create(URL(uri)) == Secret.create(uri)

    def test_k8s_secret_name(self) -> None:
        uri = "secret://test-cluster/test-user/test-secret%252d"
        sec = Secret.create(uri)
        assert sec.k8s_secret_name == "project--test-user--secrets"

    def test_to_uri(self) -> None:
        uri = "secret://test-cluster/test-user/test-secret%252d"
        sec = Secret.create(uri)
        assert sec.to_uri() == URL(uri)


class TestSecretContainerVolume:
    def test_create(self) -> None:
        uri = "secret://test-cluster/test-user/test-secret"
        volume = SecretContainerVolume.create(uri, dst_path=PurePath("/container"))
        assert volume.secret.cluster_name == "test-cluster"
        assert volume.secret.path == "test-user"
        assert volume.secret.secret_key == "test-secret"
        assert volume.dst_path == PurePath("/container")

    def test_to_and_from_primitive(self) -> None:
        primitive = {
            "src_secret_uri": "secret://test-cluster/test-user/test-secret",
            "dst_path": "/container",
        }
        volume = SecretContainerVolume.from_primitive(primitive)
        assert volume.to_primitive() == primitive

    def test_create_without_extending_dst_mount_path(self) -> None:
        uri = "storage://test-cluster/path/to/dir"
        volume = ContainerVolume.create(
            uri,
            dst_path=PurePath("/container"),
            read_only=True,
        )
        assert volume.src_path == PurePath("/path/to/dir")
        assert volume.dst_path == PurePath("/container")
        assert volume.read_only


class TestContainerBuilder:
    def test_from_payload_build(self) -> None:
        payload = {
            "image": "testimage",
            "entrypoint": "testentrypoint",
            "command": "testcommand",
            "working_dir": "/working/dir",
            "env": {"TESTVAR": "testvalue"},
            "resources": {"cpu": 0.1, "memory_mb": 128, "gpu": 1},
            "http": {"port": 80},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/path/to/dir",
                    "dst_path": "/container/path",
                    "read_only": True,
                }
            ],
        }
        container = create_container_from_payload(payload)
        assert container == Container(
            image="testimage",
            entrypoint="testentrypoint",
            command="testcommand",
            working_dir="/working/dir",
            env={"TESTVAR": "testvalue"},
            volumes=[
                ContainerVolume(
                    uri=URL("storage://test-cluster/path/to/dir"),
                    dst_path=PurePath("/container/path"),
                    read_only=True,
                )
            ],
            resources=ContainerResources(
                cpu=0.1, memory=128 * 2**20, gpu=1, shm=None
            ),
            http_server=ContainerHTTPServer(port=80, health_check_path="/"),
            tty=False,
        )

    def test_from_job_payload_build(self) -> None:
        payload = {
            "container": {
                "image": "testimage",
                "entrypoint": "testentrypoint",
                "command": "testcommand",
                "working_dir": "/working/dir",
                "env": {"TESTVAR": "testvalue"},
                "resources": {"cpu": 0.1, "memory_mb": 128, "gpu": 1},
                "http": {"port": 80},
                "volumes": [
                    {
                        "src_storage_uri": "storage://test-cluster/path/to/dir",
                        "dst_path": "/container/path",
                        "read_only": True,
                    }
                ],
            }
        }
        container = create_container_from_payload(payload)
        assert container == Container(
            image="testimage",
            entrypoint="testentrypoint",
            command="testcommand",
            working_dir="/working/dir",
            env={"TESTVAR": "testvalue"},
            volumes=[
                ContainerVolume(
                    uri=URL("storage://test-cluster/path/to/dir"),
                    dst_path=PurePath("/container/path"),
                    read_only=True,
                )
            ],
            resources=ContainerResources(
                cpu=0.1, memory=128 * 2**20, gpu=1, shm=None
            ),
            http_server=ContainerHTTPServer(port=80, health_check_path="/"),
            tty=False,
        )

    def test_from_payload_build_gpu_model(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 128,
                "gpu": 1,
                "gpu_model": "gpumodel",
            },
        }
        container = create_container_from_payload(payload)
        assert container == Container(
            image="testimage",
            resources=ContainerResources(
                cpu=0.1, memory=128 * 2**20, gpu=1, gpu_model_id="gpumodel"
            ),
        )

    def test_from_payload_build_tpu(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 128,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
        }
        container = create_container_from_payload(payload)
        assert container == Container(
            image="testimage",
            resources=ContainerResources(
                cpu=0.1,
                memory=128 * 2**20,
                tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
            ),
        )

    def test_from_payload_build_with_shm_false(self) -> None:
        payload = {
            "image": "testimage",
            "command": "testcommand",
            "env": {"TESTVAR": "testvalue"},
            "resources": {"cpu": 0.1, "memory_mb": 128, "gpu": 1, "shm": True},
            "http": {"port": 80},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/path/to/dir",
                    "dst_path": "/container/path",
                    "read_only": True,
                }
            ],
        }
        container = create_container_from_payload(payload)
        assert container == Container(
            image="testimage",
            command="testcommand",
            env={"TESTVAR": "testvalue"},
            volumes=[
                ContainerVolume(
                    uri=URL("storage://test-cluster/path/to/dir"),
                    dst_path=PurePath("/container/path"),
                    read_only=True,
                )
            ],
            resources=ContainerResources(
                cpu=0.1, memory=128 * 2**20, gpu=1, shm=True
            ),
            http_server=ContainerHTTPServer(port=80, health_check_path="/"),
        )

    def test_from_payload_build_with_tty(self) -> None:
        payload = {
            "image": "testimage",
            "entrypoint": "testentrypoint",
            "command": "testcommand",
            "env": {"TESTVAR": "testvalue"},
            "resources": {"cpu": 0.1, "memory_mb": 128, "gpu": 1},
            "http": {"port": 80},
            "volumes": [],
            "tty": True,
        }
        container = create_container_from_payload(payload)
        assert container == Container(
            image="testimage",
            entrypoint="testentrypoint",
            command="testcommand",
            env={"TESTVAR": "testvalue"},
            volumes=[],
            resources=ContainerResources(
                cpu=0.1, memory=128 * 2**20, gpu=1, shm=None
            ),
            http_server=ContainerHTTPServer(port=80, health_check_path="/"),
            tty=True,
        )


@pytest.fixture
def job_request_payload() -> dict[str, Any]:
    return {
        "job_id": "testjob",
        "description": "Description of the testjob",
        "container": {
            "image": "testimage",
            "resources": {"cpu": 1, "memory": 128 * 10**6},
            "command": None,
            "env": {"testvar": "testval"},
            "volumes": [
                {
                    "uri": "storage://host/src/path",
                    "dst_path": "/dst/path",
                    "read_only": False,
                }
            ],
            "http_server": None,
            "tty": False,
        },
    }


@pytest.fixture
def job_payload(job_request_payload: Any) -> dict[str, Any]:
    finished_at_str = datetime.now(timezone.utc).isoformat()
    return {
        "id": "testjob",
        "request": job_request_payload,
        "status": "succeeded",
        "materialized": False,
        "finished_at": finished_at_str,
        "statuses": [{"status": "failed", "transition_time": finished_at_str}],
    }


@pytest.fixture
def job_request_payload_with_shm(job_request_payload: dict[str, Any]) -> dict[str, Any]:
    data = job_request_payload
    data["container"]["resources"]["shm"] = True
    return data


@pytest.fixture
def job_request() -> JobRequest:
    container = Container(
        image="testimage",
        resources=ContainerResources(cpu=1, memory=128 * 10**6),
        http_server=ContainerHTTPServer(port=1234),
    )
    return JobRequest(
        job_id="testjob", container=container, description="Description of the testjob"
    )


class TestJobRecord:
    def test_should_be_deleted_pending(self, job_request: JobRequest) -> None:
        record = JobRecord.create(request=job_request, cluster_name="test-cluster")
        assert not record.finished_at
        assert not record.should_be_deleted(delay=timedelta(60))

    def test_should_be_deleted_finished(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        record = JobRecord.create(
            status=JobStatus.FAILED,
            request=job_request,
            cluster_name="test-cluster",
            materialized=True,
        )
        assert record.finished_at
        assert record.should_be_deleted(delay=timedelta(0))


class TestJob:
    @pytest.fixture
    def job_request_with_gpu(self) -> JobRequest:
        container = Container(
            image="testimage",
            resources=ContainerResources(
                cpu=1, memory=64 * 10**6, gpu=1, gpu_model_id="nvidia-tesla-k80"
            ),
        )
        return JobRequest(
            job_id="testjob",
            container=container,
            description="Description of the testjob with gpu",
        )

    @pytest.fixture
    def job_request_with_http(self) -> JobRequest:
        container = Container(
            image="testimage",
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
            http_server=ContainerHTTPServer(port=1234),
        )
        return JobRequest(
            job_id="testjob",
            container=container,
            description="Description of the testjob",
        )

    def test_http_host(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert job.http_host == "testjob.jobs"

    @classmethod
    def _create_http_host_named_suffix(
        cls, org_name: Optional[str], project_name: str
    ) -> str:
        hasher = hashlib.new("sha256")
        org_name = org_name or NO_ORG
        hasher.update(org_name.encode("utf-8"))
        hasher.update(project_name.encode("utf-8"))
        return hasher.hexdigest()[:10]

    def test_http_host_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                org_name="test-org",
                project_name="test-project",
                name="test-job-name",
                owner="owner",
            ),
        )
        suffix = self._create_http_host_named_suffix("test-org", "test-project")

        assert job.http_host == "testjob.jobs"
        assert job.http_host_named == f"test-job-name--{suffix}.jobs"

    def test_job_name(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                name="test-job-name-123",
            ),
        )
        assert job.name == "test-job-name-123"

    def test_job_has_gpu_false(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert not job.has_gpu

    def test_job_has_gpu_true(
        self, mock_orchestrator: MockOrchestrator, job_request_with_gpu: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request_with_gpu, cluster_name="test-cluster"
            ),
        )
        assert job.has_gpu

    def _mocked_datetime_factory(self) -> datetime:
        return datetime(year=2019, month=1, day=1)

    def test_job_gpu_model_id(
        self, mock_orchestrator: MockOrchestrator, job_request_with_gpu: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request_with_gpu, cluster_name="test-cluster"
            ),
        )
        assert job.gpu_model_id == "nvidia-tesla-k80"

    def test_job_gpu_model_id_none(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert job.gpu_model_id is None

    @pytest.fixture
    def job_factory(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> Callable[..., Job]:
        current_datetime_factory = self._mocked_datetime_factory

        def _f(
            job_status_history: JobStatusHistory, scheduler_enabled: bool = False
        ) -> Job:
            return Job(
                orchestrator_config=mock_orchestrator.config,
                record=JobRecord.create(
                    request=job_request,
                    cluster_name="test-cluster",
                    status_history=job_status_history,
                    current_datetime_factory=current_datetime_factory,
                    scheduler_enabled=scheduler_enabled,
                ),
                current_datetime_factory=current_datetime_factory,
            )

        return _f

    def test_job_get_run_time_pending_job(
        self, mock_orchestrator: MockOrchestrator, job_factory: Callable[..., Job]
    ) -> None:
        time_now = self._mocked_datetime_factory()
        started_at = time_now - timedelta(minutes=30)
        items = [JobStatusItem.create(JobStatus.PENDING, transition_time=started_at)]

        job = job_factory(JobStatusHistory(items))
        # job is still pending
        expected_timedelta = timedelta(0)
        assert job.get_run_time() == expected_timedelta

    def test_job_get_run_time_pending_job_scheduled(
        self, mock_orchestrator: MockOrchestrator, job_factory: Callable[..., Job]
    ) -> None:
        time_now = self._mocked_datetime_factory()
        started_at = time_now - timedelta(minutes=30)
        items = [JobStatusItem.create(JobStatus.PENDING, transition_time=started_at)]

        job = job_factory(JobStatusHistory(items), scheduler_enabled=True)
        # job is still pending
        expected_timedelta = timedelta(0)
        assert job.get_run_time() == expected_timedelta

    def test_job_get_run_time_running_job(
        self, mock_orchestrator: MockOrchestrator, job_factory: Callable[..., Job]
    ) -> None:
        time_now = self._mocked_datetime_factory()

        started_ago_delta = timedelta(hours=1)  # job started 1 hour ago: pending
        pending_delta = timedelta(minutes=5)  # after 5 min: running (still running)
        running_delta = started_ago_delta - pending_delta

        pending_at = time_now - started_ago_delta
        running_at = pending_at + pending_delta
        items = [
            JobStatusItem.create(JobStatus.PENDING, transition_time=pending_at),
            JobStatusItem.create(JobStatus.RUNNING, transition_time=running_at),
        ]
        job = job_factory(JobStatusHistory(items))

        assert job.get_run_time() == running_delta

    def test_job_get_run_time_running_job_scheduled(
        self, mock_orchestrator: MockOrchestrator, job_factory: Callable[..., Job]
    ) -> None:
        time_now = self._mocked_datetime_factory()

        started_ago_delta = timedelta(hours=1)  # job started 1 hour ago: pending
        pending1_delta = timedelta(minutes=3)  # after 3 min: running
        running1_delta = timedelta(minutes=4)  # after 4 min: pending
        pending2_delta = timedelta(minutes=5)  # after 5 min: running (still running)
        running2_delta = (
            started_ago_delta - pending1_delta - running1_delta - pending2_delta
        )
        running_sum_delta = running1_delta + running2_delta

        pending1_at = time_now - started_ago_delta
        running1_at = pending1_at + pending1_delta
        pending2_at = running1_at + running1_delta
        running2_at = pending2_at + pending2_delta
        items = [
            JobStatusItem.create(JobStatus.PENDING, transition_time=pending1_at),
            JobStatusItem.create(JobStatus.RUNNING, transition_time=running1_at),
            JobStatusItem.create(JobStatus.PENDING, transition_time=pending2_at),
            JobStatusItem.create(JobStatus.RUNNING, transition_time=running2_at),
        ]
        job = job_factory(JobStatusHistory(items), scheduler_enabled=True)

        assert job.get_run_time() == running_sum_delta

    @pytest.mark.parametrize(
        "terminated_status", [JobStatus.SUCCEEDED, JobStatus.FAILED]
    )
    def test_job_get_run_time_terminated_job(
        self,
        mock_orchestrator: MockOrchestrator,
        job_factory: Callable[..., Job],
        terminated_status: JobStatus,
    ) -> None:
        time_now = self._mocked_datetime_factory()

        started_ago_delta = timedelta(hours=1)  # job started 1 hour ago: pending
        pending_delta = timedelta(minutes=5)  # after 5 min: running
        running_delta = timedelta(minutes=6)  # after 6 min: succeeded

        pending_at = time_now - started_ago_delta
        running_at = pending_at + pending_delta
        terminated_at = running_at + running_delta
        items = [
            JobStatusItem.create(JobStatus.PENDING, transition_time=pending_at),
            JobStatusItem.create(JobStatus.RUNNING, transition_time=running_at),
            JobStatusItem.create(terminated_status, transition_time=terminated_at),
        ]
        job = job_factory(JobStatusHistory(items))

        assert job.get_run_time() == running_delta

    @pytest.mark.parametrize(
        "terminated_status", [JobStatus.SUCCEEDED, JobStatus.FAILED]
    )
    def test_job_get_run_time_terminated_job_scheduled(
        self,
        mock_orchestrator: MockOrchestrator,
        job_factory: Callable[..., Job],
        terminated_status: JobStatus,
    ) -> None:
        time_now = self._mocked_datetime_factory()

        started_ago_delta = timedelta(hours=1)  # job started 1 hour ago: pending
        pending1_delta = timedelta(minutes=3)  # after 3 min: running
        running1_delta = timedelta(minutes=4)  # after 4 min: pending
        pending2_delta = timedelta(minutes=5)  # after 5 min: running
        running2_delta = timedelta(minutes=6)  # after 6 min: succeeded (still)
        running_sum_delta = running1_delta + running2_delta

        pending1_at = time_now - started_ago_delta
        running1_at = pending1_at + pending1_delta
        pending2_at = running1_at + running1_delta
        running2_at = pending2_at + pending2_delta
        terminated_at = running2_at + running2_delta
        items = [
            JobStatusItem.create(JobStatus.PENDING, transition_time=pending1_at),
            JobStatusItem.create(JobStatus.RUNNING, transition_time=running1_at),
            JobStatusItem.create(JobStatus.PENDING, transition_time=pending2_at),
            JobStatusItem.create(JobStatus.RUNNING, transition_time=running2_at),
            JobStatusItem.create(terminated_status, transition_time=terminated_at),
        ]
        job = job_factory(JobStatusHistory(items), scheduler_enabled=True)

        assert job.get_run_time() == running_sum_delta

    def test_http_url(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert job.http_url == "http://testjob.jobs"

    def test_http_urls_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                name="test-job-name",
                owner="owner",
            ),
        )
        suffix = self._create_http_host_named_suffix(None, "owner")

        assert job.http_url == "http://testjob.jobs"
        assert job.http_url_named == f"http://test-job-name--{suffix}.jobs"

    def test_https_url(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        config = dataclasses.replace(
            mock_orchestrator.config, is_http_ingress_secure=True
        )
        job = Job(
            orchestrator_config=config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert job.http_url == "https://testjob.jobs"

    def test_https_urls_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        config = dataclasses.replace(
            mock_orchestrator.config, is_http_ingress_secure=True
        )
        job = Job(
            orchestrator_config=config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                name="test-job-name",
                owner="owner",
            ),
        )
        suffix = self._create_http_host_named_suffix(None, "owner")

        assert job.http_url == "https://testjob.jobs"
        assert job.http_url_named == f"https://test-job-name--{suffix}.jobs"

    def test_http_url_named(
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_with_http: JobRequest,
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request_with_http,
                cluster_name="test-cluster",
                name="test-job-name",
                owner="owner",
            ),
        )
        suffix = self._create_http_host_named_suffix(None, "owner")

        assert job.http_url == "http://testjob.jobs"
        assert job.http_url_named == f"http://test-job-name--{suffix}.jobs"

    def test_to_primitive(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                owner="testuser",
                name="test-job-name",
                scheduler_enabled=False,
                preemptible_node=True,
                schedule_timeout=15,
            ),
        )
        job.status = JobStatus.FAILED
        job.materialized = False
        assert job.finished_at
        expected_finished_at = job.finished_at.isoformat()
        assert job.to_primitive() == {
            "id": job.id,
            "name": "test-job-name",
            "owner": "testuser",
            "cluster_name": "test-cluster",
            "project_name": "testuser",
            "org_project_hash": "96d4e8e962",
            "request": job_request.to_primitive(),
            "status": "failed",
            "materialized": False,
            "fully_billed": False,
            "finished_at": expected_finished_at,
            "statuses": [
                {
                    "status": "pending",
                    "transition_time": mock.ANY,
                    "reason": None,
                    "description": None,
                },
                {
                    "status": "failed",
                    "transition_time": expected_finished_at,
                    "reason": None,
                    "description": None,
                },
            ],
            "scheduler_enabled": False,
            "preemptible_node": True,
            "pass_config": False,
            "schedule_timeout": 15,
            "restart_policy": "never",
            "privileged": False,
            "total_price_credits": "0",
            "priority": 0,
            "energy_schedule_name": "default",
        }

    def test_to_primitive_with_max_run_time(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                max_run_time_minutes=500,
            ),
        )
        assert job.to_primitive() == {
            "id": "testjob",
            "owner": "compute",
            "cluster_name": "test-cluster",
            "project_name": "compute",
            "org_project_hash": "c16d8d755d",
            "request": job_request.to_primitive(),
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "transition_time": mock.ANY,
                    "reason": None,
                    "description": None,
                }
            ],
            "materialized": False,
            "fully_billed": False,
            "finished_at": None,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "pass_config": False,
            "max_run_time_minutes": 500,
            "restart_policy": "never",
            "privileged": False,
            "total_price_credits": "0",
            "priority": 0,
            "energy_schedule_name": "default",
        }

    def test_to_primitive_with_tags(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request, cluster_name="test-cluster", tags=["t1", "t2"]
            ),
        )
        primitive = job.to_primitive()
        assert primitive["tags"] == ["t1", "t2"]

    def test_to_primitive_with_preset_name(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                preset_name="cpu-small",
            ),
        )
        primitive = job.to_primitive()
        assert primitive["preset_name"] == "cpu-small"

    def test_to_primitive_with_org_name(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                org_name="10250zxvgew",
            ),
        )
        primitive = job.to_primitive()
        assert primitive["org_name"] == "10250zxvgew"

    def test_to_primitive_with_priority(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                priority=JobPriority.HIGH,
            ),
        )

        assert job.priority == JobPriority.HIGH

    def test_from_primitive(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.id == "testjob"
        assert job.status == JobStatus.SUCCEEDED
        assert job.materialized
        assert job.finished_at
        assert job.description == "Description of the testjob"
        assert not job.tags
        assert job.name is None
        assert job.owner == "testuser"
        assert not job.scheduler_enabled
        assert not job.preemptible_node
        assert not job.org_name
        assert job.max_run_time_minutes is None
        assert job.restart_policy == JobRestartPolicy.NEVER
        assert job.org_project_hash

    def test_from_primitive_check_name(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.id == "testjob"
        assert job.name == "test-job-name"

    def test_from_primitive_with_preset_name(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "preset_name": "cpu-small",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.preset_name == "cpu-small"

    def test_from_primitive_with_tags(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        tags = ["tag1", "tag2"]
        payload = {
            "id": "testjob",
            "owner": "testuser",
            "tags": tags,
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.id == "testjob"
        assert job.tags == tags

    def test_from_primitive_with_statuses(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        finished_at_str = datetime.now(timezone.utc).isoformat()
        payload = {
            "id": "testjob",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": finished_at_str,
            "statuses": [{"status": "failed", "transition_time": finished_at_str}],
            "scheduler_enabled": True,
            "preemptible_node": True,
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.id == "testjob"
        assert job.status == JobStatus.FAILED
        assert job.materialized
        assert job.finished_at
        assert job.description == "Description of the testjob"
        assert job.owner == "compute"
        assert job.scheduler_enabled
        assert job.preemptible_node

    def test_from_primitive_with_cluster_name(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "owner": "testuser",
            "cluster_name": "testcluster",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.id == "testjob"
        assert job.status == JobStatus.SUCCEEDED
        assert job.materialized
        assert job.finished_at
        assert job.description == "Description of the testjob"
        assert job.name is None
        assert job.owner == "testuser"
        assert job.cluster_name == "testcluster"
        assert not job.scheduler_enabled
        assert not job.preemptible_node

    def test_from_primitive_with_entrypoint_without_command(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"]["entrypoint"] = "/script.sh"
        job_request_payload["container"].pop("command", None)
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.request.container.command is None
        assert job.request.container.entrypoint == "/script.sh"

    def test_from_primitive_without_entrypoint_with_command(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"].pop("entrypoint", None)
        job_request_payload["container"]["command"] = "arg1 arg2 arg3"
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.request.container.command == "arg1 arg2 arg3"
        assert job.request.container.entrypoint is None

    def test_from_primitive_without_entrypoint_without_command(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"].pop("entrypoint", None)
        job_request_payload["container"].pop("command", None)
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.request.container.command is None
        assert job.request.container.entrypoint is None

    def test_from_primitive_with_entrypoint_with_command(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"]["entrypoint"] = "/script.sh"
        job_request_payload["container"]["command"] = "arg1 arg2 arg3"
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.request.container.command == "arg1 arg2 arg3"
        assert job.request.container.entrypoint == "/script.sh"

    def test_from_primitive_with_max_run_time_minutes(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "max_run_time_minutes": 100,
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.max_run_time_minutes == 100

    def test_from_primitive_with_max_run_time_minutes_none(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "max_run_time_minutes": None,
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.max_run_time_minutes is None

    def test_from_primitive_with_org_name(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "max_run_time_minutes": None,
            "org_name": "some-random-213-tenant-id",
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.org_name == "some-random-213-tenant-id"

    def test_from_primitive_with_priority(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "priority": 1,
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.priority == JobPriority.HIGH

    def test_from_primitive_with_org_project_hash(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "materialized": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "max_run_time_minutes": None,
            "org_project_hash": "0123456789",
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.org_project_hash == bytes.fromhex("0123456789")

    def test_to_uri(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request, cluster_name="test-cluster", owner="testuser"
            ),
        )
        assert job.to_uri() == URL(f"job://test-cluster/testuser/{job.id}")

    def test_to_uri_orphaned(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert job.to_uri() == URL(f"job://test-cluster/compute/{job.id}")

    def test_to_uri_no_cluster(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request, cluster_name="", owner="testuser"
            ),
        )
        with pytest.raises(AssertionError):
            job.to_uri()

    def test_to_uri_no_owner(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request, cluster_name="test-cluster", orphaned_job_owner=""
            ),
        )
        assert job.to_uri() == URL(f"job://test-cluster/{job.id}")

    def test_to_and_from_primitive(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: dict[str, Any]
    ) -> None:
        finished_at_str = datetime.now(timezone.utc).isoformat()
        current_status_item = {
            "status": "failed",
            "transition_time": finished_at_str,
            "reason": None,
            "description": None,
        }
        expected = {
            "id": job_request_payload["job_id"],
            "request": job_request_payload,
            "owner": "user",
            "cluster_name": "testcluster",
            "project_name": "project",
            "org_project_hash": "b75bbeeaca",
            "status": current_status_item["status"],
            "statuses": [current_status_item],
            "materialized": False,
            "fully_billed": False,
            "finished_at": finished_at_str,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "pass_config": False,
            "restart_policy": str(JobRestartPolicy.ALWAYS),
            "privileged": False,
            "total_price_credits": "0",
            "priority": 0,
            "energy_schedule_name": "green",
        }
        actual = Job.to_primitive(
            Job.from_primitive(mock_orchestrator.config, expected)
        )
        assert actual == expected


class TestJobRequest:
    def test_to_primitive(self, job_request_payload: dict[str, Any]) -> None:
        container = Container(
            image="testimage",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://host/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
        )
        request = JobRequest(
            job_id="testjob",
            description="Description of the testjob",
            container=container,
        )
        assert request.to_primitive() == job_request_payload

    def test_to_primitive_with_entrypoint(
        self, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"]["entrypoint"] = "/bin/ls"
        container = Container(
            image="testimage",
            entrypoint="/bin/ls",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://host/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
        )
        request = JobRequest(
            job_id="testjob",
            description="Description of the testjob",
            container=container,
        )
        assert request.to_primitive() == job_request_payload

    def test_to_primitive_with_working_dir(
        self, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"]["working_dir"] = "/working/dir"
        container = Container(
            image="testimage",
            working_dir="/working/dir",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://host/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
        )
        request = JobRequest(
            job_id="testjob",
            description="Description of the testjob",
            container=container,
        )
        assert request.to_primitive() == job_request_payload

    def test_to_primitive_with_tty(self, job_request_payload: dict[str, Any]) -> None:
        job_request_payload["container"]["tty"] = True

        container = Container(
            image="testimage",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://host/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
            tty=True,
        )
        request = JobRequest(
            job_id="testjob",
            description="Description of the testjob",
            container=container,
        )
        assert request.to_primitive() == job_request_payload

    def test_from_primitive(self, job_request_payload: dict[str, Any]) -> None:
        request = JobRequest.from_primitive(job_request_payload)
        assert request.job_id == "testjob"
        assert request.description == "Description of the testjob"
        expected_container = Container(
            image="testimage",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://host/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
        )
        assert request.container == expected_container

    def test_from_primitive_with_working_dir(
        self, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"]["working_dir"] = "/working/dir"
        request = JobRequest.from_primitive(job_request_payload)
        assert request.job_id == "testjob"
        assert request.description == "Description of the testjob"
        expected_container = Container(
            image="testimage",
            working_dir="/working/dir",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory=128 * 10**6),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://host/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
        )
        assert request.container == expected_container

    def test_from_primitive_with_shm(
        self, job_request_payload_with_shm: dict[str, Any]
    ) -> None:
        request = JobRequest.from_primitive(job_request_payload_with_shm)
        assert request.job_id == "testjob"
        assert request.description == "Description of the testjob"
        expected_container = Container(
            image="testimage",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory=128 * 10**6, shm=True),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://host/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
        )
        assert request.container == expected_container

    def test_to_and_from_primitive(self, job_request_payload: dict[str, Any]) -> None:
        actual = JobRequest.to_primitive(JobRequest.from_primitive(job_request_payload))
        assert actual == job_request_payload

    def test_to_and_from_primitive_with_shm(
        self, job_request_payload_with_shm: dict[str, Any]
    ) -> None:
        actual = JobRequest.to_primitive(
            JobRequest.from_primitive(job_request_payload_with_shm)
        )
        assert actual == job_request_payload_with_shm

    def test_to_and_from_primitive_with_tpu(
        self, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"]["resources"]["tpu"] = {
            "type": "v2-8",
            "software_version": "1.14",
        }
        actual = JobRequest.to_primitive(JobRequest.from_primitive(job_request_payload))
        assert actual == job_request_payload

    def test_to_and_from_primitive_with_secret_env(
        self, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"]["secret_env"] = {
            "ENV_SECRET1": "secret://clustername/username/key1",
            "ENV_SECRET2": "secret://clustername/username/key2",
        }
        actual = JobRequest.to_primitive(JobRequest.from_primitive(job_request_payload))
        assert actual == job_request_payload

    def test_to_and_from_primitive_with_secret_volumes(
        self, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"]["secret_volumes"] = [
            {
                "src_secret_uri": "secret://clustername/username/key1",
                "dst_path": "/container/path1",
            },
            {
                "src_secret_uri": "secret://clustername/username/key2",
                "dst_path": "/container/path2",
            },
        ]
        actual = JobRequest.to_primitive(JobRequest.from_primitive(job_request_payload))
        assert actual == job_request_payload

    def test_to_and_from_primitive_with_disk_volumes(
        self, job_request_payload: dict[str, Any]
    ) -> None:
        job_request_payload["container"]["disk_volumes"] = [
            {
                "src_disk_uri": "disk://clustername/username/disk-id-1",
                "dst_path": "/container/path1",
                "read_only": True,
            },
            {
                "src_disk_uri": "disk://clustername/username/disk-id-2",
                "dst_path": "/container/path2",
                "read_only": False,
            },
        ]
        actual = JobRequest.to_primitive(JobRequest.from_primitive(job_request_payload))
        assert actual == job_request_payload


class TestContainerHTTPServer:
    def test_from_primitive(self) -> None:
        payload = {"port": 1234}
        server = ContainerHTTPServer.from_primitive(payload)
        assert server == ContainerHTTPServer(port=1234)

    def test_from_primitive_health_check_path(self) -> None:
        payload = {"port": 1234, "health_check_path": "/path"}
        server = ContainerHTTPServer.from_primitive(payload)
        assert server == ContainerHTTPServer(port=1234, health_check_path="/path")

    def test_from_primitive_requires_auth(self) -> None:
        payload = {"port": 1234, "requires_auth": True}
        server = ContainerHTTPServer.from_primitive(payload)
        assert server == ContainerHTTPServer(port=1234, requires_auth=True)

    def test_to_primitive(self) -> None:
        server = ContainerHTTPServer(port=1234)
        assert server.to_primitive() == {
            "port": 1234,
            "health_check_path": "/",
            "requires_auth": False,
        }

    def test_to_primitive_health_check_path(self) -> None:
        server = ContainerHTTPServer(port=1234, health_check_path="/path")
        assert server.to_primitive() == {
            "port": 1234,
            "health_check_path": "/path",
            "requires_auth": False,
        }


class TestJobStatusItem:
    def test_from_primitive(self) -> None:
        transition_time = datetime.now(timezone.utc)
        payload = {
            "status": "succeeded",
            "transition_time": transition_time.isoformat(),
            "reason": "test reason",
            "description": "test description",
            "exit_code": 0,
        }
        item = JobStatusItem.from_primitive(payload)
        assert item.status == JobStatus.SUCCEEDED
        assert item.is_finished
        assert item.transition_time == transition_time
        assert item.reason == "test reason"
        assert item.description == "test description"
        assert item.exit_code == 0

    def test_from_primitive_without_exit_code(self) -> None:
        transition_time = datetime.now(timezone.utc)
        payload = {
            "status": "succeeded",
            "transition_time": transition_time.isoformat(),
            "reason": "test reason",
            "description": "test description",
        }
        item = JobStatusItem.from_primitive(payload)
        assert item.status == JobStatus.SUCCEEDED
        assert item.is_finished
        assert item.transition_time == transition_time
        assert item.reason == "test reason"
        assert item.description == "test description"
        assert item.exit_code is None

    def test_to_primitive(self) -> None:
        item = JobStatusItem(
            status=JobStatus.SUCCEEDED,
            transition_time=datetime.now(timezone.utc),
            exit_code=321,
        )
        assert item.to_primitive() == {
            "status": "succeeded",
            "transition_time": item.transition_time.isoformat(),
            "reason": None,
            "description": None,
            "exit_code": 321,
        }

    def test_eq_defaults(self) -> None:
        old_item = JobStatusItem.create(JobStatus.RUNNING)
        new_item = JobStatusItem.create(JobStatus.RUNNING)
        assert old_item == new_item

    def test_eq_different_times(self) -> None:
        old_item = JobStatusItem.create(
            JobStatus.RUNNING, transition_time=datetime.now(timezone.utc)
        )
        new_item = JobStatusItem.create(
            JobStatus.RUNNING,
            transition_time=datetime.now(timezone.utc) + timedelta(days=1),
        )
        assert old_item == new_item

    def test_not_eq(self) -> None:
        old_item = JobStatusItem.create(JobStatus.RUNNING)
        new_item = JobStatusItem.create(JobStatus.RUNNING, reason="Whatever")
        assert old_item != new_item


class TestJobStatusHistory:
    def test_single_pending(self) -> None:
        first_item = JobStatusItem.create(JobStatus.PENDING)
        items = [first_item]
        history = JobStatusHistory(items=items)
        assert history.first == first_item
        assert history.last == first_item
        assert history.current == first_item
        assert history.created_at == first_item.transition_time
        assert history.created_at_str == first_item.transition_time.isoformat()
        assert history.created_at_timestamp == first_item.transition_time.timestamp()
        assert not history.started_at
        assert not history.started_at_str
        assert not history.is_suspended
        assert not history.is_finished
        assert not history.finished_at
        assert not history.finished_at_str

    def test_single_failed(self) -> None:
        first_item = JobStatusItem.create(JobStatus.FAILED)
        items = [first_item]
        history = JobStatusHistory(items=items)
        assert history.first == first_item
        assert history.last == first_item
        assert history.current == first_item
        assert history.created_at == first_item.transition_time
        assert history.started_at == first_item.transition_time
        assert history.is_finished
        assert not history.is_running
        assert not history.is_suspended
        assert history.finished_at == first_item.transition_time

    def test_full_cycle(self) -> None:
        pending_item = JobStatusItem.create(JobStatus.PENDING)
        running_item = JobStatusItem.create(JobStatus.RUNNING)
        finished_item = JobStatusItem.create(JobStatus.SUCCEEDED)
        items = [pending_item, running_item, finished_item]
        history = JobStatusHistory(items=items)
        assert history.first == pending_item
        assert history.last == finished_item
        assert history.current == finished_item
        assert history.created_at == pending_item.transition_time
        assert history.created_at_str == pending_item.transition_time.isoformat()
        assert history.started_at == running_item.transition_time
        assert history.started_at_str == running_item.transition_time.isoformat()
        assert history.is_finished
        assert not history.is_running
        assert not history.is_suspended
        assert history.finished_at == finished_item.transition_time
        assert history.finished_at_str == finished_item.transition_time.isoformat()

    def test_suspended(self) -> None:
        pending_item = JobStatusItem.create(JobStatus.PENDING)
        running_item = JobStatusItem.create(JobStatus.RUNNING)
        suspended_item = JobStatusItem.create(JobStatus.SUSPENDED)
        items = [pending_item, running_item, suspended_item]
        history = JobStatusHistory(items=items)
        assert history.first == pending_item
        assert history.last == suspended_item
        assert history.current == suspended_item
        assert history.created_at == pending_item.transition_time
        assert history.created_at_str == pending_item.transition_time.isoformat()
        assert history.started_at == running_item.transition_time
        assert history.started_at_str == running_item.transition_time.isoformat()
        assert history.is_suspended
        assert not history.is_running
        assert not history.is_finished

    def test_resurraction(self) -> None:
        pending_item = JobStatusItem.create(JobStatus.PENDING)
        running_item = JobStatusItem.create(JobStatus.RUNNING)
        finished_item = JobStatusItem.create(JobStatus.SUCCEEDED)
        items = [pending_item, running_item, finished_item, running_item]
        history = JobStatusHistory(items=items)
        assert history.first == pending_item
        assert history.last == running_item
        assert history.current == running_item
        assert history.created_at == pending_item.transition_time
        assert history.created_at_str == pending_item.transition_time.isoformat()
        assert history.started_at == running_item.transition_time
        assert history.started_at_str == running_item.transition_time.isoformat()
        assert not history.is_finished
        assert not history.is_suspended
        assert history.is_running
        assert not history.finished_at
        assert not history.finished_at_str

    def test_current_update(self) -> None:
        pending_item = JobStatusItem.create(JobStatus.PENDING)
        running_item = JobStatusItem.create(JobStatus.RUNNING)

        items = [pending_item]
        history = JobStatusHistory(items=items)
        assert history.current == pending_item
        assert not history.is_running

        history.current = running_item
        assert history.current == running_item
        assert history.is_running

    def test_current_discard_update(self) -> None:
        pending_item = JobStatusItem.create(JobStatus.PENDING)
        new_pending_item = JobStatusItem.create(
            JobStatus.PENDING,
            transition_time=pending_item.transition_time + timedelta(days=1),
        )

        items = [pending_item]
        history = JobStatusHistory(items=items)
        assert history.current == pending_item

        history.current = new_pending_item
        assert history.current == pending_item
        assert history.current.transition_time == pending_item.transition_time

    def test_restarts_count(self) -> None:
        pending_item = JobStatusItem.create(JobStatus.PENDING)
        running_item = JobStatusItem.create(
            JobStatus.RUNNING,
            transition_time=pending_item.transition_time + timedelta(days=1),
        )

        items = [pending_item, running_item]
        history = JobStatusHistory(items=items)
        assert history.restart_count == 0

        restarting_item = JobStatusItem.create(
            JobStatus.RUNNING, reason=JobStatusReason.RESTARTING
        )
        history.current = restarting_item

        assert history.restart_count == 1
        history.current = running_item
        history.current = restarting_item
        assert history.restart_count == 2


def test_maybe_job_id() -> None:
    assert maybe_job_id("job-1")
    assert not maybe_job_id("")
    assert not maybe_job_id("jobname")
