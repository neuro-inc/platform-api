import dataclasses
from datetime import datetime, timedelta, timezone
from pathlib import PurePath
from typing import Any, Callable, Dict
from unittest import mock

import pytest
from neuro_auth_client.client import Quota
from yarl import URL

from platform_api.cluster_config import RegistryConfig, StorageConfig
from platform_api.handlers.job_request_builder import create_container_from_payload
from platform_api.orchestrator.job import (
    AggregatedRunTime,
    Job,
    JobRecord,
    JobStatusHistory,
    JobStatusItem,
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerSSHServer,
    ContainerTPUResource,
    ContainerVolume,
    ContainerVolumeFactory,
    JobError,
    JobRequest,
    JobStatus,
)
from platform_api.user import User

from .conftest import MockOrchestrator


class TestContainer:
    def test_command_entrypoint_list_command_list_empty(self) -> None:
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
        )
        assert container.entrypoint_list == []
        assert container.command_list == []

    def test_command_list_non_empty(self) -> None:
        container = Container(
            image="testimage",
            command="bash -c date",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        assert container.command_list == ["bash", "-c", "date"]

    def test_command_list_invalid(self) -> None:
        container = Container(
            image="testimage",
            command='"',
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        with pytest.raises(JobError, match="invalid command format"):
            container.command_list

    def test_entrypoint_list_invalid(self) -> None:
        container = Container(
            image="testimage",
            entrypoint='"',
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        with pytest.raises(JobError, match="invalid command format"):
            container.entrypoint_list

    def test_entrypoint_list_non_empty(self) -> None:
        container = Container(
            image="testimage",
            entrypoint="bash -c date",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        assert container.entrypoint_list == ["bash", "-c", "date"]

    def test_command_list_entrypoint_list_non_empty(self) -> None:
        container = Container(
            image="testimage",
            entrypoint="/script.sh",
            command="arg1 arg2 arg3",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        assert container.entrypoint_list == ["/script.sh"]
        assert container.command_list == ["arg1", "arg2", "arg3"]

    def test_belongs_to_registry_no_host(self) -> None:
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
        )
        registry_config = RegistryConfig(
            url=URL("http://example.com"), username="compute", password="compute_token"
        )
        assert not container.belongs_to_registry(registry_config)

    def test_belongs_to_registry_different_host(self) -> None:
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(
            url=URL("http://example.com"), username="compute", password="compute_token"
        )
        assert not container.belongs_to_registry(registry_config)

    def test_belongs_to_registry(self) -> None:
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(
            url=URL("http://example.com"), username="compute", password="compute_token"
        )
        assert container.belongs_to_registry(registry_config)

    def test_to_image_uri_failure(self) -> None:
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(
            url=URL("http://example.com"), username="compute", password="compute_token"
        )
        with pytest.raises(AssertionError, match="Unknown registry"):
            container.to_image_uri(registry_config, "test-cluster")

    def test_to_image_uri(self) -> None:
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(
            url=URL("http://example.com"), username="compute", password="compute_token"
        )
        uri = container.to_image_uri(registry_config, "test-cluster")
        assert uri == URL("image://test-cluster/project/testimage")

    def test_to_image_uri_no_cluster_name(self) -> None:
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(
            url=URL("http://example.com"), username="compute", password="compute_token"
        )
        uri = container.to_image_uri(registry_config, None)
        assert uri == URL("image://project/testimage")

    def test_to_image_uri_registry_with_custom_port(self) -> None:
        container = Container(
            image="example.com:5000/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(
            url=URL("http://example.com:5000"),
            username="compute",
            password="compute_token",
        )
        uri = container.to_image_uri(registry_config, "test-cluster")
        assert uri == URL("image://test-cluster/project/testimage")

    def test_to_image_uri_ignore_tag(self) -> None:
        container = Container(
            image="example.com/project/testimage:latest",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(
            url=URL("http://example.com"), username="compute", password="compute_token"
        )
        uri = container.to_image_uri(registry_config, "test-cluster")
        assert uri == URL("image://test-cluster/project/testimage")


class TestContainerVolumeFactory:
    def test_invalid_storage_uri_scheme(self) -> None:
        uri = "invalid://path"
        with pytest.raises(ValueError, match="Invalid URI scheme"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/"),
                dst_mount_path=PurePath("/"),
                cluster_name="test-cluster",
            )

    @pytest.mark.parametrize(
        "uri",
        (
            "storage:///",
            "storage://",
            "storage://test-cluster",
            "storage://test-cluster/",
        ),
    )
    def test_invalid_storage_uri_path(self, uri: str) -> None:
        volume = ContainerVolumeFactory(
            uri,
            src_mount_path=PurePath("/host"),
            dst_mount_path=PurePath("/container"),
            cluster_name="test-cluster",
        ).create()
        assert volume.src_path == PurePath("/host")
        assert volume.dst_path == PurePath("/container")
        assert not volume.read_only

    @pytest.mark.parametrize(
        "uri",
        (
            "storage://test-cluster/path/to/dir",
            "storage://test-cluster/path/to//dir",
            "storage://test-cluster/path/to/./dir",
            "storage://path/to/dir",
            "storage://path/to//dir",
            "storage://path/to/./dir",
            "storage:///path/to/dir",
        ),
    )
    def test_create(self, uri: str) -> None:
        volume = ContainerVolume.create(
            uri,
            src_mount_path=PurePath("/host"),
            dst_mount_path=PurePath("/container"),
            read_only=True,
            cluster_name="test-cluster",
        )
        assert volume.src_path == PurePath("/host/path/to/dir")
        assert volume.dst_path == PurePath("/container/path/to/dir")
        assert volume.read_only

    @pytest.mark.parametrize(
        "uri",
        (
            "storage:///../to/dir",
            "storage://path/../dir",
            "storage://test-cluster/path/../dir",
        ),
    )
    def test_create_invalid_path(self, uri: str) -> None:
        with pytest.raises(ValueError, match="Invalid path"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/host"),
                dst_mount_path=PurePath("/container"),
                cluster_name="test-cluster",
            ).create()

    def test_create_without_extending_dst_mount_path(self) -> None:
        uri = "storage://test-cluster/path/to/dir"
        volume = ContainerVolume.create(
            uri,
            src_mount_path=PurePath("/host"),
            dst_mount_path=PurePath("/container"),
            read_only=True,
            extend_dst_mount_path=False,
            cluster_name="test-cluster",
        )
        assert volume.src_path == PurePath("/host/path/to/dir")
        assert volume.dst_path == PurePath("/container")
        assert volume.read_only

    def test_relative_dst_mount_path(self) -> None:
        uri = "storage://test-cluster/path/to/dir"
        with pytest.raises(ValueError, match="Mount path must be absolute"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/host"),
                dst_mount_path=PurePath("container"),
                cluster_name="test-cluster",
            )

    def test_dots_dst_mount_path(self) -> None:
        uri = "storage://test-cluster/path/to/dir"
        with pytest.raises(ValueError, match="Invalid path"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/host"),
                dst_mount_path=PurePath("/container/../path"),
                cluster_name="test-cluster",
            )


class TestContainerBuilder:
    def test_from_payload_build(self) -> None:
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
        payload = {
            "image": "testimage",
            "entrypoint": "testentrypoint",
            "command": "testcommand",
            "env": {"TESTVAR": "testvalue"},
            "resources": {"cpu": 0.1, "memory_mb": 128, "gpu": 1},
            "http": {"port": 80},
            "volumes": [
                {
                    "src_storage_uri": "storage://path/to/dir",
                    "dst_path": "/container/path",
                    "read_only": True,
                }
            ],
        }
        container = create_container_from_payload(
            payload, storage_config=storage_config, cluster_name="test-cluster"
        )
        assert container == Container(
            image="testimage",
            entrypoint="testentrypoint",
            command="testcommand",
            env={"TESTVAR": "testvalue"},
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path/to/dir"),
                    src_path=PurePath("/tmp/path/to/dir"),
                    dst_path=PurePath("/container/path"),
                    read_only=True,
                )
            ],
            resources=ContainerResources(cpu=0.1, memory_mb=128, gpu=1, shm=None),
            http_server=ContainerHTTPServer(port=80, health_check_path="/"),
            ssh_server=None,
        )

    def test_from_payload_build_gpu_model(self) -> None:
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 128,
                "gpu": 1,
                "gpu_model": "gpumodel",
            },
        }
        container = create_container_from_payload(
            payload, storage_config=storage_config, cluster_name="test-cluster"
        )
        assert container == Container(
            image="testimage",
            resources=ContainerResources(
                cpu=0.1, memory_mb=128, gpu=1, gpu_model_id="gpumodel"
            ),
        )

    def test_from_payload_build_tpu(self) -> None:
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 128,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
        }
        container = create_container_from_payload(
            payload, storage_config=storage_config, cluster_name="test-cluster"
        )
        assert container == Container(
            image="testimage",
            resources=ContainerResources(
                cpu=0.1,
                memory_mb=128,
                tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
            ),
        )

    def test_from_payload_build_with_ssh(self) -> None:
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
        payload = {
            "image": "testimage",
            "command": "testcommand",
            "env": {"TESTVAR": "testvalue"},
            "resources": {"cpu": 0.1, "memory_mb": 128, "gpu": 1},
            "http": {"port": 80},
            "ssh": {"port": 22},
            "volumes": [
                {
                    "src_storage_uri": "storage://path/to/dir",
                    "dst_path": "/container/path",
                    "read_only": True,
                }
            ],
        }
        container = create_container_from_payload(
            payload, storage_config=storage_config, cluster_name="test-cluster"
        )
        assert container == Container(
            image="testimage",
            command="testcommand",
            env={"TESTVAR": "testvalue"},
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path/to/dir"),
                    src_path=PurePath("/tmp/path/to/dir"),
                    dst_path=PurePath("/container/path"),
                    read_only=True,
                )
            ],
            resources=ContainerResources(cpu=0.1, memory_mb=128, gpu=1, shm=None),
            http_server=ContainerHTTPServer(port=80, health_check_path="/"),
            ssh_server=ContainerSSHServer(port=22),
        )

    def test_from_payload_build_with_shm_false(self) -> None:
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
        payload = {
            "image": "testimage",
            "command": "testcommand",
            "env": {"TESTVAR": "testvalue"},
            "resources": {"cpu": 0.1, "memory_mb": 128, "gpu": 1, "shm": True},
            "http": {"port": 80},
            "volumes": [
                {
                    "src_storage_uri": "storage://path/to/dir",
                    "dst_path": "/container/path",
                    "read_only": True,
                }
            ],
        }
        container = create_container_from_payload(
            payload, storage_config=storage_config, cluster_name="test-cluster"
        )
        assert container == Container(
            image="testimage",
            command="testcommand",
            env={"TESTVAR": "testvalue"},
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path/to/dir"),
                    src_path=PurePath("/tmp/path/to/dir"),
                    dst_path=PurePath("/container/path"),
                    read_only=True,
                )
            ],
            resources=ContainerResources(cpu=0.1, memory_mb=128, gpu=1, shm=True),
            http_server=ContainerHTTPServer(port=80, health_check_path="/"),
        )


@pytest.fixture
def job_request_payload() -> Dict[str, Any]:
    return {
        "job_id": "testjob",
        "description": "Description of the testjob",
        "container": {
            "image": "testimage",
            "resources": {"cpu": 1, "memory_mb": 128},
            "command": None,
            "env": {"testvar": "testval"},
            "volumes": [
                {
                    "uri": "storage://path",
                    "src_path": "/src/path",
                    "dst_path": "/dst/path",
                    "read_only": False,
                }
            ],
            "http_server": None,
            "ssh_server": None,
        },
    }


@pytest.fixture
def job_payload(job_request_payload: Any) -> Dict[str, Any]:
    finished_at_str = datetime.now(timezone.utc).isoformat()
    return {
        "id": "testjob",
        "request": job_request_payload,
        "status": "succeeded",
        "is_deleted": True,
        "finished_at": finished_at_str,
        "statuses": [{"status": "failed", "transition_time": finished_at_str}],
    }


@pytest.fixture
def job_request_payload_with_shm(job_request_payload: Dict[str, Any]) -> Dict[str, Any]:
    data = job_request_payload
    data["container"]["resources"]["shm"] = True
    return data


@pytest.fixture
def job_request() -> JobRequest:
    container = Container(
        image="testimage",
        resources=ContainerResources(cpu=1, memory_mb=128),
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
            status=JobStatus.FAILED, request=job_request, cluster_name="test-cluster"
        )
        assert record.finished_at
        assert record.should_be_deleted(delay=timedelta(0))


class TestJob:
    @pytest.fixture
    def job_request_with_gpu(self) -> JobRequest:
        container = Container(
            image="testimage",
            resources=ContainerResources(
                cpu=1, memory_mb=64, gpu=1, gpu_model_id="nvidia-tesla-k80"
            ),
        )
        return JobRequest(
            job_id="testjob",
            container=container,
            description="Description of the testjob with gpu",
        )

    @pytest.fixture
    def job_request_with_ssh_and_http(self) -> JobRequest:
        container = Container(
            image="testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
            http_server=ContainerHTTPServer(port=1234),
            ssh_server=ContainerSSHServer(port=4321),
        )
        return JobRequest(
            job_id="testjob",
            container=container,
            description="Description of the testjob",
        )

    @pytest.fixture
    def job_request_with_ssh(self) -> JobRequest:
        container = Container(
            image="testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
            ssh_server=ContainerSSHServer(port=4321),
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
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert job.http_host == "testjob.jobs"

    def test_http_host_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                name="test-job-name",
                owner="owner",
            ),
        )
        assert job.http_host == "testjob.jobs"
        assert job.http_host_named == "test-job-name--owner.jobs"

    def test_job_name(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
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
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert not job.has_gpu

    def test_job_has_gpu_true(
        self, mock_orchestrator: MockOrchestrator, job_request_with_gpu: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request_with_gpu, cluster_name="test-cluster"
            ),
        )
        assert job.has_gpu

    def _mocked_datetime_factory(self) -> datetime:
        return datetime(year=2019, month=1, day=1)

    @pytest.fixture
    def job_factory(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> Callable[..., Job]:
        current_datetime_factory = self._mocked_datetime_factory

        def _f(
            job_status_history: JobStatusHistory, is_preemptible: bool = False
        ) -> Job:
            return Job(
                storage_config=mock_orchestrator.storage_config,
                orchestrator_config=mock_orchestrator.config,
                record=JobRecord.create(
                    request=job_request,
                    cluster_name="test-cluster",
                    status_history=job_status_history,
                    current_datetime_factory=current_datetime_factory,
                    is_preemptible=is_preemptible,
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

    def test_job_get_run_time_pending_job_preemptible(
        self, mock_orchestrator: MockOrchestrator, job_factory: Callable[..., Job]
    ) -> None:
        time_now = self._mocked_datetime_factory()
        started_at = time_now - timedelta(minutes=30)
        items = [JobStatusItem.create(JobStatus.PENDING, transition_time=started_at)]

        job = job_factory(JobStatusHistory(items), is_preemptible=True)
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

    def test_job_get_run_time_running_job_preemptible(
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
        job = job_factory(JobStatusHistory(items), is_preemptible=True)

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
    def test_job_get_run_time_terminated_job_preemptible(
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
        job = job_factory(JobStatusHistory(items), is_preemptible=True)

        assert job.get_run_time() == running_sum_delta

    def test_http_url(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert job.http_url == "http://testjob.jobs"

    def test_http_urls_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                name="test-job-name",
                owner="owner",
            ),
        )
        assert job.http_url == "http://testjob.jobs"
        assert job.http_url_named == "http://test-job-name--owner.jobs"

    def test_https_url(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        config = dataclasses.replace(
            mock_orchestrator.config, is_http_ingress_secure=True
        )
        job = Job(
            storage_config=mock_orchestrator.storage_config,
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
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                name="test-job-name",
                owner="owner",
            ),
        )
        assert job.http_url == "https://testjob.jobs"
        assert job.http_url_named == "https://test-job-name--owner.jobs"

    def test_ssh_url(
        self, mock_orchestrator: MockOrchestrator, job_request_with_ssh: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request_with_ssh, cluster_name="test-cluster"
            ),
        )
        assert job.ssh_server == "ssh://nobody@ssh-auth:22"

    def test_no_ssh(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert job.ssh_server == "ssh://nobody@ssh-auth:22"

    def test_http_url_and_ssh(
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_with_ssh_and_http: JobRequest,
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request_with_ssh_and_http, cluster_name="test-cluster"
            ),
        )
        assert job.http_url == "http://testjob.jobs"
        assert job.ssh_server == "ssh://nobody@ssh-auth:22"

    def test_http_url_and_ssh_named(
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_with_ssh_and_http: JobRequest,
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request_with_ssh_and_http,
                cluster_name="test-cluster",
                name="test-job-name",
                owner="owner",
            ),
        )
        assert job.http_url == "http://testjob.jobs"
        assert job.http_url_named == "http://test-job-name--owner.jobs"
        assert job.ssh_server == "ssh://nobody@ssh-auth:22"

    def test_to_primitive(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                owner="testuser",
                name="test-job-name",
                is_preemptible=True,
                schedule_timeout=15,
            ),
        )
        job.status = JobStatus.FAILED
        job.is_deleted = True
        assert job.finished_at
        expected_finished_at = job.finished_at.isoformat()
        assert job.to_primitive() == {
            "id": job.id,
            "name": "test-job-name",
            "owner": "testuser",
            "cluster_name": "test-cluster",
            "request": job_request.to_primitive(),
            "status": "failed",
            "is_deleted": True,
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
            "is_preemptible": True,
            "schedule_timeout": 15,
        }

    def test_to_primitive_with_max_run_time(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
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
            "is_deleted": False,
            "finished_at": None,
            "is_preemptible": False,
            "max_run_time_minutes": 500,
        }

    def test_from_primitive(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.id == "testjob"
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_deleted
        assert job.finished_at
        assert job.description == "Description of the testjob"
        assert job.name is None
        assert job.owner == "testuser"
        assert not job.is_preemptible
        assert job.max_run_time == timedelta.max

    def test_from_primitive_check_name(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.id == "testjob"
        assert job.name == "test-job-name"

    def test_from_primitive_with_statuses(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        finished_at_str = datetime.now(timezone.utc).isoformat()
        payload = {
            "id": "testjob",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": finished_at_str,
            "statuses": [{"status": "failed", "transition_time": finished_at_str}],
            "is_preemptible": True,
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.id == "testjob"
        assert job.status == JobStatus.FAILED
        assert job.is_deleted
        assert job.finished_at
        assert job.description == "Description of the testjob"
        assert job.owner == "compute"
        assert job.is_preemptible

    def test_from_primitive_with_cluster_name(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "owner": "testuser",
            "cluster_name": "testcluster",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.id == "testjob"
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_deleted
        assert job.finished_at
        assert job.description == "Description of the testjob"
        assert job.name is None
        assert job.owner == "testuser"
        assert job.cluster_name == "testcluster"
        assert not job.is_preemptible

    def test_from_primitive_with_entrypoint_without_command(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        job_request_payload["container"]["entrypoint"] = "/script.sh"
        job_request_payload["container"].pop("command", None)
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.request.container.command is None
        assert job.request.container.entrypoint == "/script.sh"

    def test_from_primitive_without_entrypoint_with_command(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        job_request_payload["container"].pop("entrypoint", None)
        job_request_payload["container"]["command"] = "arg1 arg2 arg3"
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.request.container.command == "arg1 arg2 arg3"
        assert job.request.container.entrypoint is None

    def test_from_primitive_without_entrypoint_without_command(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        job_request_payload["container"].pop("entrypoint", None)
        job_request_payload["container"].pop("command", None)
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.request.container.command is None
        assert job.request.container.entrypoint is None

    def test_from_primitive_with_entrypoint_with_command(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        job_request_payload["container"]["entrypoint"] = "/script.sh"
        job_request_payload["container"]["command"] = "arg1 arg2 arg3"
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.request.container.command == "arg1 arg2 arg3"
        assert job.request.container.entrypoint == "/script.sh"

    def test_from_primitive_with_max_run_time_minutes(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "max_run_time_minutes": 100,
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.max_run_time == timedelta(minutes=100)

    def test_from_primitive_with_max_run_time_minutes_none(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "max_run_time_minutes": None,
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        assert job.max_run_time == timedelta.max

    def test_from_primitive_with_max_run_time_minutes_zero(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "max_run_time_minutes": 0,
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        with pytest.raises(
            AssertionError, match="max_run_time_minutes must be positive, got: 0"
        ):
            job.max_run_time

    def test_from_primitive_with_max_run_time_minutes_negative(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
    ) -> None:
        payload = {
            "id": "testjob",
            "name": "test-job-name",
            "owner": "testuser",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "max_run_time_minutes": -1,
        }
        job = Job.from_primitive(
            mock_orchestrator.storage_config, mock_orchestrator.config, payload
        )
        with pytest.raises(
            AssertionError, match="max_run_time_minutes must be positive, got: -1"
        ):
            job.max_run_time

    def test_to_uri(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.storage_config,
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
            mock_orchestrator.storage_config,
            mock_orchestrator.config,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        assert job.to_uri() == URL(f"job://test-cluster/compute/{job.id}")

    def test_to_uri_no_use_cluster_name(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.storage_config,
            mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request, cluster_name="test-cluster", owner="testuser"
            ),
        )
        assert job.to_uri(use_cluster_names_in_uris=False) == URL(
            f"job://testuser/{job.id}"
        )

    def test_to_uri_no_cluster(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.storage_config,
            mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request, cluster_name="", owner="testuser"
            ),
        )
        assert job.to_uri() == URL(f"job://testuser/{job.id}")

    def test_to_uri_no_owner(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.storage_config,
            mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request, cluster_name="test-cluster", orphaned_job_owner=""
            ),
        )
        assert job.to_uri() == URL(f"job://test-cluster/{job.id}")

    def test_to_uri_no_cluster_no_owner(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.storage_config,
            mock_orchestrator.config,
            record=JobRecord.create(
                request=job_request, cluster_name="", orphaned_job_owner=""
            ),
        )
        assert job.to_uri() == URL(f"job:/{job.id}")

    def test_to_and_from_primitive(
        self, mock_orchestrator: MockOrchestrator, job_request_payload: Dict[str, Any]
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
            "status": current_status_item["status"],
            "statuses": [current_status_item],
            "is_deleted": "False",
            "finished_at": finished_at_str,
            "is_preemptible": False,
        }
        actual = Job.to_primitive(
            Job.from_primitive(
                mock_orchestrator.storage_config, mock_orchestrator.config, expected
            )
        )
        assert actual == expected


class TestJobRequest:
    def test_to_primitive(self, job_request_payload: Dict[str, Any]) -> None:
        container = Container(
            image="testimage",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory_mb=128),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path"),
                    src_path=PurePath("/src/path"),
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
        self, job_request_payload: Dict[str, Any]
    ) -> None:
        job_request_payload["container"]["entrypoint"] = "/bin/ls"
        container = Container(
            image="testimage",
            entrypoint="/bin/ls",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory_mb=128),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path"),
                    src_path=PurePath("/src/path"),
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

    def test_to_primitive_with_ssh(self, job_request_payload: Dict[str, Any]) -> None:
        job_request_payload["container"]["ssh_server"] = {"port": 678}

        container = Container(
            image="testimage",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory_mb=128),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path"),
                    src_path=PurePath("/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
            ssh_server=ContainerSSHServer(678),
        )
        request = JobRequest(
            job_id="testjob",
            description="Description of the testjob",
            container=container,
        )
        assert request.to_primitive() == job_request_payload

    def test_from_primitive(self, job_request_payload: Dict[str, Any]) -> None:
        request = JobRequest.from_primitive(job_request_payload)
        assert request.job_id == "testjob"
        assert request.description == "Description of the testjob"
        expected_container = Container(
            image="testimage",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory_mb=128),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path"),
                    src_path=PurePath("/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
        )
        assert request.container == expected_container

    def test_from_primitive_with_ssh(self, job_request_payload: Dict[str, Any]) -> None:
        job_request_payload["container"]["ssh_server"] = {"port": 678}
        request = JobRequest.from_primitive(job_request_payload)
        assert request.job_id == "testjob"
        assert request.description == "Description of the testjob"
        expected_container = Container(
            image="testimage",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory_mb=128),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path"),
                    src_path=PurePath("/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
            ssh_server=ContainerSSHServer(port=678),
        )
        assert request.container == expected_container

    def test_from_primitive_with_shm(
        self, job_request_payload_with_shm: Dict[str, Any]
    ) -> None:
        request = JobRequest.from_primitive(job_request_payload_with_shm)
        assert request.job_id == "testjob"
        assert request.description == "Description of the testjob"
        expected_container = Container(
            image="testimage",
            env={"testvar": "testval"},
            resources=ContainerResources(cpu=1, memory_mb=128, shm=True),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path"),
                    src_path=PurePath("/src/path"),
                    dst_path=PurePath("/dst/path"),
                )
            ],
        )
        assert request.container == expected_container

    def test_to_and_from_primitive(self, job_request_payload: Dict[str, Any]) -> None:
        actual = JobRequest.to_primitive(JobRequest.from_primitive(job_request_payload))
        assert actual == job_request_payload

    def test_to_and_from_primitive_with_shm(
        self, job_request_payload_with_shm: Dict[str, Any]
    ) -> None:
        actual = JobRequest.to_primitive(
            JobRequest.from_primitive(job_request_payload_with_shm)
        )
        assert actual == job_request_payload_with_shm

    def test_to_and_from_primitive_with_ssh(
        self, job_request_payload: Dict[str, Any]
    ) -> None:
        job_request_payload["container"]["ssh_server"] = {"port": 678}
        actual = JobRequest.to_primitive(JobRequest.from_primitive(job_request_payload))
        assert actual == job_request_payload

    def test_to_and_from_primitive_with_tpu(
        self, job_request_payload: Dict[str, Any]
    ) -> None:
        job_request_payload["container"]["resources"]["tpu"] = {
            "type": "v2-8",
            "software_version": "1.14",
        }
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


class TestContainerSSHServer:
    def test_from_primitive(self) -> None:
        payload = {"port": 1234}
        server = ContainerSSHServer.from_primitive(payload)
        assert server == ContainerSSHServer(port=1234)

    def test_to_primitive(self) -> None:
        server = ContainerSSHServer(port=1234)
        assert server.to_primitive() == {"port": 1234}


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
        assert history.created_at_str == (pending_item.transition_time.isoformat())
        assert history.started_at == running_item.transition_time
        assert history.started_at_str == (running_item.transition_time.isoformat())
        assert history.is_finished
        assert not history.is_running
        assert history.finished_at == finished_item.transition_time
        assert history.finished_at_str == (finished_item.transition_time.isoformat())

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


class TestAggregatedRunTime:
    @pytest.mark.parametrize(
        "quota",
        [
            Quota(total_gpu_run_time_minutes=None, total_non_gpu_run_time_minutes=10),
            Quota(total_gpu_run_time_minutes=10, total_non_gpu_run_time_minutes=None),
            Quota(total_gpu_run_time_minutes=10, total_non_gpu_run_time_minutes=10),
        ],
    )
    def test_from_quota_initialized(self, quota: Quota) -> None:
        run_time = AggregatedRunTime.from_quota(quota)
        assert run_time == AggregatedRunTime(
            total_gpu_run_time_delta=quota.total_gpu_run_time_delta,
            total_non_gpu_run_time_delta=quota.total_non_gpu_run_time_delta,
        )

    def test_from_quota_non_initialized(self) -> None:
        quota = Quota(
            total_gpu_run_time_minutes=None, total_non_gpu_run_time_minutes=None
        )
        run_time = AggregatedRunTime.from_quota(quota)
        assert run_time == AggregatedRunTime(
            total_gpu_run_time_delta=timedelta.max,
            total_non_gpu_run_time_delta=timedelta.max,
        )


class TestUser:
    q_max = timedelta.max
    q_value = timedelta(10)
    q_zero = timedelta()

    @pytest.mark.parametrize(
        "quota",
        [
            # max + non-zero
            AggregatedRunTime(
                total_gpu_run_time_delta=q_max, total_non_gpu_run_time_delta=q_value
            ),
            AggregatedRunTime(
                total_gpu_run_time_delta=q_value, total_non_gpu_run_time_delta=q_max
            ),
            AggregatedRunTime(
                total_gpu_run_time_delta=q_value, total_non_gpu_run_time_delta=q_value
            ),
            # max + zero
            AggregatedRunTime(
                total_gpu_run_time_delta=q_max, total_non_gpu_run_time_delta=q_zero
            ),
            AggregatedRunTime(
                total_gpu_run_time_delta=q_zero, total_non_gpu_run_time_delta=q_max
            ),
            AggregatedRunTime(
                total_gpu_run_time_delta=q_zero, total_non_gpu_run_time_delta=q_zero
            ),
            # zero + non-zero
            AggregatedRunTime(
                total_gpu_run_time_delta=q_value, total_non_gpu_run_time_delta=q_zero
            ),
            AggregatedRunTime(
                total_gpu_run_time_delta=q_zero, total_non_gpu_run_time_delta=q_value
            ),
        ],
    )
    def test_user_has_quota_true(self, quota: Quota) -> None:
        user = User(name="name", token="token", quota=quota)
        assert user.has_quota()

    def test_user_has_quota_false(self) -> None:
        quota = AggregatedRunTime(
            total_gpu_run_time_delta=self.q_max, total_non_gpu_run_time_delta=self.q_max
        )
        user = User(name="name", token="token", quota=quota)
        assert not user.has_quota()
