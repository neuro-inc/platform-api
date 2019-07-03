import dataclasses
from datetime import datetime, timedelta, timezone
from pathlib import PurePath
from typing import Any, Dict
from unittest import mock

import pytest
from multidict import MultiDict
from neuro_auth_client import Permission
from neuro_auth_client.client import (
    ClientAccessSubTreeView,
    ClientSubTreeViewRoot,
    Quota,
)
from trafaret import DataError
from yarl import URL

from platform_api.config import RegistryConfig, StorageConfig
from platform_api.handlers.job_request_builder import ContainerBuilder
from platform_api.handlers.jobs_handler import (
    BulkJobFilter,
    BulkJobFilterBuilder,
    JobFilterException,
    JobFilterFactory,
    convert_container_volume_to_json,
    convert_job_container_to_json,
    convert_job_to_job_response,
    infer_permissions_from_container,
)
from platform_api.handlers.validators import JOB_NAME_MAX_LENGTH, USER_NAME_MAX_LENGTH
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
    ContainerVolume,
    ContainerVolumeFactory,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.user import User

from .conftest import MockOrchestrator


class TestContainer:
    def test_command_list_empty(self) -> None:
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
        )
        assert container.command_list == []

    def test_command_list(self) -> None:
        container = Container(
            image="testimage",
            command="bash -c date",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        assert container.command_list == ["bash", "-c", "date"]

    def test_belongs_to_registry_no_host(self) -> None:
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        assert not container.belongs_to_registry(registry_config)

    def test_belongs_to_registry_different_host(self) -> None:
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        assert not container.belongs_to_registry(registry_config)

    def test_belongs_to_registry(self) -> None:
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        assert container.belongs_to_registry(registry_config)

    def test_to_image_uri_failure(self) -> None:
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        with pytest.raises(AssertionError, match="Unknown registry"):
            container.to_image_uri(registry_config)

    def test_to_image_uri(self) -> None:
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        uri = container.to_image_uri(registry_config)
        assert uri == URL("image://project/testimage")

    def test_to_image_uri_registry_with_custom_port(self) -> None:
        container = Container(
            image="example.com:5000/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(url=URL("http://example.com:5000"))
        uri = container.to_image_uri(registry_config)
        assert uri == URL("image://project/testimage")

    def test_to_image_uri_ignore_tag(self) -> None:
        container = Container(
            image="example.com/project/testimage:latest",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        uri = container.to_image_uri(registry_config)
        assert uri == URL("image://project/testimage")


class TestContainerVolumeFactory:
    def test_invalid_storage_uri_scheme(self) -> None:
        uri = "invalid://path"
        with pytest.raises(ValueError, match="Invalid URI scheme"):
            ContainerVolumeFactory(
                uri, src_mount_path=PurePath("/"), dst_mount_path=PurePath("/")
            )

    @pytest.mark.parametrize("uri", ("storage:///", "storage://"))
    def test_invalid_storage_uri_path(self, uri: str) -> None:
        volume = ContainerVolumeFactory(
            uri, src_mount_path=PurePath("/host"), dst_mount_path=PurePath("/container")
        ).create()
        assert volume.src_path == PurePath("/host")
        assert volume.dst_path == PurePath("/container")
        assert not volume.read_only

    @pytest.mark.parametrize(
        "uri",
        (
            "storage:///path/to/dir",
            "storage:///path/to//dir",
            "storage:///path/to/./dir",
            "storage://path/to/dir",
        ),
    )
    def test_create(self, uri: str) -> None:
        volume = ContainerVolume.create(
            uri,
            src_mount_path=PurePath("/host"),
            dst_mount_path=PurePath("/container"),
            read_only=True,
        )
        assert volume.src_path == PurePath("/host/path/to/dir")
        assert volume.dst_path == PurePath("/container/path/to/dir")
        assert volume.read_only

    @pytest.mark.parametrize("uri", ("storage:///../to/dir", "storage://path/../dir"))
    def test_create_invalid_path(self, uri: str) -> None:
        with pytest.raises(ValueError, match="Invalid path"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/host"),
                dst_mount_path=PurePath("/container"),
            ).create()

    def test_create_without_extending_dst_mount_path(self) -> None:
        uri = "storage:///path/to/dir"
        volume = ContainerVolume.create(
            uri,
            src_mount_path=PurePath("/host"),
            dst_mount_path=PurePath("/container"),
            read_only=True,
            extend_dst_mount_path=False,
        )
        assert volume.src_path == PurePath("/host/path/to/dir")
        assert volume.dst_path == PurePath("/container")
        assert volume.read_only

    def test_relative_dst_mount_path(self) -> None:
        uri = "storage:///path/to/dir"
        with pytest.raises(ValueError, match="Mount path must be absolute"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/host"),
                dst_mount_path=PurePath("container"),
            )

    def test_dots_dst_mount_path(self) -> None:
        uri = "storage:///path/to/dir"
        with pytest.raises(ValueError, match="Invalid path"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/host"),
                dst_mount_path=PurePath("/container/../path"),
            )


class TestContainerBuilder:
    def test_from_payload_build(self) -> None:
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
        payload = {
            "image": "testimage",
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
        container = ContainerBuilder.from_container_payload(
            payload, storage_config=storage_config
        ).build()
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
        container = ContainerBuilder.from_container_payload(
            payload, storage_config=storage_config
        ).build()
        assert container == Container(
            image="testimage",
            resources=ContainerResources(
                cpu=0.1, memory_mb=128, gpu=1, gpu_model_id="gpumodel"
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
        container = ContainerBuilder.from_container_payload(
            payload, storage_config=storage_config
        ).build()
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
        container = ContainerBuilder.from_container_payload(
            payload, storage_config=storage_config
        ).build()
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
            "resources": {
                "cpu": 1,
                "memory_mb": 128,
                "gpu": None,
                "gpu_model_id": None,
                "shm": None,
            },
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
        record = JobRecord.create(request=job_request)
        assert not record.finished_at
        assert not record.should_be_deleted(delay=timedelta(60))

    def test_should_be_deleted_finished(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        record = JobRecord.create(status=JobStatus.FAILED, request=job_request)
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
            job_request=job_request,
        )
        assert job.http_host == "testjob.jobs"

    def test_http_host_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            name="test-job-name",
            owner="owner",
        )
        assert job.http_host == "testjob.jobs"
        assert job.http_host_named == "test-job-name--owner.jobs"

    def test_job_name(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            name="test-job-name-123",
        )
        assert job.name == "test-job-name-123"

    def test_job_has_gpu_false(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
        )
        assert not job.has_gpu

    def test_job_has_gpu_true(
        self, mock_orchestrator: MockOrchestrator, job_request_with_gpu: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request_with_gpu,
        )
        assert job.has_gpu

    def test_job_get_run_time_active_job(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        def mocked_datetime_factory() -> datetime:
            return datetime(year=2099, month=1, day=1)

        started_at = datetime(year=2019, month=1, day=1)
        first_item = JobStatusItem.create(JobStatus.PENDING, transition_time=started_at)
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            status_history=JobStatusHistory(items=[first_item]),
            current_datetime_factory=mocked_datetime_factory,
        )
        expected_timedelta = mocked_datetime_factory() - started_at
        assert job.get_run_time() == expected_timedelta

    def test_job_get_run_time_terminated_job(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        started_at = datetime(year=2019, month=3, day=1)
        finished_at = datetime(year=2019, month=3, day=2)
        first_item = JobStatusItem.create(JobStatus.PENDING, transition_time=started_at)
        last_item = JobStatusItem.create(JobStatus.FAILED, transition_time=finished_at)
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            status_history=JobStatusHistory(items=[first_item, last_item]),
        )
        expected_timedelta = finished_at - started_at
        assert job.get_run_time() == expected_timedelta

    def test_http_url(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
        )
        assert job.http_url == "http://testjob.jobs"

    def test_http_urls_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            name="test-job-name",
            owner="owner",
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
            job_request=job_request,
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
            job_request=job_request,
            name="test-job-name",
            owner="owner",
        )
        assert job.http_url == "https://testjob.jobs"
        assert job.http_url_named == "https://test-job-name--owner.jobs"

    def test_ssh_url(
        self, mock_orchestrator: MockOrchestrator, job_request_with_ssh: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request_with_ssh,
        )
        assert job.ssh_server == "ssh://nobody@ssh-auth:22"

    def test_no_ssh(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
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
            job_request=job_request_with_ssh_and_http,
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
            job_request=job_request_with_ssh_and_http,
            name="test-job-name",
            owner="owner",
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
            job_request=job_request,
            owner="testuser",
            name="test-job-name",
            is_preemptible=True,
        )
        job.status = JobStatus.FAILED
        job.is_deleted = True
        assert job.finished_at
        expected_finished_at = job.finished_at.isoformat()
        assert job.to_primitive() == {
            "id": job.id,
            "name": "test-job-name",
            "owner": "testuser",
            "cluster_name": "",
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

    def test_to_uri(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.storage_config,
            mock_orchestrator.config,
            job_request,
            owner="testuser",
        )
        assert job.to_uri() == URL(f"job://testuser/{job.id}")

    def test_to_uri_orphaned(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.storage_config, mock_orchestrator.config, job_request
        )
        assert job.to_uri() == URL(f"job://compute/{job.id}")

    def test_to_uri_no_owner(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            mock_orchestrator.storage_config,
            mock_orchestrator.config,
            job_request,
            orphaned_job_owner="",
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


class TestJobContainerToJson:
    @pytest.fixture
    def storage_config(self) -> StorageConfig:
        return StorageConfig(host_mount_path=PurePath("/whatever"))

    def test_minimal(self, storage_config: StorageConfig) -> None:
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory_mb=16)
        )
        assert convert_job_container_to_json(container, storage_config) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "volumes": [],
        }

    def test_gpu_and_shm_resources(self, storage_config: StorageConfig) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory_mb=16, gpu=1, shm=True),
        )
        assert convert_job_container_to_json(container, storage_config) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 1, "shm": True},
            "volumes": [],
        }

    def test_with_ssh(self, storage_config: StorageConfig) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory_mb=16, gpu=1, shm=True),
            ssh_server=ContainerSSHServer(port=777),
        )
        assert convert_job_container_to_json(container, storage_config) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 1, "shm": True},
            "volumes": [],
            "ssh": {"port": 777},
        }

    def test_src_storage_uri_fallback_default(
        self, storage_config: StorageConfig
    ) -> None:
        volume = ContainerVolume(
            uri=URL(""),
            src_path=PurePath("/"),
            dst_path=PurePath("/var/storage/username/dataset"),
        )
        payload = convert_container_volume_to_json(volume, storage_config)
        assert payload == {
            "src_storage_uri": "storage://username/dataset",
            "dst_path": "/var/storage/username/dataset",
            "read_only": False,
        }

    def test_src_storage_uri_fallback_root(self, storage_config: StorageConfig) -> None:
        volume = ContainerVolume(
            uri=URL(""), src_path=PurePath("/"), dst_path=PurePath("/var/storage")
        )
        payload = convert_container_volume_to_json(volume, storage_config)
        assert payload == {
            "src_storage_uri": "storage:",
            "dst_path": "/var/storage",
            "read_only": False,
        }

    def test_src_storage_uri_fallback_custom(
        self, storage_config: StorageConfig
    ) -> None:
        volume = ContainerVolume(
            uri=URL(""),
            src_path=PurePath("/"),
            dst_path=PurePath("/var/custom/username/dataset"),
        )
        payload = convert_container_volume_to_json(volume, storage_config)
        assert payload == {
            "src_storage_uri": "storage:",
            "dst_path": "/var/custom/username/dataset",
            "read_only": False,
        }


class TestJobFilterFactory:
    def test_create_from_query(self) -> None:
        factory = JobFilterFactory().create_from_query

        query: Any = MultiDict()
        assert factory(query) == JobFilter()

        query = MultiDict([("name", "test-job")])
        assert factory(query) == JobFilter(name="test-job")

        query = MultiDict([("name", "test-job"), ("name", "other-job")])
        assert factory(query) == JobFilter(name="test-job")

        query = MultiDict([("owner", "alice"), ("owner", "bob")])
        assert factory(query) == JobFilter(owners={"bob", "alice"})

        query = MultiDict([("name", "test-job"), ("owner", "alice"), ("owner", "bob")])
        assert factory(query) == JobFilter(owners={"bob", "alice"}, name="test-job")

    def test_create_from_query_with_status(self) -> None:
        factory = JobFilterFactory().create_from_query

        query: Any = MultiDict(
            [
                ("name", "test-job"),
                ("status", "running"),
                ("status", "pending"),
                ("status", "failed"),
                ("status", "succeeded"),
            ]
        )
        assert factory(query) == JobFilter(
            statuses={
                JobStatus.FAILED,
                JobStatus.PENDING,
                JobStatus.SUCCEEDED,
                JobStatus.RUNNING,
            },
            name="test-job",
        )

        query = MultiDict(
            [
                ("name", "test-job"),
                ("owner", "alice"),
                ("owner", "bob"),
                ("status", "failed"),
                ("status", "succeeded"),
            ]
        )
        assert factory(query) == JobFilter(
            statuses={JobStatus.FAILED, JobStatus.SUCCEEDED},
            owners={"bob", "alice"},
            name="test-job",
        )

    def test_create_from_query_by_hostname(self) -> None:
        factory = JobFilterFactory().create_from_query

        query: Any = MultiDict([("hostname", "test-job--john-doe.example.org")])
        assert factory(query) == JobFilter(name="test-job", owners={"john-doe"})

        query = MultiDict([("hostname", "test-job-id.example.org")])
        assert factory(query) == JobFilter(ids={"test-job-id"})

        query = MultiDict(
            [
                ("hostname", "test-job--john-doe.example.org"),
                ("hostname", "test-job-id.example.org"),
            ]
        )
        assert factory(query) == JobFilter(name="test-job", owners={"john-doe"})

        query = MultiDict(
            [
                ("hostname", "test-job-id.example.org"),
                ("hostname", "test-job--john-doe.example.org"),
            ]
        )
        assert factory(query) == JobFilter(name=None, ids={"test-job-id"})

    def test_create_from_query_by_hostname_with_status(self) -> None:
        factory = JobFilterFactory().create_from_query

        query: Any = MultiDict(
            [
                ("hostname", "test-job--john-doe.example.org"),
                ("status", "failed"),
                ("status", "succeeded"),
            ]
        )
        assert factory(query) == JobFilter(
            statuses={JobStatus.FAILED, JobStatus.SUCCEEDED},
            name="test-job",
            owners={"john-doe"},
        )

        query = MultiDict(
            [
                ("hostname", "test-job-id.example.org"),
                ("status", "failed"),
                ("status", "succeeded"),
            ]
        )
        assert factory(query) == JobFilter(
            statuses={JobStatus.FAILED, JobStatus.SUCCEEDED}, ids={"test-job-id"}
        )

    @pytest.mark.parametrize(
        "query",
        [
            [("name", "jt")],
            [("name", "test_job")],
            [("name", "test.job")],
            [("name", "0testjob")],
            [("owner", "jd")],
            [("owner", "john.doe")],
            [("owner", "john_doe")],
            [("owner", "john--doe")],
            [("status", "unknown")],
            [("hostname", "testjob--.example.org")],
            [("hostname", "--johndoe.example.org")],
            [("hostname", "testjob--john_doe.example.org")],
            [("hostname", "test_job--johndoe.example.org")],
            [("hostname", "xn--johndoe.example.org")],
            [("hostname", "testjob--johndoe.example.org"), ("name", "testjob")],
            [("hostname", "testjob--johndoe.example.org"), ("owner", "johndoe")],
            [("hostname", "TESTJOB--johndoe.example.org")],
            [("hostname", "testjob--JOHNDOE.example.org")],
        ],
    )
    def test_create_from_query_fail(self, query: Any) -> None:
        factory = JobFilterFactory().create_from_query
        with pytest.raises((ValueError, DataError)):
            factory(MultiDict(query))  # type: ignore # noqa


class TestBulkJobFilterBuilder:
    def test_no_access(self) -> None:
        query_filter = JobFilter()
        tree = ClientSubTreeViewRoot(
            path="/", sub_tree=ClientAccessSubTreeView(action="deny", children={})
        )
        with pytest.raises(JobFilterException, match="no jobs"):
            BulkJobFilterBuilder(query_filter, tree).build()

    def test_no_access_with_owners(self) -> None:
        query_filter = JobFilter(owners={"someuser"})
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        with pytest.raises(JobFilterException, match="no jobs"):
            BulkJobFilterBuilder(query_filter, tree).build()

    def test_full_access_no_owners(self) -> None:
        query_filter = JobFilter()
        tree = ClientSubTreeViewRoot(
            path="/", sub_tree=ClientAccessSubTreeView(action="manage", children={})
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(), shared_ids=set(), shared_ids_filter=None
        )

    def test_full_access_with_owners(self) -> None:
        query_filter = JobFilter(owners={"testuser"})
        tree = ClientSubTreeViewRoot(
            path="/", sub_tree=ClientAccessSubTreeView(action="manage", children={})
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(owners={"testuser"}),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_access_no_owners(self) -> None:
        query_filter = JobFilter()
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(owners={"testuser"}),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(),
        )

    def test_mixed_access_owners_shared_all(self) -> None:
        query_filter = JobFilter(owners={"testuser"})
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(owners={"testuser"}),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_access_shared_ids_only(self) -> None:
        query_filter = JobFilter(owners={"anotheruser"})
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=None,
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(owners={"anotheruser"}),
        )

    def test_mixed_access_owners_shared_all_and_specific(self) -> None:
        query_filter = JobFilter(
            owners={"testuser", "anotheruser"},
            statuses={JobStatus.PENDING},
            name="testname",
        )
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                owners={"testuser"}, statuses={JobStatus.PENDING}, name="testname"
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(
                owners={"testuser", "anotheruser"},
                statuses={JobStatus.PENDING},
                name="testname",
            ),
        )


class TestInferPermissionsFromContainer:
    def test_no_volumes(self) -> None:
        user = User(name="testuser", token="")
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory_mb=16)
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        permissions = infer_permissions_from_container(user, container, registry_config)
        assert permissions == [Permission(uri="job://testuser", action="write")]

    def test_volumes(self) -> None:
        user = User(name="testuser", token="")
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://testuser/dataset"),
                    src_path=PurePath("/"),
                    dst_path=PurePath("/var/storage/testuser/dataset"),
                    read_only=True,
                ),
                ContainerVolume(
                    uri=URL("storage://testuser/result"),
                    src_path=PurePath("/"),
                    dst_path=PurePath("/var/storage/testuser/result"),
                ),
            ],
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        permissions = infer_permissions_from_container(user, container, registry_config)
        assert permissions == [
            Permission(uri="job://testuser", action="write"),
            Permission(uri="storage://testuser/dataset", action="read"),
            Permission(uri="storage://testuser/result", action="write"),
        ]

    def test_image(self) -> None:
        user = User(name="testuser", token="")
        container = Container(
            image="example.com/testuser/image",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        permissions = infer_permissions_from_container(user, container, registry_config)
        assert permissions == [
            Permission(uri="job://testuser", action="write"),
            Permission(uri="image://testuser/image", action="read"),
        ]


class TestConvertJobToResponse:
    @pytest.mark.asyncio
    async def test_job_to_job_response(
        self, mock_orchestrator: MockOrchestrator
    ) -> None:
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory_mb=128),
                ),
                description="test test description",
            ),
            name="test-job-name",
        )
        response = convert_job_to_job_response(job)
        assert response == {
            "id": job.id,
            "owner": "compute",
            "status": "pending",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": mock.ANY,
            },
            "container": {
                "image": "testimage",
                "env": {},
                "volumes": [],
                "resources": {"cpu": 1, "memory_mb": 128},
            },
            "name": "test-job-name",
            "description": "test test description",
            "ssh_server": "ssh://nobody@ssh-auth:22",
            "ssh_auth_server": "ssh://nobody@ssh-auth:22",
            "is_preemptible": False,
        }

    @pytest.mark.asyncio
    async def test_job_to_job_response_with_job_name_and_http_exposed(
        self, mock_orchestrator: MockOrchestrator
    ) -> None:
        owner_name = "a" * USER_NAME_MAX_LENGTH
        job_name = "b" * JOB_NAME_MAX_LENGTH
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory_mb=128),
                    http_server=ContainerHTTPServer(port=80),
                )
            ),
            owner=owner_name,
            name=job_name,
        )
        response = convert_job_to_job_response(job)
        assert response == {
            "id": job.id,
            "owner": owner_name,
            "name": job_name,
            "http_url": f"http://{job.id}.jobs",
            "http_url_named": f"http://{job_name}--{owner_name}.jobs",
            "status": "pending",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": mock.ANY,
            },
            "container": {
                "image": "testimage",
                "env": {},
                "volumes": [],
                "resources": {"cpu": 1, "memory_mb": 128},
                "http": {"port": 80, "health_check_path": "/", "requires_auth": False},
            },
            "ssh_server": "ssh://nobody@ssh-auth:22",
            "ssh_auth_server": "ssh://nobody@ssh-auth:22",
            "is_preemptible": False,
        }

    @pytest.mark.asyncio
    async def test_job_to_job_response_with_job_name_and_http_exposed_too_long_name(
        self, mock_orchestrator: MockOrchestrator
    ) -> None:
        owner_name = "a" * USER_NAME_MAX_LENGTH
        job_name = "b" * (JOB_NAME_MAX_LENGTH + 1)
        job = Job(
            storage_config=mock_orchestrator.storage_config,
            orchestrator_config=mock_orchestrator.config,
            job_request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory_mb=128),
                    http_server=ContainerHTTPServer(port=80),
                )
            ),
            owner=owner_name,
            name=job_name,
        )
        response = convert_job_to_job_response(job)
        assert response == {
            "id": job.id,
            "owner": owner_name,
            "name": job_name,
            "http_url": f"http://{job.id}.jobs",
            # NOTE: field `http_url_named` is cut off when it is invalid
            "status": "pending",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": mock.ANY,
            },
            "container": {
                "image": "testimage",
                "env": {},
                "volumes": [],
                "resources": {"cpu": 1, "memory_mb": 128},
                "http": {"port": 80, "health_check_path": "/", "requires_auth": False},
            },
            "ssh_server": "ssh://nobody@ssh-auth:22",
            "ssh_auth_server": "ssh://nobody@ssh-auth:22",
            "is_preemptible": False,
        }
