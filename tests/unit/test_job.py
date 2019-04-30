import dataclasses
from datetime import datetime, timedelta, timezone
from pathlib import PurePath
from typing import Any, Dict
from unittest import mock

import pytest
from neuro_auth_client.client import Quota
from yarl import URL

from platform_api.config import StorageConfig
from platform_api.handlers.job_request_builder import ContainerBuilder
from platform_api.handlers.models_handler import ModelRequest
from platform_api.orchestrator.job import (
    AggregatedRunTime,
    Job,
    JobStatusHistory,
    JobStatusItem,
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerVolumeFactory,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.kube_client import (
    ContainerHTTPServer,
    ContainerResources,
    ContainerSSHServer,
    ContainerVolume,
)
from platform_api.user import User

from .conftest import MockOrchestrator


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


class TestModelRequest:
    def test_to_container(self) -> None:
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
        payload = {
            "container": {
                "image": "testimage",
                "command": "testcommand",
                "env": {"TESTVAR": "testvalue"},
                "resources": {"cpu": 0.1, "memory_mb": 128, "gpu": 1},
                "http": {"port": 80},
            },
            "dataset_storage_uri": "storage://path/to/dir",
            "result_storage_uri": "storage://path/to/another/dir",
        }
        request = ModelRequest(payload, storage_config=storage_config, env_prefix="NP")
        assert request.to_container() == Container(
            image="testimage",
            command="testcommand",
            env={
                "TESTVAR": "testvalue",
                "NP_DATASET_PATH": "/var/storage/path/to/dir",
                "NP_RESULT_PATH": "/var/storage/path/to/another/dir",
            },
            volumes=[
                ContainerVolume(
                    uri=URL("storage://path/to/dir"),
                    src_path=PurePath("/tmp/path/to/dir"),
                    dst_path=PurePath("/var/storage/path/to/dir"),
                    read_only=True,
                ),
                ContainerVolume(
                    uri=URL("storage://path/to/another/dir"),
                    src_path=PurePath("/tmp/path/to/another/dir"),
                    dst_path=PurePath("/var/storage/path/to/another/dir"),
                    read_only=False,
                ),
            ],
            resources=ContainerResources(cpu=0.1, memory_mb=128, gpu=1),
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


class TestJob:
    @pytest.fixture
    def job_request(self) -> JobRequest:
        container = Container(
            image="testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
            http_server=ContainerHTTPServer(port=1234),
        )
        return JobRequest(
            job_id="testjob",
            container=container,
            description="Description of the testjob",
        )

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
        job = Job(orchestrator_config=mock_orchestrator.config, job_request=job_request)
        assert job.http_host == "testjob.jobs"

    def test_http_host_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            name="test-job-name",
            owner="owner",
        )
        assert job.http_host == "testjob.jobs"
        assert job.http_host_named == "test-job-name-owner.jobs"

    def test_job_name(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            name="test-job-name-123",
        )
        assert job.name == "test-job-name-123"

    def test_job_has_gpu_false(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(orchestrator_config=mock_orchestrator.config, job_request=job_request)
        assert not job.has_gpu

    def test_job_has_gpu_true(
        self, mock_orchestrator: MockOrchestrator, job_request_with_gpu: JobRequest
    ) -> None:
        job = Job(
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
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            status_history=JobStatusHistory(items=[first_item, last_item]),
        )
        expected_timedelta = finished_at - started_at
        assert job.get_run_time() == expected_timedelta

    def test_http_url(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(orchestrator_config=mock_orchestrator.config, job_request=job_request)
        assert job.http_url == "http://testjob.jobs"

    def test_http_urls_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            name="test-job-name",
            owner="owner",
        )
        assert job.http_url == "http://testjob.jobs"
        assert job.http_url_named == "http://test-job-name-owner.jobs"

    def test_https_url(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        config = dataclasses.replace(
            mock_orchestrator.config, is_http_ingress_secure=True
        )
        job = Job(orchestrator_config=config, job_request=job_request)
        assert job.http_url == "https://testjob.jobs"

    def test_https_urls_named(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        config = dataclasses.replace(
            mock_orchestrator.config, is_http_ingress_secure=True
        )
        job = Job(
            orchestrator_config=config,
            job_request=job_request,
            name="test-job-name",
            owner="owner",
        )
        assert job.http_url == "https://testjob.jobs"
        assert job.http_url_named == "https://test-job-name-owner.jobs"

    def test_ssh_url(
        self, mock_orchestrator: MockOrchestrator, job_request_with_ssh: JobRequest
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request_with_ssh,
        )
        assert job.ssh_server == "ssh://testjob.ssh:22"

    def test_no_ssh(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(orchestrator_config=mock_orchestrator.config, job_request=job_request)
        assert not job.has_ssh_server_exposed
        with pytest.raises(AssertionError):
            assert job.ssh_server

    def test_http_url_and_ssh(
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_with_ssh_and_http: JobRequest,
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request_with_ssh_and_http,
        )
        assert job.http_url == "http://testjob.jobs"
        assert job.ssh_server == "ssh://testjob.ssh:22"

    def test_http_url_and_ssh_named(
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_with_ssh_and_http: JobRequest,
    ) -> None:
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request_with_ssh_and_http,
            name="test-job-name",
            owner="owner",
        )
        assert job.http_url == "http://testjob.jobs"
        assert job.http_url_named == "http://test-job-name-owner.jobs"
        assert job.ssh_server == "ssh://testjob.ssh:22"

    def test_should_be_deleted_pending(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(orchestrator_config=mock_orchestrator.config, job_request=job_request)
        assert not job.finished_at
        assert not job.should_be_deleted

    def test_should_be_deleted_finished(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        job = Job(orchestrator_config=config, job_request=job_request)
        job.status = JobStatus.FAILED
        assert job.finished_at
        assert job.should_be_deleted

    def test_to_primitive(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(
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
        job = Job.from_primitive(mock_orchestrator.config, payload)
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
        job = Job.from_primitive(mock_orchestrator.config, payload)
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
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.id == "testjob"
        assert job.status == JobStatus.FAILED
        assert job.is_deleted
        assert job.finished_at
        assert job.description == "Description of the testjob"
        assert job.owner == "compute"
        assert job.is_preemptible

    def test_to_uri(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(mock_orchestrator.config, job_request, owner="testuser")
        assert job.to_uri() == URL(f"job://testuser/{job.id}")

    def test_to_uri_orphaned(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        job = Job(mock_orchestrator.config, job_request)
        assert job.to_uri() == URL(f"job://compute/{job.id}")

    def test_to_uri_no_owner(
        self, mock_orchestrator: MockOrchestrator, job_request: JobRequest
    ) -> None:
        config = dataclasses.replace(mock_orchestrator.config, orphaned_job_owner="")
        job = Job(config, job_request)
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
            "status": current_status_item["status"],
            "statuses": [current_status_item],
            "is_deleted": "False",
            "finished_at": finished_at_str,
            "is_preemptible": False,
        }
        actual = Job.to_primitive(
            Job.from_primitive(mock_orchestrator.config, expected)
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
        }
        item = JobStatusItem.from_primitive(payload)
        assert item.status == JobStatus.SUCCEEDED
        assert item.is_finished
        assert item.transition_time == transition_time
        assert item.reason == "test reason"
        assert item.description == "test description"

    def test_to_primitive(self) -> None:
        item = JobStatusItem(
            status=JobStatus.SUCCEEDED, transition_time=datetime.now(timezone.utc)
        )
        assert item.to_primitive() == {
            "status": "succeeded",
            "transition_time": item.transition_time.isoformat(),
            "reason": None,
            "description": None,
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
