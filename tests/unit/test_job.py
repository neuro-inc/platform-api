import dataclasses
from datetime import datetime, timedelta, timezone
from pathlib import PurePath
from unittest import mock

import pytest
from yarl import URL

from platform_api.config import RegistryConfig, StorageConfig
from platform_api.handlers.job_request_builder import ContainerBuilder
from platform_api.handlers.models_handler import ModelRequest
from platform_api.orchestrator.job import Job, JobStatusHistory, JobStatusItem
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


class TestContainer:
    def test_command_list_empty(self):
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
        )
        assert container.command_list == []

    def test_command_list(self):
        container = Container(
            image="testimage",
            command="bash -c date",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        assert container.command_list == ["bash", "-c", "date"]

    def test_belongs_to_registry_no_host(self):
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
        )
        registry_config = RegistryConfig(host="example.com")
        assert not container.belongs_to_registry(registry_config)

    def test_belongs_to_registry_different_host(self):
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(host="example.com")
        assert not container.belongs_to_registry(registry_config)

    def test_belongs_to_registry(self):
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(host="example.com")
        assert container.belongs_to_registry(registry_config)

    def test_to_image_uri_failure(self):
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(host="example.com")
        with pytest.raises(AssertionError, match="Unknown registry"):
            container.to_image_uri(registry_config)

    def test_to_image_uri(self):
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(host="example.com")
        uri = container.to_image_uri(registry_config)
        assert uri == URL("image://project/testimage")


class TestContainerVolumeFactory:
    def test_invalid_storage_uri_scheme(self):
        uri = "invalid://path"
        with pytest.raises(ValueError, match="Invalid URI scheme"):
            ContainerVolumeFactory(
                uri, src_mount_path=PurePath("/"), dst_mount_path=PurePath("/")
            )

    @pytest.mark.parametrize("uri", ("storage:///", "storage://"))
    def test_invalid_storage_uri_path(self, uri):
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
    def test_create(self, uri):
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
    def test_create_invalid_path(self, uri):
        with pytest.raises(ValueError, match="Invalid path"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/host"),
                dst_mount_path=PurePath("/container"),
            ).create()

    def test_create_without_extending_dst_mount_path(self):
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

    def test_relative_dst_mount_path(self):
        uri = "storage:///path/to/dir"
        with pytest.raises(ValueError, match="Mount path must be absolute"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/host"),
                dst_mount_path=PurePath("container"),
            )

    def test_dots_dst_mount_path(self):
        uri = "storage:///path/to/dir"
        with pytest.raises(ValueError, match="Invalid path"):
            ContainerVolumeFactory(
                uri,
                src_mount_path=PurePath("/host"),
                dst_mount_path=PurePath("/container/../path"),
            )


class TestContainerBuilder:
    def test_from_payload_build(self):
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))  # type: ignore
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

    def test_from_payload_build_gpu_model(self):
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))  # type: ignore
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

    def test_from_payload_build_with_ssh(self):
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))  # type: ignore
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

    def test_from_payload_build_with_shm_false(self):
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))  # type: ignore
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
    def test_to_container(self):
        storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))  # type: ignore
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
def job_request_payload():
    return {
        "job_id": "testjob",
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
def job_request_payload_with_shm(job_request_payload):
    data = job_request_payload
    data["container"]["resources"]["shm"] = True
    return data


class TestJob:
    @pytest.fixture
    def job_request(self):
        container = Container(
            image="testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
            http_server=ContainerHTTPServer(port=1234),
        )
        return JobRequest(job_id="testjob", container=container)

    @pytest.fixture
    def job_request_with_ssh_and_http(self):
        container = Container(
            image="testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
            http_server=ContainerHTTPServer(port=1234),
            ssh_server=ContainerSSHServer(port=4321),
        )
        return JobRequest(job_id="testjob", container=container)

    @pytest.fixture
    def job_request_with_ssh(self):
        container = Container(
            image="testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
            ssh_server=ContainerSSHServer(port=4321),
        )
        return JobRequest(job_id="testjob", container=container)

    def test_http_url(self, mock_orchestrator, job_request):
        job = Job(orchestrator_config=mock_orchestrator.config, job_request=job_request)
        assert job.http_url == "http://testjob.jobs"

    def test_ssh_url(self, mock_orchestrator, job_request_with_ssh):
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request_with_ssh,
        )
        assert job.ssh_server == "ssh://testjob.ssh:22"

    def test_no_ssh(self, mock_orchestrator, job_request):
        job = Job(orchestrator_config=mock_orchestrator.config, job_request=job_request)
        assert not job.has_ssh_server_exposed
        with pytest.raises(AssertionError):
            assert job.ssh_server

    def test_http_url_and_ssh(self, mock_orchestrator, job_request_with_ssh_and_http):
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request_with_ssh_and_http,
        )
        assert job.http_url == "http://testjob.jobs"
        assert job.ssh_server == "ssh://testjob.ssh:22"

    def test_should_be_deleted_pending(self, mock_orchestrator, job_request):
        job = Job(orchestrator_config=mock_orchestrator.config, job_request=job_request)
        assert not job.finished_at
        assert not job.should_be_deleted

    def test_should_be_deleted_finished(self, mock_orchestrator, job_request):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        job = Job(orchestrator_config=config, job_request=job_request)
        job.status = JobStatus.FAILED
        assert job.finished_at
        assert job.should_be_deleted

    def test_to_primitive(self, mock_orchestrator, job_request):
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=job_request,
            owner="testuser",
        )
        job.status = JobStatus.FAILED
        job.is_deleted = True
        expected_finished_at = job.finished_at.isoformat()
        assert job.to_primitive() == {
            "id": job.id,
            "owner": "testuser",
            "request": mock.ANY,
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
        }

    def test_from_primitive(self, mock_orchestrator, job_request_payload):
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
        assert job.owner == "testuser"

    def test_from_primitive_with_statuses(self, mock_orchestrator, job_request_payload):
        finished_at_str = datetime.now(timezone.utc).isoformat()
        payload = {
            "id": "testjob",
            "request": job_request_payload,
            "status": "succeeded",
            "is_deleted": True,
            "finished_at": finished_at_str,
            "statuses": [{"status": "failed", "transition_time": finished_at_str}],
        }
        job = Job.from_primitive(mock_orchestrator.config, payload)
        assert job.id == "testjob"
        assert job.status == JobStatus.FAILED
        assert job.is_deleted
        assert job.finished_at
        assert job.owner == "compute"

    def test_to_uri(self, mock_orchestrator, job_request) -> None:
        job = Job(mock_orchestrator.config, job_request, owner="testuser")
        assert job.to_uri() == URL(f"job://testuser/{job.id}")

    def test_to_uri_no_owner(self, mock_orchestrator, job_request) -> None:
        job = Job(mock_orchestrator.config, job_request)
        assert job.to_uri() == URL(f"job://compute/{job.id}")


class TestJobRequest:
    def test_to_primitive(self, job_request_payload):
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
        request = JobRequest(job_id="testjob", container=container)
        assert request.to_primitive() == job_request_payload

    def test_to_primitive_with_ssh(self, job_request_payload):
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
        request = JobRequest(job_id="testjob", container=container)
        assert request.to_primitive() == job_request_payload

    def test_from_primitive(self, job_request_payload):
        request = JobRequest.from_primitive(job_request_payload)
        assert request.job_id == "testjob"
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

    def test_from_primitive_with_ssh(self, job_request_payload):
        job_request_payload["container"]["ssh_server"] = {"port": 678}
        request = JobRequest.from_primitive(job_request_payload)
        assert request.job_id == "testjob"
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

    def test_from_primitive_with_shm(self, job_request_payload_with_shm):
        request = JobRequest.from_primitive(job_request_payload_with_shm)
        assert request.job_id == "testjob"
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


class TestContainerHTTPServer:
    def test_from_primitive(self):
        payload = {"port": 1234}
        server = ContainerHTTPServer.from_primitive(payload)
        assert server == ContainerHTTPServer(port=1234)

    def test_from_primitive_health_check_path(self):
        payload = {"port": 1234, "health_check_path": "/path"}
        server = ContainerHTTPServer.from_primitive(payload)
        assert server == ContainerHTTPServer(port=1234, health_check_path="/path")

    def test_to_primitive(self):
        server = ContainerHTTPServer(port=1234)
        assert server.to_primitive() == {"port": 1234, "health_check_path": "/"}

    def test_to_primitive_health_check_path(self):
        server = ContainerHTTPServer(port=1234, health_check_path="/path")
        assert server.to_primitive() == {"port": 1234, "health_check_path": "/path"}


class TestContainerSSHServer:
    def test_from_primitive(self):
        payload = {"port": 1234}
        server = ContainerSSHServer.from_primitive(payload)
        assert server == ContainerSSHServer(port=1234)

    def test_to_primitive(self):
        server = ContainerSSHServer(port=1234)
        assert server.to_primitive() == {"port": 1234}


class TestJobStatusItem:
    def test_from_primitive(self):
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

    def test_to_primitive(self):
        item = JobStatusItem(
            status=JobStatus.SUCCEEDED, transition_time=datetime.now(timezone.utc)
        )
        assert item.to_primitive() == {
            "status": "succeeded",
            "transition_time": item.transition_time.isoformat(),
            "reason": None,
            "description": None,
        }

    def test_eq_defaults(self):
        old_item = JobStatusItem.create(JobStatus.RUNNING)
        new_item = JobStatusItem.create(JobStatus.RUNNING)
        assert old_item == new_item

    def test_eq_different_times(self):
        old_item = JobStatusItem.create(
            JobStatus.RUNNING, transition_time=datetime.now(timezone.utc)
        )
        new_item = JobStatusItem.create(
            JobStatus.RUNNING,
            transition_time=datetime.now(timezone.utc) + timedelta(days=1),
        )
        assert old_item == new_item

    def test_not_eq(self):
        old_item = JobStatusItem.create(JobStatus.RUNNING)
        new_item = JobStatusItem.create(JobStatus.RUNNING, reason="Whatever")
        assert old_item != new_item


class TestJobStatusHistory:
    def test_single_pending(self):
        first_item = JobStatusItem.create(JobStatus.PENDING)
        items = [first_item]
        history = JobStatusHistory(items=items)
        assert history.first == first_item
        assert history.last == first_item
        assert history.current == first_item
        assert history.created_at == first_item.transition_time
        assert history.created_at_str == first_item.transition_time.isoformat()
        assert not history.started_at
        assert not history.started_at_str
        assert not history.is_finished
        assert not history.finished_at
        assert not history.finished_at_str

    def test_single_failed(self):
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

    def test_full_cycle(self):
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

    def test_current_update(self):
        pending_item = JobStatusItem.create(JobStatus.PENDING)
        running_item = JobStatusItem.create(JobStatus.RUNNING)

        items = [pending_item]
        history = JobStatusHistory(items=items)
        assert history.current == pending_item
        assert not history.is_running

        history.current = running_item
        assert history.current == running_item
        assert history.is_running

    def test_current_discard_update(self):
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
