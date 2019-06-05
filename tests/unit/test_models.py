from pathlib import PurePath
from typing import Any, Dict
from unittest import mock

import pytest
from neuro_auth_client import Permission
from neuro_auth_client.client import ClientAccessSubTreeView, ClientSubTreeViewRoot
from yarl import URL

from platform_api.config import RegistryConfig, StorageConfig
from platform_api.handlers.jobs_handler import (
    BulkJobFilter,
    BulkJobFilterBuilder,
    JobFilterException,
    convert_container_volume_to_json,
    convert_job_container_to_json,
    convert_job_to_job_response,
    infer_permissions_from_container,
)
from platform_api.handlers.models_handler import (
    create_model_request_validator,
    create_model_response_validator,
)
from platform_api.handlers.validators import (
    JOB_NAME_MAX_LENGTH,
    USER_NAME_MAX_LENGTH,
    create_container_request_validator,
    create_container_response_validator,
)
from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerSSHServer,
    ContainerVolume,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.resource import GPUModel
from platform_api.user import User

from .conftest import MockOrchestrator


class TestContainerRequestValidator:
    @pytest.fixture
    def payload(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_zero_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 0},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_negative_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": -1},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_one_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 1},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_too_many_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 130},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_dev_shm(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_ssh(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
            "ssh": {"port": 666},
        }

    def test_allowed_volumes(self, payload: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload)
        assert result["volumes"][0]["read_only"]
        assert "shm" not in result["resources"]

    def test_allowed_volumes_with_shm(
        self, payload_with_dev_shm: Dict[str, Any]
    ) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_dev_shm)
        assert result["volumes"][0]["read_only"]
        assert result["resources"]["shm"]

    def test_disallowed_volumes(self, payload: Dict[str, Any]) -> None:
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match="volumes is not allowed key"):
            validator.check(payload)

    def test_with_zero_gpu(self, payload_with_zero_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_zero_gpu)
        assert result["resources"]["gpu"] == 0

    def test_with_ssh(self, payload_with_ssh: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_ssh)
        assert result["ssh"]
        assert result["ssh"]["port"]
        assert result["ssh"]["port"] == 666

    def test_with_one_gpu(self, payload_with_one_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_one_gpu)
        assert result["resources"]["gpu"]
        assert result["resources"]["gpu"] == 1

    def test_with_too_many_gpu(self, payload_with_too_many_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        with pytest.raises(ValueError, match="gpu"):
            validator.check(payload_with_too_many_gpu)

    def test_with_negative_gpu(self, payload_with_negative_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        with pytest.raises(ValueError, match="gpu"):
            validator.check(payload_with_negative_gpu)

    def test_gpu_model_but_no_gpu(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu_model": "unknown"},
        }
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match="gpu_model is not allowed key"):
            validator.check(payload)

    def test_gpu_model_unknown(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "gpu": 1,
                "gpu_model": "unknown",
            },
        }
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match="value doesn't match any variant"):
            validator.check(payload)

    def test_gpu_model(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "gpu": 1,
                "gpu_model": "unknown",
            },
        }
        validator = create_container_request_validator(
            allowed_gpu_models=[GPUModel(id="unknown")]
        )
        result = validator.check(payload)
        assert result["resources"]["gpu"] == 1
        assert result["resources"]["gpu_model"] == "unknown"


class TestModelRequestValidator:
    def test_model_without_name(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "ssh": {"port": 666},
        }
        validator = create_model_request_validator(allowed_gpu_models=[])
        assert validator.check(
            {
                "container": container,
                "dataset_storage_uri": "dataset",
                "result_storage_uri": "result",
            }
        )

    def test_model_with_name(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "ssh": {"port": 666},
        }
        validator = create_model_request_validator(allowed_gpu_models=[])
        assert validator.check(
            {
                "container": container,
                "name": "test-job-name",
                "description": "test-job",
                "dataset_storage_uri": "dataset",
                "result_storage_uri": "result",
            }
        )


class TestContainerResponseValidator:
    def test_gpu_model(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "gpu": 1,
                "gpu_model": "unknown",
            },
        }
        validator = create_container_response_validator()
        result = validator.check(payload)
        assert result["resources"]["gpu"] == 1
        assert result["resources"]["gpu_model"] == "unknown"


class TestModelResponseValidator:
    def test_empty(self) -> None:
        validator = create_model_response_validator()
        with pytest.raises(ValueError, match="is required"):
            validator.check({})

    def test_failure(self) -> None:
        validator = create_model_response_validator()
        with pytest.raises(ValueError, match="doesn't match any variant"):
            validator.check({"job_id": "testjob", "status": "INVALID"})

    def test_success(self) -> None:
        validator = create_model_response_validator()
        assert validator.check(
            {
                "job_id": "testjob",
                "status": "pending",
                "http_url": "http://testjob",
                "http_url_named": "http://test-job-owner",
                "name": "test-job-name",
                "description": "test-job",
                "is_preemptible": False,
            }
        )

    def test_success_without_name_label(self) -> None:
        validator = create_model_response_validator()
        assert validator.check(
            {
                "job_id": "testjob",
                "status": "pending",
                "http_url": "http://testjob",
                "is_preemptible": False,
            }
        )


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


@pytest.mark.asyncio
async def test_job_to_job_response(mock_orchestrator: MockOrchestrator) -> None:
    job = Job(
        storage_config=mock_orchestrator.storage_config,
        orchestrator_config=mock_orchestrator.config,
        job_request=JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
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
    mock_orchestrator: MockOrchestrator
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
    mock_orchestrator: MockOrchestrator
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
