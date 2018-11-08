from pathlib import PurePath

import pytest
from neuro_auth_client import Permission
from neuro_auth_client.client import ClientAccessSubTreeView, ClientSubTreeViewRoot
from yarl import URL

from platform_api.config import RegistryConfig, StorageConfig
from platform_api.handlers.jobs_handler import (
    convert_container_volume_to_json,
    convert_job_container_to_json,
    filter_jobs_with_access_tree,
    infer_permissions_from_container,
)
from platform_api.handlers.models_handler import (
    create_model_request_validator,
    create_model_response_validator,
)
from platform_api.handlers.validators import (
    create_container_request_validator,
    create_container_response_validator,
)
from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    ContainerSSHServer,
    ContainerVolume,
)
from platform_api.resource import GPUModel
from platform_api.user import User


class TestContainerRequestValidator:
    @pytest.fixture
    def payload(self):
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_zero_gpu(self):
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 0},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_negative_gpu(self):
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": -1},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_one_gpu(self):
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 1},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_too_many_gpu(self):
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 130},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_dev_shm(self):
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_ssh(self):
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
            "ssh": {"port": 666},
        }

    def test_allowed_volumes(self, payload):
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload)
        assert result["volumes"][0]["read_only"]
        assert "shm" not in result["resources"]

    def test_allowed_volumes_with_shm(self, payload_with_dev_shm):
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_dev_shm)
        assert result["volumes"][0]["read_only"]
        assert result["resources"]["shm"]

    def test_disallowed_volumes(self, payload):
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match="volumes is not allowed key"):
            validator.check(payload)

    def test_with_zero_gpu(self, payload_with_zero_gpu):
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_zero_gpu)
        assert result["resources"]["gpu"] == 0

    def test_with_ssh(self, payload_with_ssh):
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_ssh)
        assert result["ssh"]
        assert result["ssh"]["port"]
        assert result["ssh"]["port"] == 666

    def test_with_one_gpu(self, payload_with_one_gpu):
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_one_gpu)
        assert result["resources"]["gpu"]
        assert result["resources"]["gpu"] == 1

    def test_with_too_many_gpu(self, payload_with_too_many_gpu):
        validator = create_container_request_validator(allow_volumes=True)
        with pytest.raises(ValueError, match="gpu"):
            validator.check(payload_with_too_many_gpu)

    def test_with_negative_gpu(self, payload_with_negative_gpu):
        validator = create_container_request_validator(allow_volumes=True)
        with pytest.raises(ValueError, match="gpu"):
            validator.check(payload_with_negative_gpu)

    def test_gpu_model_but_no_gpu(self):
        payload = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu_model": "unknown"},
        }
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match="gpu_model is not allowed key"):
            validator.check(payload)

    def test_gpu_model_unknown(self):
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

    def test_gpu_model(self):
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
                "description": "test-job",
                "dataset_storage_uri": "dataset",
                "result_storage_uri": "result",
            }
        )


class TestContainerResponseValidator:
    def test_gpu_model(self):
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
    def test_empty(self):
        validator = create_model_response_validator()
        with pytest.raises(ValueError, match="is required"):
            validator.check({})

    def test_failure(self):
        validator = create_model_response_validator()
        with pytest.raises(ValueError, match="doesn't match any variant"):
            validator.check({"job_id": "testjob", "status": "INVALID"})

    def test_success(self):
        validator = create_model_response_validator()
        assert validator.check(
            {
                "job_id": "testjob",
                "status": "pending",
                "http_url": "http://testjob",
                "description": "test-job",
            }
        )

    def test_success_without_name_label(self):
        validator = create_model_response_validator()
        assert validator.check(
            {"job_id": "testjob", "status": "pending", "http_url": "http://testjob"}
        )


class TestJobContainerToJson:
    @pytest.fixture
    def storage_config(self) -> StorageConfig:
        return StorageConfig(host_mount_path=PurePath("/whatever"))

    def test_minimal(self, storage_config):
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory_mb=16)
        )
        assert convert_job_container_to_json(container, storage_config) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "volumes": [],
        }

    def test_gpu_and_shm_resources(self, storage_config):
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

    def test_with_ssh(self, storage_config):
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

    def test_src_storage_uri_fallback_default(self, storage_config):
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

    def test_src_storage_uri_fallback_root(self, storage_config):
        volume = ContainerVolume(
            uri=URL(""), src_path=PurePath("/"), dst_path=PurePath("/var/storage")
        )
        payload = convert_container_volume_to_json(volume, storage_config)
        assert payload == {
            "src_storage_uri": "storage:",
            "dst_path": "/var/storage",
            "read_only": False,
        }

    def test_src_storage_uri_fallback_custom(self, storage_config):
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


class TestFilterJobsWithAccessTree:
    @pytest.fixture
    def job_factory(self, mock_orchestrator, job_request_factory):
        def _factory(owner):
            return Job(mock_orchestrator.config, job_request_factory(), owner=owner)

        return _factory

    def test_no_access(self, job_factory):
        jobs = [job_factory("testuser")]
        tree = ClientSubTreeViewRoot(
            path="/", sub_tree=ClientAccessSubTreeView(action="deny", children={})
        )
        result = filter_jobs_with_access_tree(jobs, tree)
        assert not result

    def test_full_access(self, job_factory):
        jobs = [job_factory("testuser"), job_factory("anotheruser")]
        tree = ClientSubTreeViewRoot(
            path="/", sub_tree=ClientAccessSubTreeView(action="manage", children={})
        )
        result = filter_jobs_with_access_tree(jobs, tree)
        expected_ids = {job.id for job in jobs}
        result_ids = {job.id for job in result}
        assert result_ids == expected_ids

    def test_mixed_access(self, job_factory):
        testuser_job = job_factory("testuser")
        anotheruser_job1 = job_factory("anotheruser")
        anotheruser_job2 = job_factory("anotheruser")
        someuser_job = job_factory("someuser")
        jobs = [testuser_job, anotheruser_job1, anotheruser_job2, someuser_job]
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            anotheruser_job1.id: ClientAccessSubTreeView(
                                "read", children={}
                            ),
                            anotheruser_job2.id: ClientAccessSubTreeView(
                                "deny", children={}
                            ),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        result = filter_jobs_with_access_tree(jobs, tree)
        expected_ids = {testuser_job.id, anotheruser_job1.id}
        result_ids = {job.id for job in result}
        assert result_ids == expected_ids


class TestInferPermissionsFromContainer:
    def test_no_volumes(self):
        user = User(name="testuser", token="")
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory_mb=16)
        )
        registry_config = RegistryConfig(host="example.com")
        permissions = infer_permissions_from_container(user, container, registry_config)
        assert permissions == [Permission(uri="job://testuser", action="write")]

    def test_volumes(self):
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
        registry_config = RegistryConfig(host="example.com")
        permissions = infer_permissions_from_container(user, container, registry_config)
        assert permissions == [
            Permission(uri="job://testuser", action="write"),
            Permission(uri="storage://testuser/dataset", action="read"),
            Permission(uri="storage://testuser/result", action="write"),
        ]

    def test_image(self):
        user = User(name="testuser", token="")
        container = Container(
            image="example.com/testuser/image",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
        )
        registry_config = RegistryConfig(host="example.com")
        permissions = infer_permissions_from_container(user, container, registry_config)
        assert permissions == [
            Permission(uri="job://testuser", action="write"),
            Permission(uri="image://testuser/image", action="read"),
        ]
