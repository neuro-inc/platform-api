from pathlib import PurePath

import pytest
from yarl import URL

from platform_api.config import StorageConfig
from platform_api.handlers.jobs_handler import (
    convert_container_volume_to_json,
    convert_job_container_to_json,
)
from platform_api.handlers.models_handler import create_model_response_validator
from platform_api.handlers.validators import create_container_request_validator
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    ContainerVolume,
)


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
            uri=URL(""),
            src_path=PurePath("/"),
            dst_path=PurePath("/var/storage"),
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
