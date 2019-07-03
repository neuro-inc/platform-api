from typing import Any, Dict

import pytest

from platform_api.handlers.validators import (
    create_container_request_validator,
    create_container_response_validator,
)
from platform_api.resource import GPUModel


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
