import pytest

from platform_api.handlers.jobs_handler import (
    create_job_request_validator,
    create_job_response_validator,
)
from platform_api.handlers.validators import validate_job_name


@pytest.mark.parametrize(
    "fail_value,description",
    [
        ("", "should be at least 2 letters"),
        ("a", "should be at least 2 letters"),
        ("-abc", "should not start with dash"),
        ("_abc", "should not start with underscore"),
        (".abc", "should not start with dot"),
        ("A" * 257, "should be at most 256 letters"),
    ],
)
def test_validate_job_name__fail(fail_value: str, description: str):
    with pytest.raises(ValueError, match=f"Invalid job name '{fail_value}'"):
        assert validate_job_name(fail_value), description


@pytest.mark.parametrize(
    "ok_value,description",
    [
        ("ab", "minimum length"),
        ("Abcde", "startswith a capital letter"),
        ("abCde", "contains capital letter"),
        ("abc_d", "contains dash"),
        ("abc_d", "contains underscore"),
        ("abc_d", "contains dot"),
        ("abc5", "contains a number"),
        ("5abc", "startswith a number"),
        ("A" * 256, "maximum length"),
    ],
)
def test_validate_job_name__ok(ok_value: str, description: str):
    assert validate_job_name(ok_value) == ok_value, description


class TestJobRequestValidator:
    def test_job_without_name(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "ssh": {"port": 666},
        }
        validator = create_job_request_validator(allowed_gpu_models=[])
        assert validator.check({"container": container})

    def test_job_with_name(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "ssh": {"port": 666},
        }
        validator = create_job_request_validator(allowed_gpu_models=[])
        assert validator.check(
            {
                "container": container,
                "name": "test_job-Name_123",
                "description": "test-job",
            }
        )


class TestJobResponseValidator:
    def test_job_details_with_name(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "ssh": {"port": 666},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "status": "pending",
            "name": "test-job-Name_123",
            "description": "test-job",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
            },
            "container": container,
            "ssh_auth_server": "ssh-auth",
            "is_preemptible": False,
        }
        validator = create_job_response_validator()
        assert validator.check(response)

    def test_job_details_without_name(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "ssh": {"port": 666},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "status": "pending",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
            },
            "container": container,
            "ssh_auth_server": "ssh-auth",
            "is_preemptible": False,
        }
        validator = create_job_response_validator()
        assert validator.check(response)
