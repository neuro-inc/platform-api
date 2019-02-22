from platform_api.handlers.validators import validate_job_name
from platform_api.handlers.jobs_handler import (
    create_job_request_validator,
    create_job_response_validator,
)


def test_validate_job_name__fail():
    assert validate_job_name("") is False, "should be at least 2 letters"
    assert validate_job_name("a") is False, "should be at least 2 letters"
    assert validate_job_name("-abc") is False, "should not start with dash"
    assert validate_job_name("_abc") is False, "should not start with underscore"
    assert validate_job_name(".abc") is False, "should not start with dot"
    assert validate_job_name("A" * 257) is False, "should be at most 256 letters"


def test_validate_job_name__ok():
    assert validate_job_name("ab") is True, "minimum length"
    assert validate_job_name("Abcde") is True, "startswith a capital letter"
    assert validate_job_name("abCde") is True, "contains capital letter"
    assert validate_job_name("abc-d") is True, "contains dash"
    assert validate_job_name("abc_d") is True, "contains underscore"
    assert validate_job_name("abc.d") is True, "contains dot"
    assert validate_job_name("abc5") is True, "contains a number"
    assert validate_job_name("5abc") is True, "startswith a number"
    assert validate_job_name("A" * 256) is True, "maximum length"


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
