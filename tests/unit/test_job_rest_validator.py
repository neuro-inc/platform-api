import pytest
import trafaret as t

from platform_api.handlers.jobs_handler import (
    create_job_request_validator,
    create_job_response_validator,
)
from platform_api.handlers.validators import create_job_name_validator


class TestJobNameValidator:
    @pytest.mark.parametrize(
        "value,description",
        [
            ("abc-d", "contains dash"),
            ("a-b-c-d", "contains dash"),
            ("ab3d", "contains a number"),
            ("abc5", "ends with a number"),
            ("a" * 3, "minimum length"),
            ("a" * 100, "maximum length"),
        ],
    )
    def test_valid_job_names(self, value: str, description: str):
        validator = create_job_name_validator()
        assert validator.check(value)

    @pytest.mark.parametrize("value", ["", None])
    def test_invalid_job_names__empty(self, value: str):
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="blank value is not allowed"):
            assert validator.check(value)

    @pytest.mark.parametrize("value", ["a", "aa"])
    def test_invalid_job_names__too_short(self, value: str):
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="String is shorter than 3 characters"):
            assert validator.check(value)

    @pytest.mark.parametrize("value", ["a" * 101])
    def test_invalid_job_names__too_long(self, value: str):
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="String is longer than 100 characters"):
            assert validator.check(value)

    @pytest.mark.parametrize(
        "value",
        [
            "-abc",
            "_abc",
            ".abc",
            "?abc",
            "#abc",
            "a_bc",
            "a.bc",
            "a?bc",
            "a#bc",
            "abc-",
            "abc_",
            "abc.",
            "abc?",
            "abc#",
        ],
    )
    def test_invalid_job_names__contains_illegal_char(self, value: str):
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="does not match pattern"):
            assert validator.check(value)

    @pytest.mark.parametrize("value", ["Abcde", "abCde", "abcdE"])
    def test_invalid_job_names__contains_upppercase_char(self, value: str):
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="does not match pattern"):
            assert validator.check(value)


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
            {"container": container, "name": "test-job-name", "description": "test-job"}
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
            "name": "test-job-name",
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
