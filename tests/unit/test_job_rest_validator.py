import pytest
from trafaret import DataError

from platform_api.handlers.jobs_handler import (
    create_job_request_validator,
    create_job_response_validator,
)
from platform_api.handlers.validators import create_job_filter_request_validator


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
        assert validator.check({"container": container, "description": "test-job"})


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


class TestJobFilterRequestValidator:
    def test_job_filter_request_validator__none(self):
        request = None
        validator = create_job_filter_request_validator()
        with pytest.raises(DataError, match="value is not a dict"):
            validator.check(request)

    def test_job_filter_request_validator__empty(self):
        request = {}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {}

    def test_job_filter_request_validator__status_empty_fail(self):
        request = {"status": ""}
        validator = create_job_filter_request_validator()
        with pytest.raises(DataError, match="Empty status line"):
            validator.check(request)

    def test_job_filter_request_validator__status_none_fail(self):
        request = {"status": None}
        validator = create_job_filter_request_validator()
        with pytest.raises(DataError, match="Empty status line"):
            validator.check(request)

    def test_job_filter_request_validator__status_single_element_pending(self):
        request = {"status": "pending"}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"pending"}}

    def test_job_filter_request_validator__status_single_element_running(self):
        request = {"status": "pending"}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"pending"}}

    def test_job_filter_request_validator__status_single_element_succeeded(self):
        request = {"status": "succeeded"}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"succeeded"}}

    def test_job_filter_request_validator__status_single_element_failed(self):
        request = {"status": "failed"}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"failed"}}

    def test_job_filter_request_validator__status_single_element_foo(self):
        request = {"status": "foo"}
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match='Invalid status: "foo"'):
            validator.check(request)

    def test_job_filter_request_validator__status_two_elements_running_pending(self):
        request = {"status": "running+pending"}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"running", "pending"}}

    def test_job_filter_request_validator__status_two_elements_pending_running(self):
        request = {"status": "pending+running"}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"running", "pending"}}

    def test_job_filter_request_validator__status_three_elements(self):
        request = {"status": "pending+running+failed"}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"running", "pending", "failed"}}

    def test_job_filter_request_validator__status_four_elements(self):
        request = {"status": "pending+running+failed+succeeded"}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {
            "status": {"running", "pending", "failed", "succeeded"}
        }

    def test_job_filter_request_validator__status_four_elements__fail(self):
        request = {"status": "pending+foo+failed+succeeded"}
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match='Invalid status: "foo"'):
            validator.check(request)

    def test_job_filter_request_validator__all__fail(self):
        request = {"status": "all"}
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match='Invalid status: "all"'):
            validator.check(request)
