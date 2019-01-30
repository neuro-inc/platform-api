from typing import List, Tuple

import pytest
from multidict import MultiDict, MultiDictProxy
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
    def test_job_filter_request_validator__empty(self):
        request = MultiDictProxy(MultiDict())
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {}

    def test_job_filter_request_validator__status_none_fail(self):
        request = MultiDictProxy(MultiDict([("status", None)]))
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match="None is not a valid JobStatus"):
            validator.check(request)

    def test_job_filter_request_validator__status_empty_set_fail(self):
        request = MultiDictProxy(MultiDict([("status", "")]))
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match="'' is not a valid JobStatus"):
            validator.check(request)

    def test_job_filter_request_validator__single_value_pending(self):
        request = MultiDictProxy(MultiDict([("status", "pending")]))
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"pending"}}

    def test_job_filter_request_validator__single_value_running(self):
        request = MultiDictProxy(MultiDict([("status", "running")]))
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"running"}}

    def test_job_filter_request_validator__single_value_succeeded(self):
        request = MultiDictProxy(MultiDict([("status", "succeeded")]))
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"succeeded"}}

    def test_job_filter_request_validator__single_value_failed(self):
        request = MultiDictProxy(MultiDict([("status", "failed")]))
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"failed"}}

    def test_job_filter_request_validator__single_value_foo(self):
        request = MultiDictProxy(MultiDict([("status", "foo")]))
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match="'foo' is not a valid JobStatus"):
            validator.check(request)

    def test_job_filter_request_validator__single_value_all__fail(self):
        request = MultiDictProxy(MultiDict([("status", "all")]))
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match="'all' is not a valid JobStatus"):
            validator.check(request)

    def test_job_filter_request_validator__set_two_elements_running_pending(self):
        request = MultiDictProxy(
            MultiDict([("status", "running"), ("status", "failed")])
        )
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"running", "failed"}}

    def test_job_filter_request_validator__set_two_elements_same_values(self):
        request = MultiDictProxy(
            MultiDict([("status", "running"), ("status", "running")])
        )
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"running"}}

    def test_job_filter_request_validator__set_four_elements(self):
        request = MultiDictProxy(
            MultiDict(
                [
                    ("status", "running"),
                    ("status", "pending"),
                    ("status", "failed"),
                    ("status", "succeeded"),
                ]
            )
        )
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {
            "status": {"running", "pending", "failed", "succeeded"}
        }

    def test_job_filter_request_validator__set_four_elements_with_wrong_value(self):
        request = MultiDictProxy(
            MultiDict(
                [
                    ("status", "running"),
                    ("status", "FOO"),
                    ("status", "failed"),
                    ("status", "succeeded"),
                ]
            )
        )
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match="'FOO' is not a valid JobStatus"):
            validator.check(request)
