import pytest
from multidict import MultiDict, MultiDictProxy
from trafaret import DataError

from platform_api.handlers.jobs_handler import (
    create_job_request_validator,
    create_job_response_validator,
)
from platform_api.handlers.validators import (
    convert_multidict_to_dict,
    create_job_filter_request_validator,
)


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
    def test_convert_multidict_to_dict_single_values(self):
        multidict = MultiDictProxy(MultiDict([("a", 1), ("b", 2)]))
        assert convert_multidict_to_dict(multidict) == {"a": 1, "b": 2}

    def test_convert_multidict_to_dict_multiple_values(self):
        multidict = MultiDictProxy(MultiDict([("a", 1), ("a", 3), ("b", 2)]))
        assert convert_multidict_to_dict(multidict) == {"a": {1, 3}, "b": 2}

    def test_job_filter_request_validator__none(self):
        request = None
        validator = create_job_filter_request_validator()
        with pytest.raises(DataError, match="value is not a dict"):
            validator.check(request)

    def test_job_filter_request_validator__empty(self):
        request = {}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {}

    def test_job_filter_request_validator__status_none_fail(self):
        request = {"status": None}
        validator = create_job_filter_request_validator()
        with pytest.raises(DataError, match="empty status value"):
            validator.check(request)

    def test_job_filter_request_validator__status_list_fail(self):
        request = {"status": ["pending"]}
        validator = create_job_filter_request_validator()
        with pytest.raises(
            DataError, match="value is not a string or a set of strings"
        ):
            validator.check(request)

    def test_job_filter_request_validator__status_empty_set_fail(self):
        request = {"status": {}}
        validator = create_job_filter_request_validator()
        with pytest.raises(DataError, match="empty status value"):
            validator.check(request)

    def test_job_filter_request_validator__status_empty_str_fail(self):
        request = {"status": ""}
        validator = create_job_filter_request_validator()
        with pytest.raises(DataError, match="empty status value"):
            validator.check(request)

    def test_job_filter_request_validator__empty_str_in_set_fail(self):
        request = {"status": {""}}
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match='Invalid status: ""'):
            validator.check(request)

    def test_job_filter_request_validator__none_in_set_fail(self):
        request = {"status": {None}}
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match='Invalid status: "None"'):
            validator.check(request)

    def test_job_filter_request_validator__single_value(self):
        request = {"status": "pending"}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"pending"}}

    def test_job_filter_request_validator__set_single_element_pending(self):
        request = {"status": {"pending"}}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"pending"}}

    def test_job_filter_request_validator__single_value_foo__fail(self):
        request = {"status": "foo"}
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match='Invalid status: "foo"'):
            validator.check(request)

    def test_job_filter_request_validator__set_single_element_foo__fail(self):
        request = {"status": {"foo"}}
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match='Invalid status: "foo"'):
            validator.check(request)

    def test_job_filter_request_validator__set_two_elements_running_pending(self):
        request = {"status": {"running", "pending"}}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {"status": {"running", "pending"}}

    def test_job_filter_request_validator__set_four_elements(self):
        request = {"status": {"pending", "running", "failed", "succeeded"}}
        validator = create_job_filter_request_validator()
        assert validator.check(request) == {
            "status": {"running", "pending", "failed", "succeeded"}
        }

    def test_job_filter_request_validator__set_four_elements__fail(self):
        request = {"status": {"pending", "foo", "failed", "succeeded"}}
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match='Invalid status: "foo"'):
            validator.check(request)

    def test_job_filter_request_validator__single_value_all__fail(self):
        request = {"status": "all"}
        validator = create_job_filter_request_validator()
        with pytest.raises(ValueError, match='Invalid status: "all"'):
            validator.check(request)
