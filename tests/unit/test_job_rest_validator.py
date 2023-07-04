from datetime import datetime, timezone

import pytest
import trafaret as t

from platform_api.handlers.jobs_handler import create_job_response_validator
from platform_api.handlers.validators import (
    JOB_NAME_MAX_LENGTH,
    USER_NAME_MAX_LENGTH,
    create_base_owner_name_validator,
    create_job_name_validator,
    create_mount_path_validator,
    create_path_uri_validator,
    create_user_name_validator,
    create_volumes_validator,
)


class TestJobNameValidator:
    @pytest.mark.parametrize(
        "value,description",
        [
            ("abc-d", "contains dash"),
            ("a-b-c-d", "contains dash"),
            ("ab3d", "contains a number"),
            ("abc3", "ends with a number"),
            ("a" * 3, "minimum length"),
            ("a" * JOB_NAME_MAX_LENGTH, "maximum length"),
        ],
    )
    def test_valid_job_names(self, value: str, description: str) -> None:
        validator = create_job_name_validator()
        assert validator.check(value)

    def test_invalid_job_names__none(self) -> None:
        value = None
        validator = create_job_name_validator()
        assert validator.check(value) is None

    def test_invalid_job_names__empty(self) -> None:
        value = ""
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="blank value is not allowed"):
            assert validator.check(value)

    @pytest.mark.parametrize("value", ["a", "aa"])
    def test_invalid_job_names__too_short(self, value: str) -> None:
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="String is shorter than 3 characters"):
            assert validator.check(value)

    def test_invalid_job_names__too_long(self) -> None:
        value = "a" * (JOB_NAME_MAX_LENGTH + 1)
        validator = create_job_name_validator()
        with pytest.raises(
            t.DataError, match=f"String is longer than {JOB_NAME_MAX_LENGTH} characters"
        ):
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
            "abc\n",
        ],
    )
    def test_invalid_job_names__contains_illegal_char(self, value: str) -> None:
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="does not match pattern"):
            assert validator.check(value)

    def test_invalid_job_names__startswith_dash(self) -> None:
        value = "-abc"
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="does not match pattern"):
            assert validator.check(value)

    def test_invalid_job_names__endswith_dash(self) -> None:
        value = "abc-"
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="does not match pattern"):
            assert validator.check(value)

    def test_invalid_job_names__contains_doubledash(self) -> None:
        value = "abc--d"
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="does not match pattern"):
            assert validator.check(value)

    def test_invalid_job_names__startswith_number(self) -> None:
        value = "5abc"
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="does not match pattern"):
            assert validator.check(value)

    @pytest.mark.parametrize("value", ["Abcde", "abCde", "abcdE"])
    def test_invalid_job_names__contains_upppercase_char(self, value: str) -> None:
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="does not match pattern"):
            assert validator.check(value)

    def test_create_job_name_validator_custom_max_length_non_null(self) -> None:
        value = "a" * 11
        validator = create_job_name_validator(max_length=10)
        with pytest.raises(t.DataError):
            assert validator.check(value)

    def test_create_job_name_validator_custom_max_length_null(self) -> None:
        value = "a" * 11
        validator = create_job_name_validator(max_length=None)
        assert validator.check(value)


class TestUserNameValidator:
    """Almost the same test suite as used for the same
    user-name validation method in platform-auth
    """

    def test_create_user_name_validator_none__ok(self) -> None:
        value = None
        validator = create_user_name_validator()
        with pytest.raises(t.DataError):
            validator.check(value)

    @pytest.mark.parametrize(
        "pair",
        [
            ("test", 1),
            ("abc", 1),
            ("a", USER_NAME_MAX_LENGTH),
            ("a-b-c", 1),
            ("a-b-c", (USER_NAME_MAX_LENGTH // len("a-b-c"))),
            ("123", 1),
            ("with123numbers", 1),
            ("with123nums-and-dash", 1),
        ],
    )
    def test_user_name_validators__ok(self, pair: tuple[str, int]) -> None:
        value = pair[0] * pair[1]
        validator = create_user_name_validator()
        assert validator.check(value)
        validator = create_base_owner_name_validator()
        assert validator.check(value)

    @pytest.mark.parametrize(
        "pair",
        [
            ("test/foo/bar", 1),
        ],
    )
    def test_role_name_validator__ok(self, pair: tuple[str, int]) -> None:
        value = pair[0] * pair[1]
        validator = create_user_name_validator()
        assert validator.check(value)

    @pytest.mark.parametrize(
        "pair",
        [
            ("test/foo/bar", 1),
        ],
    )
    def test_base_owner_validator__fail(self, pair: tuple[str, int]) -> None:
        value = pair[0] * pair[1]
        validator = create_base_owner_name_validator()
        with pytest.raises(t.DataError):
            assert validator.check(value)

    @pytest.mark.parametrize(
        "pair",
        [
            ("", 1),
            ("\t", 1),
            ("abc-", 1),
            ("-abc", 1),
            ("a", 1),
            ("a", 2),
            ("a", (USER_NAME_MAX_LENGTH + 1)),
            ("a" * (USER_NAME_MAX_LENGTH + 1) + "/test/parts", 1),
            ("too-long-string", 1000),
            ("a-b-c.com", 1),
            ("a_b_c", 1),
            ("a-b-c.bla_bla.com", 1),
            ("abc--def", 1),
            ("WithCapitalLetters", 1),
            ("with123numbers-and-hyphen_and-underscore", 1),
            ("name_with_hyphen-and-numbers123_and-underscore", 1),
            ("with123numbers.co.uk", 1),
            ("WithCapitalLetters", 1),
            ("foo!", 1),
            ("foo@", 1),
            ("foo#", 1),
            ("foo$", 1),
            ("foo%", 1),
            ("foo^", 1),
            ("foo&", 1),
            ("foo*", 1),
            ("foo(", 1),
            ("foo)", 1),
            ("foo:", 1),
            ("foo_", 1),
            ("foo+", 1),
            ("foo=", 1),
            ("foo/", 1),
            ("foo\\", 1),
            ("foo~", 1),
            ("foo,", 1),
            ("foo.", 1),
            ("foo\n", 1),
            ("46CAC3A6-2956-481B-B4AA-A80A6EAF2CDE", 1),  # regression test
        ],
    )
    def test_user_name_validators__fail(self, pair: tuple[str, int]) -> None:
        value = pair[0] * pair[1]
        validator = create_user_name_validator()
        with pytest.raises(t.DataError):
            assert validator.check(value)
        validator = create_base_owner_name_validator()
        with pytest.raises(t.DataError):
            assert validator.check(value)


class TestJobResponseValidator:
    def test_job_details_with_name(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "org_project_hash": "0123456789",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "name": "test-job-name",
            "description": "test-job",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": 10,
                "restarts": 0,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "privileged": False,
            "being_dropped": False,
            "logs_removed": False,
            "price_credits_per_hour": "10",
            "total_price_credits": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        assert validator.check(response)

    def test_job_empty_description(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "org_project_hash": "0123456789",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "name": "test-job-name",
            "description": "test-job",
            "history": {
                "status": "pending",
                "reason": None,
                "description": "",
                "created_at": "now",
                "run_time_seconds": 10,
                "restarts": 0,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "privileged": False,
            "being_dropped": False,
            "logs_removed": False,
            "price_credits_per_hour": "10",
            "total_price_credits": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        assert validator.check(response)

    def test_job_details_without_name(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "org_project_hash": "0123456789",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": 10,
                "restarts": 0,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "privileged": False,
            "being_dropped": False,
            "logs_removed": False,
            "price_credits_per_hour": "10",
            "total_price_credits": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        assert validator.check(response)

    def test_with_entrypoint_and_cmd(self) -> None:
        container = {
            "image": "testimage",
            "entrypoint": "/script.sh",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "org_project_hash": "0123456789",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "name": "test-job-name",
            "description": "test-job",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": 10,
                "restarts": 0,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "privileged": False,
            "being_dropped": False,
            "logs_removed": False,
            "price_credits_per_hour": "10",
            "total_price_credits": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        assert validator.check(response)

    def test_with_absolute_working_dir(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "working_dir": "/working/dir",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "org_project_hash": "0123456789",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": 10,
                "restarts": 0,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "privileged": False,
            "being_dropped": False,
            "logs_removed": False,
            "price_credits_per_hour": "0",
            "total_price_credits": "0",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        assert validator.check(response)

    def test_with_relative_working_dir(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "working_dir": "working/dir",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": 10,
                "restarts": 0,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "price_credits_per_hour": "10",
            "total_price_credits": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        with pytest.raises(t.DataError):
            validator.check(response)

    def test_with_max_run_time_minutes(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "org_project_hash": "0123456789",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "name": "test-job-name",
            "description": "test-job",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": 10,
                "restarts": 0,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "max_run_time_minutes": 10,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "privileged": False,
            "being_dropped": False,
            "logs_removed": False,
            "price_credits_per_hour": "10",
            "total_price_credits": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        assert validator.check(response)

    def test_with_invalid_run_time_seconds(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "name": "test-job-name",
            "description": "test-job",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": -10.0,
                "restarts": 0,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "price_credits_per_hour": "10",
            "total_price_credits": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        with pytest.raises(t.DataError, match="value is less than 0"):
            assert validator.check(response)

    def test_with_invalid_restarts_count(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "name": "test-job-name",
            "description": "test-job",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": 20.0,
                "restarts": -10,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "price_credits_per_hour": "10",
            "total_price_credits": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        with pytest.raises(t.DataError, match="value is less than 0"):
            assert validator.check(response)

    def test_without_price_per_hour(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "name": "test-job-name",
            "description": "test-job",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": 10,
                "restarts": 0,
            },
            "container": container,
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "privileged": False,
            "being_dropped": False,
            "logs_removed": False,
            "total_price_credits": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        with pytest.raises(t.DataError, match="price_credits_per_hour"):
            assert validator.check(response)

    def test_without_total_price(self) -> None:
        response = {
            "id": "test-job-id",
            "owner": "tests",
            "cluster_name": "cluster-name",
            "project_name": "project",
            "status": "pending",
            "statuses": [
                {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "transition_time": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "name": "test-job-name",
            "description": "test-job",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": "now",
                "run_time_seconds": 10,
                "restarts": 0,
            },
            "container": {
                "image": "testimage",
                "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            },
            "scheduler_enabled": False,
            "preemptible_node": False,
            "materialized": False,
            "pass_config": False,
            "uri": "job://cluster-name/tests/test-job-id",
            "restart_policy": "never",
            "privileged": False,
            "being_dropped": False,
            "logs_removed": False,
            "price_credits_per_hour": "10",
            "priority": "normal",
        }
        validator = create_job_response_validator()
        with pytest.raises(t.DataError, match="total_price_credits"):
            assert validator.check(response)


class TestVolumesValidator:
    def test_valid_volumes(self) -> None:
        value = [
            {
                "src_storage_uri": "storage://test-cluster/uri1",
                "dst_path": "/path1",
                "read_only": True,
            },
            {
                "src_storage_uri": "storage://test-cluster/uri2",
                "dst_path": "/path2",
                "read_only": True,
            },
        ]
        validator = create_volumes_validator(cluster_name="test-cluster")
        assert validator.check(value)

    def test_destination_paths_are_unique(self) -> None:
        value = [
            {
                "src_storage_uri": "storage://test-cluster/uri1",
                "dst_path": "path",
                "read_only": True,
            },
            {
                "src_storage_uri": "storage://test-cluster/uri2",
                "dst_path": "path",
                "read_only": True,
            },
        ]
        validator = create_volumes_validator(cluster_name="test-cluster")
        with pytest.raises(t.DataError):
            assert validator.check(value)

    def test_volumes_are_unique(self) -> None:
        value = [
            {
                "src_storage_uri": "storage://test-cluster/uri",
                "dst_path": "path",
                "read_only": True,
            },
            {
                "src_storage_uri": "storage://test-cluster/uri",
                "dst_path": "path",
                "read_only": True,
            },
        ]
        validator = create_volumes_validator(cluster_name="test-cluster")
        with pytest.raises(t.DataError):
            assert validator.check(value)


class TestSecretVolumesValidator:
    def test_valid_volumes(self) -> None:
        value = [
            {"src_secret_uri": "storage://test-cluster/uri1", "dst_path": "/path1"},
            {"src_secret_uri": "storage://test-cluster/uri2", "dst_path": "/path2"},
        ]
        validator = create_volumes_validator(
            uri_key="src_secret_uri",
            has_read_only_key=False,
            cluster_name="test-cluster",
        )
        assert validator.check(value)

    def test_destination_paths_are_unique(self) -> None:
        value = [
            {"src_secret_uri": "storage://test-cluster/uri1", "dst_path": "path"},
            {"src_secret_uri": "storage://test-cluster/uri2", "dst_path": "path"},
        ]
        validator = create_volumes_validator(
            uri_key="src_secret_uri",
            has_read_only_key=False,
            cluster_name="test-cluster",
        )
        with pytest.raises(t.DataError):
            assert validator.check(value)

    def test_volumes_are_unique(self) -> None:
        value = [
            {"src_secret_uri": "storage://test-cluster/uri", "dst_path": "path"},
            {"src_secret_uri": "storage://test-cluster/uri", "dst_path": "path"},
        ]
        validator = create_volumes_validator(
            uri_key="src_secret_uri",
            has_read_only_key=False,
            cluster_name="test-cluster",
        )
        with pytest.raises(t.DataError):
            assert validator.check(value)


class TestPathUriValidator:
    def test_invalid_uri_scheme(self) -> None:
        cluster = "test-cluster"
        uri = f"invalid://{cluster}/path"
        validator = create_path_uri_validator(
            storage_scheme="storage", cluster_name=cluster
        )
        with pytest.raises(t.DataError, match="Invalid URI scheme"):
            assert validator.check(uri)

    @pytest.mark.parametrize(
        "uri",
        (
            "storage:",
            "storage:/",
            "storage://",
            "storage:/path/to/dir",
            "storage://path/to/dir",
        ),
    )
    def test_create_invalid_uri(self, uri: str) -> None:
        cluster = "test-cluster"
        validator = create_path_uri_validator(
            storage_scheme="storage", cluster_name=cluster
        )
        with pytest.raises(t.DataError, match="Invalid URI cluster"):
            validator.check(uri)

    @pytest.mark.parametrize(
        "uri",
        (
            "storage://test-cluster/../to/dir",
            "storage://test-cluster/%2e./to/dir",
            "storage://test-cluster/.%2E/to/dir",
            "storage://test-cluster/%2E%2e/to/dir",
            "storage://test-cluster/..%2fto/dir",
            "storage://test-cluster/..%2Fto/dir",
            "storage://test-cluster/path/../dir",
            "storage://test-cluster/path/%2e./dir",
            "storage://test-cluster/path/.%2E/dir",
            "storage://test-cluster/path/%2E%2e/dir",
            "storage://test-cluster/path/..%2fdir",
            "storage://test-cluster/path/..%2Fdir",
        ),
    )
    def test_create_invalid_path(self, uri: str) -> None:
        cluster = "test-cluster"
        validator = create_path_uri_validator(
            storage_scheme="storage", cluster_name=cluster
        )
        with pytest.raises(t.DataError, match="Invalid path"):
            validator.check(uri)

    @pytest.mark.parametrize("uri", ("secret://test-cluster", "secret://test-cluster/"))
    def test_create_assert_username_missing(self, uri: str) -> None:
        cluster = "test-cluster"
        validator = create_path_uri_validator(
            storage_scheme="secret", cluster_name=cluster, assert_username="usr1"
        )
        with pytest.raises(
            t.DataError, match="Invalid URI path: Not enough path items"
        ):
            validator.check(uri)

    def test_create_assert_username_wrong(self) -> None:
        cluster = "test-cluster"
        uri = "secret://test-cluster/usr2/key1"
        validator = create_path_uri_validator(
            storage_scheme="secret", cluster_name=cluster, assert_username="usr1"
        )
        with pytest.raises(
            t.DataError, match="Invalid URI: Invalid user in path: 'usr2' != 'usr1'"
        ):
            validator.check(uri)

    @pytest.mark.parametrize(
        "uri",
        (
            "storage://test-cluster/user/path/to#",
            "storage://test-cluster/user/path/to#fragment",
        ),
    )
    def test_create_with_fragment(self, uri: str) -> None:
        cluster = "test-cluster"
        validator = create_path_uri_validator(
            storage_scheme="storage", cluster_name=cluster
        )
        with pytest.raises(t.DataError, match="Fragment part is not allowed in URI"):
            validator.check(uri)

    @pytest.mark.parametrize(
        "uri",
        (
            "storage://test-cluster/user/path/to?",
            "storage://test-cluster/user/path/to?key=value",
        ),
    )
    def test_create_with_query(self, uri: str) -> None:
        cluster = "test-cluster"
        validator = create_path_uri_validator(
            storage_scheme="storage", cluster_name=cluster
        )
        with pytest.raises(t.DataError, match="Query part is not allowed in URI"):
            validator.check(uri)

    @pytest.mark.parametrize(
        "uri",
        (
            "storage://test-cluster",
            "storage://test-cluster/",
            "storage://test-cluster/to",
        ),
    )
    def test_invalid_parts_count(self, uri: str) -> None:
        cluster = "test-cluster"
        validator = create_path_uri_validator(
            storage_scheme="storage", cluster_name=cluster, assert_parts_count_ge=3
        )
        with pytest.raises(t.DataError, match="Invalid URI path"):
            validator.check(uri)


class TestMountPathValidator:
    def test_relative_dst_mount_path(self) -> None:
        mount_path = "container/relative/path"
        validator = create_mount_path_validator()
        with pytest.raises(t.DataError, match="Mount path must be absolute"):
            validator.check(mount_path)

    def test_dots_dst_mount_path(self) -> None:
        mount_path = "/container/../path"
        validator = create_mount_path_validator()
        with pytest.raises(ValueError, match="Invalid path"):
            validator.check(mount_path)
