from typing import Tuple

import pytest
import trafaret as t

from platform_api.handlers.jobs_handler import create_job_response_validator
from platform_api.handlers.validators import (
    create_job_name_validator,
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
            ("a" * 35, "maximum length"),
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

    @pytest.mark.parametrize("value", ["a" * 36])
    def test_invalid_job_names__too_long(self, value: str) -> None:
        validator = create_job_name_validator()
        with pytest.raises(t.DataError, match="String is longer than 35 characters"):
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


class TestUserNameValidator:
    """ Almost the same test suite as used for the same
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
            ("a", 25),
            ("a-b-c", 1),
            ("a-b-c", (25 // len("a-b-c"))),
            ("123", 1),
            ("with123numbers", 1),
            ("with123numbers-and-dash", 1),
        ],
    )
    def test_create_user_name_validator__ok(self, pair: Tuple[str, int]) -> None:
        value = pair[0] * pair[1]
        validator = create_user_name_validator()
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
            ("a", 26),
            ("too_long-string", 1000),
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
            ("46CAC3A6-2956-481B-B4AA-A80A6EAF2CDE", 1),  # regression test
        ],
    )
    def test_create_user_name_validator__fail(self, pair: Tuple[str, int]) -> None:
        value = pair[0] * pair[1]
        validator = create_user_name_validator()
        with pytest.raises(t.DataError):
            assert validator.check(value)


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


class TestVolumesValidator:
    def test_valid_volumes(self) -> None:
        value = [
            {
                "src_storage_uri": "storage://uri1",
                "dst_path": "path1",
                "read_only": True,
            },
            {
                "src_storage_uri": "storage://uri2",
                "dst_path": "path2",
                "read_only": True,
            },
        ]
        validator = create_volumes_validator()
        assert validator.check(value)

    def test_destination_paths_are_unique(self) -> None:
        value = [
            {
                "src_storage_uri": "storage://uri1",
                "dst_path": "path",
                "read_only": True,
            },
            {
                "src_storage_uri": "storage://uri2",
                "dst_path": "path",
                "read_only": True,
            },
        ]
        validator = create_volumes_validator()
        with pytest.raises(t.DataError):
            assert validator.check(value)

    def test_volumes_are_unique(self) -> None:
        value = [
            {"src_storage_uri": "storage://uri", "dst_path": "path", "read_only": True},
            {"src_storage_uri": "storage://uri", "dst_path": "path", "read_only": True},
        ]
        validator = create_volumes_validator()
        with pytest.raises(t.DataError):
            assert validator.check(value)
