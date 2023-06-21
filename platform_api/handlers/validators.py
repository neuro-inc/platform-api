import shlex
from collections.abc import Sequence
from pathlib import PurePath
from typing import Any, Optional, Union
from urllib.parse import unquote, urlsplit

import trafaret as t
from yarl import URL

from platform_api.orchestrator.job import JOB_NAME_SEPARATOR
from platform_api.orchestrator.job_request import JobStatus
from platform_api.resource import TPUResource

JOB_NAME_PATTERN = r"\A[a-z](?:-?[a-z0-9])*\Z"
USER_NAME_PATTERN = r"\A[a-z0-9](?:-?[a-z0-9])*\Z"
ROLE_NAME_PATTERN = r"\A[a-z0-9](?:[-/]?[a-z0-9])*\Z"
CLUSTER_NAME_PATTERN = r"\A[a-z0-9](?:-?[a-z0-9])*\Z"
ORG_NAME_PATTERN = r"\A[a-z0-9](?:-?[a-z0-9])*\Z"
PROJECT_NAME_PATTERN = r"\A[a-z0-9](?:-?[a-z0-9])*\Z"
JOB_TAG_PATTERN = r"\A(?:\S)*\Z"

# Since the client supports job-names to be interchangeable with job-IDs
# (see PR https://github.com/neuromation/platform-client-python/pull/648)
# therefore job-ID has to conform job-name validator; all job-IDs are
# of the form `job-{uuid4()}` of length 40.
JOB_NAME_MAX_LENGTH = 40
# For named jobs, their hostname is of the form of
# `{job-id}{JOB_USER_NAMES_SEPARATOR}{job-project-name}.jobs.neu.ro`.
# The length limit for DNS label is 63 chars.
USER_NAME_MAX_LENGTH = 63 - len(JOB_NAME_SEPARATOR) - JOB_NAME_MAX_LENGTH

OptionalString = t.String(allow_blank=True) | t.Null


def create_job_name_validator(
    max_length: Optional[int] = JOB_NAME_MAX_LENGTH,
) -> t.Trafaret:
    return t.Null | t.String(min_length=3, max_length=max_length) & t.Regexp(
        JOB_NAME_PATTERN
    )


def create_base_owner_name_validator() -> t.Trafaret:
    # NOTE: this validator is almost the same as the one used in platform-auth
    return t.String(min_length=3, max_length=USER_NAME_MAX_LENGTH) & t.Regexp(
        USER_NAME_PATTERN
    )


def create_user_name_validator() -> t.Trafaret:
    def _validate(username: str) -> str:
        base_owner_name = username.split("/", 0)[0]
        t.String(min_length=3, max_length=USER_NAME_MAX_LENGTH).check(base_owner_name)
        return username

    return t.String(min_length=3) & t.Regexp(ROLE_NAME_PATTERN) & t.Call(_validate)


def create_cluster_name_validator() -> t.Trafaret:
    # NOTE: this validator is almost the same as the one used in platform-auth
    return t.String(min_length=3, max_length=255) & t.Regexp(CLUSTER_NAME_PATTERN)


def create_org_name_validator() -> t.Trafaret:
    return t.String(min_length=3, max_length=255) & t.Regexp(ORG_NAME_PATTERN)


def create_project_name_validator() -> t.Trafaret:
    return t.String(min_length=3, max_length=255) & t.Regexp(PROJECT_NAME_PATTERN)


def create_job_status_validator() -> t.Trafaret:
    return t.Enum(*JobStatus.values())


def create_job_history_validator() -> t.Trafaret:
    return t.Dict(
        {
            "status": create_job_status_validator(),
            "reason": OptionalString,
            "description": OptionalString,
            "created_at": t.String,
            "restarts": t.Int(gte=0),
            t.Key("exit_code", optional=True): t.Int,
            t.Key("started_at", optional=True): t.String,
            t.Key("finished_at", optional=True): t.String,
            t.Key("run_time_seconds"): t.Float(gte=0),
        }
    )


def _check_dots_in_path(path: Union[str, PurePath]) -> None:
    if ".." in PurePath(path).parts:
        raise t.DataError(f"Invalid path: '{path}'")


def create_path_uri_validator(
    storage_scheme: str,
    cluster_name: str = "",
    check_cluster: bool = True,
    assert_username: Optional[str] = None,
    assert_parts_count_ge: Optional[int] = None,
) -> t.Trafaret:
    assert storage_scheme
    if check_cluster:
        assert cluster_name

    def _validate(uri_str: str) -> str:
        uri = urlsplit(uri_str)
        if uri.scheme != storage_scheme:
            # validate `scheme` in `scheme://cluster/username/path/to`
            raise t.DataError(
                f"Invalid URI scheme: '{uri.scheme}' != '{storage_scheme}'"
            )
        if "#" in uri_str:
            raise t.DataError("Fragment part is not allowed in URI")
        if "?" in uri_str:
            raise t.DataError("Query part is not allowed in URI")
        if check_cluster and uri.netloc != cluster_name:
            # validate `cluster` in `scheme://cluster/username/path/to`
            raise t.DataError(
                f"Invalid URI cluster: '{uri.netloc}' != '{cluster_name}'"
            )
        path = unquote(uri.path)
        if assert_parts_count_ge:
            parts = PurePath(path).parts
            if len(parts) < assert_parts_count_ge:
                raise t.DataError("Invalid URI path: Wrong number of path items")
        if assert_username is not None:
            # validate `username` in `scheme://cluster/username/path/to`
            parts = PurePath(path).parts
            if len(parts) < 2:
                raise t.DataError("Invalid URI path: Not enough path items")
            assert parts[0] == "/", (uri, parts)
            usr = parts[1]
            if usr != assert_username:
                raise t.DataError(
                    f"Invalid URI: Invalid user in path: '{usr}' != '{assert_username}'"
                )
        _check_dots_in_path(path)
        return uri_str

    return t.Call(_validate)


def create_mount_path_validator() -> t.Trafaret:
    def _validate(path_str: str) -> str:
        path = PurePath(path_str)
        if not path.is_absolute():
            raise t.DataError(f"Mount path must be absolute: '{path}'")
        _check_dots_in_path(path)
        return str(path)

    return t.Call(_validate)


def _validate_unique_volume_paths(
    volumes: Sequence[dict[str, Any]]
) -> Sequence[dict[str, Any]]:
    paths: set[str] = set()
    for volume in volumes:
        path = volume["dst_path"]
        if path in paths:
            raise t.DataError(
                "destination path '{path}' was encountered multiple times".format(
                    path=path
                )
            )
        paths.add(path)
    return volumes


def create_volumes_validator(
    uri_key: str = "src_storage_uri",
    has_read_only_key: bool = True,
    storage_scheme: str = "storage",
    cluster_name: str = "",
    check_cluster: bool = True,
    assert_username: Optional[str] = None,
    assert_parts_count_ge: Optional[int] = None,
) -> t.Trafaret:
    template_dict = {
        uri_key: create_path_uri_validator(
            storage_scheme=storage_scheme,
            cluster_name=cluster_name,
            check_cluster=check_cluster,
            assert_username=assert_username,
            assert_parts_count_ge=assert_parts_count_ge,
        ),
        "dst_path": create_mount_path_validator(),
    }
    if has_read_only_key:
        template_dict[t.Key("read_only", optional=True, default=True)] = t.Bool()
    single_volume_validator: t.Trafaret = t.Dict(template_dict)
    return t.List(single_volume_validator) & t.Call(_validate_unique_volume_paths)


def create_resources_validator(
    *,
    allow_any_gpu_models: bool = False,
    allowed_gpu_models: Optional[Sequence[str]] = None,
    allow_any_tpu: bool = False,
    allowed_tpu_resources: Sequence[TPUResource] = (),
) -> t.Trafaret:
    def check_memory_keys(data: Any) -> Any:
        if "memory" not in data and "memory_mb" not in data:
            raise t.DataError("Either memory or memory_mb should be present")
        return data

    common_resources_validator = t.Dict(
        {
            "cpu": t.Float(gte=0.1),
            t.Key("memory", optional=True): t.Int(gte=16 * 2**20),
            t.Key("memory_mb", optional=True): t.Int(gte=16),
            t.Key("shm", optional=True): t.Bool,
        }
    )

    tpu_validator = create_tpu_validator(
        allow_any=allow_any_tpu, allowed=allowed_tpu_resources
    )

    gpu_validator = t.Int(gte=0)
    if allow_any_gpu_models:
        gpu_model_validator = t.String
    else:
        gpu_model_validator = t.Enum(*(allowed_gpu_models or []))

    validators = [
        common_resources_validator,
        common_resources_validator
        + t.Dict(
            {
                "gpu": gpu_validator,
                t.Key("gpu_model", optional=True): gpu_model_validator,
            }
        ),
    ]

    if tpu_validator:
        validators.append(common_resources_validator + t.Dict({"tpu": tpu_validator}))

    return t.Or(*validators) & check_memory_keys


def create_tpu_validator(
    *, allow_any: bool = False, allowed: Sequence[TPUResource] = ()
) -> Optional[t.Trafaret]:
    if allow_any:
        return t.Dict({"type": t.String, "software_version": t.String})

    if not allowed:
        return None

    validators = []
    for resource in allowed:
        validators.append(
            t.Dict(
                {
                    "type": t.Enum(*resource.types),
                    "software_version": t.Enum(*resource.software_versions),
                }
            )
        )
    return t.Or(*validators)


def create_container_validator(
    *,
    allow_volumes: bool = False,
    allow_any_gpu_models: bool = False,
    allowed_gpu_models: Optional[Sequence[str]] = None,
    allow_any_tpu: bool = False,
    allowed_tpu_resources: Sequence[TPUResource] = (),
    allow_any_command: bool = False,
    storage_scheme: str = "storage",
    cluster_name: str = "",
    check_cluster: bool = True,
) -> t.Trafaret:
    """Create a validator for primitive container objects.

    Meant to be used in high-level resources such as jobs, models, batch
    inference etc.
    """

    validator = t.Dict(
        {
            "image": t.String,
            t.Key("entrypoint", optional=True): create_container_command_validator(
                allow_any_command=allow_any_command
            ),
            t.Key("command", optional=True): create_container_command_validator(
                allow_any_command=allow_any_command
            ),
            t.Key("env", optional=True): t.Mapping(
                t.String, t.String(allow_blank=True)
            ),
            "resources": create_resources_validator(
                allow_any_gpu_models=allow_any_gpu_models,
                allowed_gpu_models=allowed_gpu_models,
                allow_any_tpu=allow_any_tpu,
                allowed_tpu_resources=allowed_tpu_resources,
            ),
            t.Key("http", optional=True): t.Dict(
                {
                    "port": t.Int(gte=0, lte=65535),
                    t.Key("health_check_path", optional=True): t.String,
                    t.Key("requires_auth", optional=True, default=False): t.Bool,
                }
            ),
            t.Key("tty", optional=True, default=False): t.Bool,
            t.Key("secret_env", optional=True): t.Mapping(
                t.String,
                create_path_uri_validator(
                    storage_scheme="secret",
                    cluster_name=cluster_name,
                    check_cluster=check_cluster,
                    assert_parts_count_ge=3,
                ),
            ),
            t.Key("secret_volumes", optional=True): create_volumes_validator(
                uri_key="src_secret_uri",
                has_read_only_key=False,
                storage_scheme="secret",
                cluster_name=cluster_name,
                check_cluster=check_cluster,
                # Should exactly include ("/", "username", "secret_name")
                assert_parts_count_ge=3,
            ),
            t.Key("disk_volumes", optional=True): create_volumes_validator(
                uri_key="src_disk_uri",
                has_read_only_key=True,
                storage_scheme="disk",
                cluster_name=cluster_name,
                check_cluster=check_cluster,
                # Should exactly include ("/", "username", "disk_name")
                assert_parts_count_ge=3,
            ),
            t.Key("working_dir", optional=True): create_working_dir_validator(),
        }
    )

    if allow_volumes:
        validator += t.Dict(
            {
                t.Key("volumes", optional=True): create_volumes_validator(
                    uri_key="src_storage_uri",
                    has_read_only_key=True,
                    storage_scheme=storage_scheme,
                    cluster_name=cluster_name,
                    check_cluster=check_cluster,
                )
            }
        )

    return validator


def create_container_request_validator(
    *,
    allow_volumes: bool = False,
    allowed_gpu_models: Optional[Sequence[str]] = None,
    allow_any_tpu: bool = False,
    allowed_tpu_resources: Sequence[TPUResource] = (),
    storage_scheme: str = "storage",
    cluster_name: str = "",
) -> t.Trafaret:
    return create_container_validator(
        allow_volumes=allow_volumes,
        allowed_gpu_models=allowed_gpu_models,
        allow_any_tpu=allow_any_tpu,
        allowed_tpu_resources=allowed_tpu_resources,
        storage_scheme=storage_scheme,
        cluster_name=cluster_name,
        check_cluster=True,
    )


def create_container_response_validator() -> t.Trafaret:
    return create_container_validator(
        allow_volumes=True,
        allow_any_gpu_models=True,
        allow_any_tpu=True,
        allow_any_command=True,
        check_cluster=False,
    )


def sanitize_dns_name(value: str) -> Optional[str]:
    """This is a TEMPORARY METHOD used to sanitize DNS names so that they are parseable
    by the client (issue #642).
    :param value: String representing a DNS name
    :return: `value` if it can be parsed by `yarl.URL`, `None` otherwise
    """
    try:
        URL(value)
        return value
    except ValueError:
        return None


def create_container_command_validator(
    *, allow_any_command: bool = False
) -> t.Trafaret:
    def _validate(command: str) -> str:
        if not allow_any_command:
            try:
                shlex.split(command)
            except ValueError:
                raise t.DataError("invalid command format")
        return command

    return t.String() >> _validate


def create_working_dir_validator() -> t.Trafaret:
    return t.String() & t.Regexp("/.*")


def create_job_tag_validator() -> t.Trafaret:
    return t.String(min_length=1, max_length=256) & t.Regexp(JOB_TAG_PATTERN)
