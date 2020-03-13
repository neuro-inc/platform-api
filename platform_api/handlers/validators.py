import shlex
from typing import Any, Dict, Optional, Sequence, Set

import trafaret as t
from yarl import URL

from platform_api.orchestrator.job import JOB_USER_NAMES_SEPARATOR
from platform_api.orchestrator.job_request import JobStatus
from platform_api.resource import TPUResource


JOB_NAME_PATTERN = r"\A[a-z](?:-?[a-z0-9])*\Z"
USER_NAME_PATTERN = r"\A[a-z0-9](?:-?[a-z0-9])*\Z"
CLUSTER_NAME_PATTERN = r"\A[a-z0-9](?:-?[a-z0-9])*\Z"
JOB_TAG_PATTERN = r"\A[a-z](?:[-.:/]?[a-z0-9])*\Z"

# Since the client supports job-names to be interchangeable with job-IDs
# (see PR https://github.com/neuromation/platform-client-python/pull/648)
# therefore job-ID has to conform job-name validator; all job-IDs are
# of the form `job-{uuid4()}` of length 40.
JOB_NAME_MAX_LENGTH = 40
# For named jobs, their hostname is of the form of
# `{job-id}{JOB_USER_NAMES_SEPARATOR}{job-owner}.jobs.neu.ro`.
# The length limit for DNS label is 63 chars.
USER_NAME_MAX_LENGTH = 63 - len(JOB_USER_NAMES_SEPARATOR) - JOB_NAME_MAX_LENGTH

OptionalString = t.String(allow_blank=True) | t.Null


def create_job_name_validator(
    max_length: Optional[int] = JOB_NAME_MAX_LENGTH,
) -> t.Trafaret:
    return t.Null | t.String(min_length=3, max_length=max_length) & t.Regexp(
        JOB_NAME_PATTERN
    )


def create_user_name_validator() -> t.Trafaret:
    # NOTE: this validator is almost the same as the one used in platform-auth
    return t.String(min_length=3, max_length=USER_NAME_MAX_LENGTH) & t.Regexp(
        USER_NAME_PATTERN
    )


def create_cluster_name_validator() -> t.Trafaret:
    # NOTE: this validator is almost the same as the one used in platform-auth
    return t.String(min_length=3, max_length=255) & t.Regexp(CLUSTER_NAME_PATTERN)


def create_job_status_validator() -> t.Trafaret:
    return t.Enum(*JobStatus.values())


def create_job_history_validator() -> t.Trafaret:
    return t.Dict(
        {
            "status": create_job_status_validator(),
            "reason": OptionalString,
            "description": OptionalString,
            "created_at": t.String,
            t.Key("exit_code", optional=True): t.Int,
            t.Key("started_at", optional=True): t.String,
            t.Key("finished_at", optional=True): t.String,
            t.Key("run_time_seconds"): t.Float(gte=0),
        }
    )


def _validate_unique_volume_paths(
    volumes: Sequence[Dict[str, Any]]
) -> Sequence[Dict[str, Any]]:
    paths: Set[str] = set()
    for volume in volumes:
        path = volume["dst_path"]
        if path in paths:
            return t.DataError(
                "destination path '{path}' was encountered multiple times".format(
                    path=path
                )
            )
        paths.add(path)
    return volumes


def create_volumes_validator() -> t.Trafaret:
    single_volume_validator: t.Trafaret = t.Dict(
        {
            "src_storage_uri": t.String,
            "dst_path": t.String,
            t.Key("read_only", optional=True, default=True): t.Bool(),
        }
    )
    return t.List(single_volume_validator) & t.Call(_validate_unique_volume_paths)


def create_resources_validator(
    *,
    allow_any_gpu_models: bool = False,
    allowed_gpu_models: Optional[Sequence[str]] = None,
    allow_any_tpu: bool = False,
    allowed_tpu_resources: Sequence[TPUResource] = (),
) -> t.Trafaret:
    MAX_GPU_COUNT = 128
    MAX_CPU_COUNT = 128.0

    common_resources_validator = t.Dict(
        {
            "cpu": t.Float(gte=0.1, lte=MAX_CPU_COUNT),
            "memory_mb": t.Int(gte=16),
            t.Key("shm", optional=True): t.Bool,
        }
    )

    tpu_validator = create_tpu_validator(
        allow_any=allow_any_tpu, allowed=allowed_tpu_resources
    )

    gpu_validator = t.Int(gte=0, lte=MAX_GPU_COUNT)
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

    tpu_validator = create_tpu_validator(
        allow_any=allow_any_tpu, allowed=allowed_tpu_resources
    )

    return t.Or(*validators)


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
            t.Key("ssh", optional=True): t.Dict({"port": t.Int(gte=0, lte=65535)}),
            t.Key("tty", optional=True, default=False): t.Bool,
        }
    )

    if allow_volumes:
        validator += t.Dict(
            {t.Key("volumes", optional=True): create_volumes_validator()}
        )

    return validator


def create_container_request_validator(
    *,
    allow_volumes: bool = False,
    allowed_gpu_models: Optional[Sequence[str]] = None,
    allow_any_tpu: bool = False,
    allowed_tpu_resources: Sequence[TPUResource] = (),
) -> t.Trafaret:
    return create_container_validator(
        allow_volumes=allow_volumes,
        allowed_gpu_models=allowed_gpu_models,
        allow_any_tpu=allow_any_tpu,
        allowed_tpu_resources=allowed_tpu_resources,
    )


def create_container_response_validator() -> t.Trafaret:
    return create_container_validator(
        allow_volumes=True,
        allow_any_gpu_models=True,
        allow_any_tpu=True,
        allow_any_command=True,
    )


def sanitize_dns_name(value: str) -> Optional[str]:
    """ This is a TEMPORARY METHOD used to sanitize DNS names so that they are parseable
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


def create_job_tag_validator() -> t.Trafaret:
    return t.String(min_length=1, max_length=256) & t.Regexp(JOB_TAG_PATTERN)


def create_tag_list_per_job_validator() -> t.Trafaret:
    return t.List(create_job_tag_validator(), max_length=256)


def create_tag_list_per_user_validator() -> t.Trafaret:
    return t.List(t.String)
