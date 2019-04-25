from typing import Any, Dict, Optional, Sequence, Set

import trafaret as t

from platform_api.orchestrator.job_request import JobStatus
from platform_api.resource import GPUModel


JOB_NAME_PATTERN = "^[a-z][-a-z0-9]*[a-z0-9]$"
USER_NAME_PATTERN = "^[a-z0-9]([a-z0-9]|(-[a-z0-9]))*$"

OptionalString = t.String | t.Null


def create_job_name_validator() -> t.Trafaret:
    return t.Null | t.String(min_length=3, max_length=35) & t.Regexp(JOB_NAME_PATTERN)


def create_user_name_validator() -> t.Trafaret:
    """ Completely the same validator as the one used in platform-auth
    """
    return t.String(min_length=3, max_length=25) & t.Regexp(USER_NAME_PATTERN)


def create_job_status_validator() -> t.Trafaret:
    return t.Enum(*JobStatus.values())


def create_job_history_validator() -> t.Trafaret:
    return t.Dict(
        {
            "status": create_job_status_validator(),
            "reason": OptionalString,
            "description": OptionalString,
            "created_at": t.String,
            t.Key("started_at", optional=True): t.String,
            t.Key("finished_at", optional=True): t.String,
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
    allowed_gpu_models: Optional[Sequence[GPUModel]] = None,
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
    gpu_validator = t.Int(gte=0, lte=MAX_GPU_COUNT)
    if allow_any_gpu_models:
        gpu_model_validator = t.String
    else:
        gpu_model_validator = t.Enum(
            *(gpu_model.id for gpu_model in allowed_gpu_models or [])
        )

    resources_gpu_validator = common_resources_validator + t.Dict(
        {t.Key("gpu", optional=True): gpu_validator}
    )
    resources_gpu_model_validator = common_resources_validator + t.Dict(
        {"gpu": gpu_validator, t.Key("gpu_model", optional=True): gpu_model_validator}
    )

    return resources_gpu_validator | resources_gpu_model_validator


def create_container_validator(
    *,
    allow_volumes: bool = False,
    allow_any_gpu_models: bool = False,
    allowed_gpu_models: Optional[Sequence[GPUModel]] = None,
) -> t.Trafaret:
    """Create a validator for primitive container objects.

    Meant to be used in high-level resources such as jobs, models, batch
    inference etc.
    """

    validator = t.Dict(
        {
            "image": t.String,
            t.Key("command", optional=True): t.String,
            t.Key("env", optional=True): t.Mapping(
                t.String, t.String(allow_blank=True)
            ),
            "resources": create_resources_validator(
                allow_any_gpu_models=allow_any_gpu_models,
                allowed_gpu_models=allowed_gpu_models,
            ),
            t.Key("http", optional=True): t.Dict(
                {
                    "port": t.Int(gte=0, lte=65535),
                    t.Key("health_check_path", optional=True): t.String,
                    t.Key("requires_auth", optional=True, default=False): t.Bool,
                }
            ),
            t.Key("ssh", optional=True): t.Dict({"port": t.Int(gte=0, lte=65535)}),
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
    allowed_gpu_models: Optional[Sequence[GPUModel]] = None,
) -> t.Trafaret:
    return create_container_validator(
        allow_volumes=allow_volumes, allowed_gpu_models=allowed_gpu_models
    )


def create_container_response_validator() -> t.Trafaret:
    return create_container_validator(allow_volumes=True, allow_any_gpu_models=True)
