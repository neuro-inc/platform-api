import trafaret as t

from platform_api.orchestrator.job_request import JobStatus


OptionalString = t.String | t.Null


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


def create_volume_request_validator() -> t.Trafaret:
    return t.Dict(
        {
            "src_storage_uri": t.String,
            "dst_path": t.String,
            t.Key("read_only", optional=True, default=True): t.Bool(),
        }
    )


def create_container_request_validator(*, allow_volumes: bool = False) -> t.Trafaret:
    """Create a validator for primitive container objects.

    Meant to be used in high-level resources such as jobs, models, batch
    inference etc.
    """

    MAX_GPU_COUNT = 128
    MAX_CPU_COUNT = 128.0

    validator = t.Dict(
        {
            "image": t.String,
            t.Key("command", optional=True): t.String,
            t.Key("env", optional=True): t.Mapping(
                t.String, t.String(allow_blank=True)
            ),
            "resources": t.Dict(
                {
                    "cpu": t.Float(gte=0.1, lte=MAX_CPU_COUNT),
                    "memory_mb": t.Int(gte=16),
                    t.Key("gpu", optional=True): t.Int(gte=0, lte=MAX_GPU_COUNT),
                    t.Key("shm", optional=True): t.Bool,
                }
            ),
            t.Key("http", optional=True): t.Dict(
                {
                    "port": t.Int(gte=0, lte=65535),
                    t.Key("health_check_path", optional=True): t.String,
                }
            ),
        }
    )

    if allow_volumes:
        volume_validator = create_volume_request_validator()
        validator += t.Dict({t.Key("volumes", optional=True): t.List(volume_validator)})

    return validator
