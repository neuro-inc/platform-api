import trafaret as t


def create_volume_request_validator() -> t.Trafaret:
    return t.Dict({
        'src_storage_uri': t.String,
        'dst_path': t.String,
        t.Key('read_only', optional=True): t.Bool(default=False),
    })


def create_container_request_validator(
        allow_volumes: bool = False) -> t.Trafaret:
    """Create a validator for primitive container objects.

    Meant to be used in high-level resources such as jobs, models, batch
    inference etc.
    """
    validator = t.Dict({
        'image': t.String,
        t.Key('command', optional=True): t.String,
        t.Key('env', optional=True): t.Mapping(
            t.String, t.String(allow_blank=True)),
        'resources': t.Dict({
            'cpu': t.Float(gte=0.1),
            'memory_mb': t.Int(gte=16),
            t.Key('gpu', optional=True): t.Int(gte=1),
        }),
        t.Key('http', optional=True): t.Dict({
            'port': t.Int(gte=0, lte=65535),
            t.Key('health_check_path', optional=True): t.String,
        }),
    })

    if allow_volumes:
        volume_validator = create_volume_request_validator()
        validator += t.Dict({
            t.Key('volumes', optional=True): t.List(volume_validator),
        })

    return validator
