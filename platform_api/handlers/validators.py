import trafaret as t


def create_container_request_validator() -> t.Trafaret:
    return t.Dict({
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
