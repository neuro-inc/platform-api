from typing import Any, Dict, List, Optional

from platform_api.config import StorageConfig
from platform_api.orchestrator.job_request import (
    Container, ContainerResources, ContainerVolume
)


# TODO: ContainerHTTPServer


class ContainerBuilder:
    def __init__(self) -> None:
        self._image: str = ''
        self._command: Optional[str] = None
        self._env: Dict[str, str] = {}
        self._resources: Optional[ContainerResources] = None
        self._volumes: List[ContainerVolume] = []
        self._port: Optional[int] = None
        self._health_check_path: Optional[str] = None

    def set_image(self, image: str) -> 'ContainerBuilder':
        self._image = image
        return self

    def set_command(self, command: str) -> 'ContainerBuilder':
        self._command = command
        return self

    def add_env(self, name: str, value: str) -> 'ContainerBuilder':
        return self.update_env({name: value})

    def update_env(self, env: Dict[str, str]) -> 'ContainerBuilder':
        self._env.update(env)
        return self

    def add_volume(self, volume: ContainerVolume) -> 'ContainerBuilder':
        self._volumes.append(volume)
        return self

    def set_resources(
            self, resources: ContainerResources) -> 'ContainerBuilder':
        self._resources = resources
        return self

    def set_port_and_health_check(
            self, port: int, path: str) -> 'ContainerBuilder':
        self._port = port
        self._health_check_path = path
        return self

    @classmethod
    def from_container_payload(
            cls, payload: Dict[str, Any]) -> 'ContainerBuilder':
        builder = cls()
        builder.set_image(payload['image'])
        if 'command' in payload:
            builder.set_command(payload['command'])
        builder.update_env(payload.get('env', {}))

        builder.set_resources(
            cls.create_volume_from_payload(payload['resources']))

        http = payload.get('http', {})
        if 'port' in http:
            builder.set_port_and_health_check(
                port=http['port'],
                path=http.get('health_check_path', Container.health_check_path)
            )

        return builder

    @classmethod
    def create_volume_from_payload(
            cls, payload: Dict[str, Any]) -> ContainerVolume:
        return ContainerResources(  # type: ignore
            cpu=payload['cpu'],
            memory_mb=payload['memory_mb'],
            gpu=payload.get('gpu'),
        )

    def build(self) -> Container:
        # TODO: validate attrs
        # TODO: make sure all the attrs are immutable
        return Container(  # type: ignore
            image=self._image,
            command=self._command,
            env=self._env,
            volumes=self._volumes,
            resources=self._resources,
            port=self._port,
            health_check_path=self._health_check_path,
        )


class ModelRequest:
    def __init__(
            self, payload, *, storage_config: StorageConfig,
            env_prefix: str = '') -> None:
        self._payload = payload

        self._env_prefix = env_prefix
        self._storage_config = storage_config

        self._dataset_env_var_name = self._create_env_var_name('DATASET_PATH')
        self._result_env_var_name = self._create_env_var_name('RESULT_PATH')

        self._dataset_volume = self._create_dataset_volume()
        self._result_volume = self._create_result_volume()

    def _create_dataset_volume(self) -> ContainerVolume:
        return ContainerVolume.create(
            self._payload['dataset_storage_uri'],
            src_mount_path=self._storage_config.host_mount_path,
            dst_mount_path=self._storage_config.container_mount_path,
            read_only=True,
            scheme=self._storage_config.uri_scheme,
        )

    def _create_result_volume(self) -> ContainerVolume:
        return ContainerVolume.create(
            self._payload['result_storage_uri'],
            src_mount_path=self._storage_config.host_mount_path,
            dst_mount_path=self._storage_config.container_mount_path,
            read_only=False,
            scheme=self._storage_config.uri_scheme,
        )

    def _create_env_var_name(self, name):
        if self._env_prefix:
            return f'{self._env_prefix}_{name}'
        return name

    def to_container(self) -> Container:
        builder = ContainerBuilder.from_container_payload(
            self._payload['container'])
        builder.add_volume(self._dataset_volume)
        builder.add_env(
            self._dataset_env_var_name, str(self._dataset_volume.dst_path))
        builder.add_volume(self._result_volume)
        builder.add_env(
            self._result_env_var_name, str(self._result_volume.dst_path))
        return builder.build()
