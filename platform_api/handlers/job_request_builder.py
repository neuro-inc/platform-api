from pathlib import PurePath
from typing import Any, Dict, List, Optional

from platform_api.config import StorageConfig
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerVolume,
)


class ContainerBuilder:
    def __init__(self, *, storage_config: StorageConfig) -> None:
        self._storage_config = storage_config

        self._image: str = ""
        self._command: Optional[str] = None
        self._env: Dict[str, str] = {}
        self._resources: Optional[ContainerResources] = None
        self._volumes: List[ContainerVolume] = []
        self._http_server: Optional[ContainerHTTPServer] = None

    def set_image(self, image: str) -> "ContainerBuilder":
        self._image = image
        return self

    def set_command(self, command: str) -> "ContainerBuilder":
        self._command = command
        return self

    def add_env(self, name: str, value: str) -> "ContainerBuilder":
        return self.update_env({name: value})

    def update_env(self, env: Dict[str, str]) -> "ContainerBuilder":
        self._env.update(env)
        return self

    def add_volume(self, volume: ContainerVolume) -> "ContainerBuilder":
        self._volumes.append(volume)
        return self

    def set_resources(self, resources: ContainerResources) -> "ContainerBuilder":
        self._resources = resources
        return self

    def set_http_server(self, http_server: ContainerHTTPServer) -> "ContainerBuilder":
        self._http_server = http_server
        return self

    @classmethod
    def from_container_payload(
        cls, payload: Dict[str, Any], *, storage_config: StorageConfig
    ) -> "ContainerBuilder":
        builder = cls(storage_config=storage_config)
        builder.set_image(payload["image"])
        if "command" in payload:
            builder.set_command(payload["command"])
        builder.update_env(payload.get("env", {}))

        builder.set_resources(cls.create_resources_from_payload(payload["resources"]))

        http = payload.get("http", {})
        if "port" in http:
            http_server = ContainerHTTPServer(
                port=http["port"],
                health_check_path=http.get(
                    "health_check_path", ContainerHTTPServer.health_check_path
                ),
            )
            builder.set_http_server(http_server)

        for volume_payload in payload.get("volumes", []):
            volume = cls.create_volume_from_payload(
                volume_payload, storage_config=storage_config
            )
            builder.add_volume(volume)

        return builder

    @classmethod
    def create_resources_from_payload(
        cls, payload: Dict[str, Any]
    ) -> ContainerResources:
        return ContainerResources(  # type: ignore
            cpu=payload["cpu"],
            memory_mb=payload["memory_mb"],
            gpu=payload.get("gpu"),
            shm=payload.get("shm"),
        )

    @classmethod
    def create_volume_from_payload(
        cls, payload: Dict[str, Any], *, storage_config: StorageConfig
    ) -> ContainerVolume:
        dst_path = PurePath(payload["dst_path"])
        return ContainerVolume.create(
            payload["src_storage_uri"],
            src_mount_path=storage_config.host_mount_path,
            dst_mount_path=dst_path,
            extend_dst_mount_path=False,
            read_only=bool(payload.get("read_only")),
            scheme=storage_config.uri_scheme,
        )

    def build(self) -> Container:
        return Container(  # type: ignore
            image=self._image,
            command=self._command,
            env=self._env,
            volumes=self._volumes,
            resources=self._resources,
            http_server=self._http_server,
        )


class ModelRequest:
    def __init__(
        self, payload, *, storage_config: StorageConfig, env_prefix: str = ""
    ) -> None:
        self._payload = payload

        self._env_prefix = env_prefix
        self._storage_config = storage_config

        self._dataset_env_var_name = self._create_env_var_name("DATASET_PATH")
        self._result_env_var_name = self._create_env_var_name("RESULT_PATH")

        self._dataset_volume = self._create_dataset_volume()
        self._result_volume = self._create_result_volume()

    def _create_dataset_volume(self) -> ContainerVolume:
        return ContainerVolume.create(
            self._payload["dataset_storage_uri"],
            src_mount_path=self._storage_config.host_mount_path,
            dst_mount_path=self._storage_config.container_mount_path,
            read_only=True,
            scheme=self._storage_config.uri_scheme,
        )

    def _create_result_volume(self) -> ContainerVolume:
        return ContainerVolume.create(
            self._payload["result_storage_uri"],
            src_mount_path=self._storage_config.host_mount_path,
            dst_mount_path=self._storage_config.container_mount_path,
            read_only=False,
            scheme=self._storage_config.uri_scheme,
        )

    def _create_env_var_name(self, name):
        if self._env_prefix:
            return f"{self._env_prefix}_{name}"
        return name

    def to_container(self) -> Container:
        builder = ContainerBuilder.from_container_payload(
            self._payload["container"], storage_config=self._storage_config
        )
        builder.add_volume(self._dataset_volume)
        builder.add_env(self._dataset_env_var_name, str(self._dataset_volume.dst_path))
        builder.add_volume(self._result_volume)
        builder.add_env(self._result_env_var_name, str(self._result_volume.dst_path))
        return builder.build()
