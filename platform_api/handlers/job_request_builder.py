from pathlib import PurePath
from typing import Any, Dict, List, Optional

from platform_api.config import StorageConfig
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerSSHServer,
    ContainerTPUResource,
    ContainerVolume,
)


class ContainerBuilder:
    def __init__(self, *, storage_config: StorageConfig) -> None:
        self._storage_config = storage_config

        self._image: str = ""
        self._entrypoint: Optional[str] = None
        self._command: Optional[str] = None
        self._env: Dict[str, str] = {}
        self._resources: Optional[ContainerResources] = None
        self._volumes: List[ContainerVolume] = []
        self._http_server: Optional[ContainerHTTPServer] = None
        self._ssh_server: Optional[ContainerSSHServer] = None

    def set_image(self, image: str) -> "ContainerBuilder":
        self._image = image
        return self

    def set_entrypoint(self, entrypoint: str) -> "ContainerBuilder":
        self._entrypoint = entrypoint
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

    def set_ssh_server(self, ssh_server: ContainerSSHServer) -> "ContainerBuilder":
        self._ssh_server = ssh_server
        return self

    @classmethod
    def from_container_payload(
        cls, payload: Dict[str, Any], *, storage_config: StorageConfig
    ) -> "ContainerBuilder":
        builder = cls(storage_config=storage_config)
        builder.set_image(payload["image"])
        if "entrypoint" in payload:
            builder.set_entrypoint(payload["entrypoint"])
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
                requires_auth=http.get(
                    "requires_auth", ContainerHTTPServer.requires_auth
                ),
            )
            builder.set_http_server(http_server)

        ssh = payload.get("ssh", {})
        if "port" in ssh:
            ssh_server = ContainerSSHServer(ssh["port"])
            builder.set_ssh_server(ssh_server)

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
        tpu = None
        if payload.get("tpu"):
            tpu = cls.create_tpu_resource_from_payload(payload["tpu"])
        return ContainerResources(
            cpu=payload["cpu"],
            memory_mb=payload["memory_mb"],
            gpu=payload.get("gpu"),
            gpu_model_id=payload.get("gpu_model"),
            shm=payload.get("shm"),
            tpu=tpu,
        )

    @classmethod
    def create_tpu_resource_from_payload(
        self, payload: Dict[str, Any]
    ) -> ContainerTPUResource:
        return ContainerTPUResource(
            type=payload["type"], software_version=payload["software_version"]
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
        return Container(  # type: ignore  # noqa
            image=self._image,
            entrypoint=self._entrypoint,
            command=self._command,
            env=self._env,
            volumes=self._volumes,
            resources=self._resources,
            http_server=self._http_server,
            ssh_server=self._ssh_server,
        )
