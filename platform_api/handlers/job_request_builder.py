from pathlib import PurePath
from typing import Any, Dict

from platform_api.cluster_config import StorageConfig
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerSSHServer,
    ContainerTPUResource,
    ContainerVolume,
)


def create_container_from_payload(
    payload: Dict[str, Any], *, storage_config: StorageConfig, cluster_name: str
) -> Container:
    http_server = None
    http = payload.get("http", {})
    if "port" in http:
        http_server = ContainerHTTPServer(
            port=http["port"],
            health_check_path=http.get(
                "health_check_path", ContainerHTTPServer.health_check_path
            ),
            requires_auth=http.get("requires_auth", ContainerHTTPServer.requires_auth),
        )

    ssh_server = None
    ssh = payload.get("ssh", {})
    if "port" in ssh:
        ssh_server = ContainerSSHServer(ssh["port"])

    volumes = []
    for volume_payload in payload.get("volumes", ()):
        volume = create_volume_from_payload(
            volume_payload, storage_config=storage_config, cluster_name=cluster_name
        )
        volumes.append(volume)

    return Container(
        image=payload["image"],
        entrypoint=payload.get("entrypoint"),
        command=payload.get("command"),
        env=payload.get("env", {}),
        volumes=volumes,
        resources=create_resources_from_payload(payload["resources"]),
        http_server=http_server,
        ssh_server=ssh_server,
        tty=payload.get("tty", False),
    )


def create_resources_from_payload(payload: Dict[str, Any]) -> ContainerResources:
    tpu = None
    if "tpu" in payload:
        tpu = create_tpu_resource_from_payload(payload["tpu"])
    return ContainerResources(
        cpu=payload["cpu"],
        memory_mb=payload["memory_mb"],
        gpu=payload.get("gpu"),
        gpu_model_id=payload.get("gpu_model"),
        shm=payload.get("shm"),
        tpu=tpu,
    )


def create_tpu_resource_from_payload(payload: Dict[str, Any]) -> ContainerTPUResource:
    return ContainerTPUResource(
        type=payload["type"], software_version=payload["software_version"]
    )


def create_volume_from_payload(
    payload: Dict[str, Any], *, storage_config: StorageConfig, cluster_name: str
) -> ContainerVolume:
    dst_path = PurePath(payload["dst_path"])
    return ContainerVolume.create(
        payload["src_storage_uri"],
        src_mount_path=storage_config.host_mount_path,
        dst_mount_path=dst_path,
        extend_dst_mount_path=False,
        read_only=bool(payload.get("read_only")),
        scheme=storage_config.uri_scheme,
        cluster_name=cluster_name,
    )
