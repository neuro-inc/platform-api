from pathlib import PurePath
from typing import Any

from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerTPUResource,
    ContainerVolume,
    DiskContainerVolume,
    Secret,
    SecretContainerVolume,
)


def create_container_from_payload(payload: dict[str, Any]) -> Container:
    if "container" in payload:
        # Deprecated. Use flat structure
        payload = payload["container"]
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

    volumes = []
    for volume_payload in payload.get("volumes", ()):
        volume = create_volume_from_payload(volume_payload)
        volumes.append(volume)

    secret_volumes = [
        create_secret_volume_from_payload(volume_payload)
        for volume_payload in payload.get("secret_volumes", ())
    ]
    disk_volumes = [
        create_disk_volume_from_payload(volume_payload)
        for volume_payload in payload.get("disk_volumes", ())
    ]
    secret_env = {
        env_var: Secret.create(value)
        for env_var, value in payload.get("secret_env", {}).items()
    }

    return Container(
        image=payload["image"],
        entrypoint=payload.get("entrypoint"),
        command=payload.get("command"),
        env=payload.get("env", {}),
        volumes=volumes,
        secret_env=secret_env,
        secret_volumes=secret_volumes,
        disk_volumes=disk_volumes,
        resources=create_resources_from_payload(payload["resources"]),
        http_server=http_server,
        tty=payload.get("tty", False),
        working_dir=payload.get("working_dir"),
    )


def create_resources_from_payload(payload: dict[str, Any]) -> ContainerResources:
    tpu = None
    if "tpu" in payload:
        tpu = create_tpu_resource_from_payload(payload["tpu"])
    return ContainerResources(
        cpu=payload["cpu"],
        memory=(
            payload["memory"] if "memory" in payload else payload["memory_mb"] * 2**20
        ),
        nvidia_gpu=payload.get("nvidia_gpu"),
        amd_gpu=payload.get("amd_gpu"),
        intel_gpu=payload.get("intel_gpu"),
        nvidia_gpu_model=payload.get("nvidia_gpu_model") or payload.get("gpu_model"),
        amd_gpu_model=payload.get("amd_gpu_model"),
        intel_gpu_model=payload.get("intel_gpu_model"),
        shm=payload.get("shm"),
        tpu=tpu,
    )


def create_tpu_resource_from_payload(payload: dict[str, Any]) -> ContainerTPUResource:
    return ContainerTPUResource(
        type=payload["type"], software_version=payload["software_version"]
    )


def create_volume_from_payload(payload: dict[str, Any]) -> ContainerVolume:
    dst_path = PurePath(payload["dst_path"])
    return ContainerVolume.create(
        payload["src_storage_uri"],
        dst_path=dst_path,
        read_only=bool(payload.get("read_only")),
    )


def create_secret_volume_from_payload(payload: dict[str, Any]) -> SecretContainerVolume:
    return SecretContainerVolume.create(
        uri=payload["src_secret_uri"], dst_path=PurePath(payload["dst_path"])
    )


def create_disk_volume_from_payload(payload: dict[str, Any]) -> DiskContainerVolume:
    return DiskContainerVolume.create(
        uri=payload["src_disk_uri"],
        dst_path=PurePath(payload["dst_path"]),
        read_only=payload["read_only"],
    )
