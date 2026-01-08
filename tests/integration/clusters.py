import asyncio
import logging
import tempfile
from asyncio import timeout
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
import yaml
from apolo_kube_client import (
    KubeClient,
    KubeConfig,
    ResourceNotFound,
)
from apolo_kube_client.apolo import create_namespace

logger = logging.getLogger(__name__)

VCLUSTER_NAME = "vcluster"
VCLUSTER_SECRET_NAME = "vc-vcluster"
VCLUSTER_CREATION_TIMEOUT_S = 600
PORT_FORWARD_START_TIMEOUT_S = 600
VCLUSTER_SECRET_POLL_INTERVAL_S = 5


@pytest.fixture(
    scope="session",
    params=[
        "minikube",
        # "vcluster",
    ],
)
async def _org_project(
    request: pytest.FixtureRequest,
    kube_config: KubeConfig,
) -> AsyncIterator[tuple[str, str, bool]]:
    """
    Returns an org name, a project name, and a flag if this is a vcluster or not
    Creates a dedicated namespace.
    This fixture is parametrized with two possible scenarios of running:
    - on a plain minikube:
        just yields a random names
    - on a vcluster created inside a minikube:
        performs a setup of a vcluster
        starts a vcluster port-forwarding, so it becomes reachable from localhost
        patches the kubeconfig
        yields org proj
    """
    is_vcluster: bool = request.param == "vcluster"
    org_name, project_name = uuid4().hex, uuid4().hex
    if is_vcluster:
        org_name = f"vcluster-{org_name}"
        project_name = f"vcluster-{project_name}"

    async with KubeClient(config=kube_config) as kube_client:
        namespace = await create_namespace(kube_client, org_name, project_name)
        assert namespace.metadata.name
        try:
            if not is_vcluster:
                yield org_name, project_name, is_vcluster
                return

            async with _vcluster_environment(
                kube_client=kube_client,
                namespace=namespace.metadata.name,
                org_name=org_name,
                project_name=project_name,
            ) as org_project:
                org_name, project_name = org_project
                yield org_name, project_name, is_vcluster

        finally:
            await _delete_namespace(kube_client, namespace.metadata.name)


@pytest.fixture
async def org_project(
    _org_project: tuple[str, str, bool],
) -> AsyncIterator[tuple[str, str]]:
    org_name, project_name, is_vcluster = _org_project
    yield org_name, project_name
    if is_vcluster:
        # add a small sleep to give a chance for vcluster to sync back and force
        await asyncio.sleep(1)


@pytest.fixture
def org_name(org_project: tuple[str, str]) -> str:
    """Get org_name from org_project fixture."""
    org, _ = org_project
    return org


@pytest.fixture
def project_name(org_project: tuple[str, str]) -> str:
    """Get project_name from org_project fixture."""
    _, project = org_project
    return project


@pytest.fixture
def is_vcluster(_org_project: tuple[str, str, bool]) -> bool:
    """Check if the current test is running on vcluster."""
    _, _, is_vcluster = _org_project
    return is_vcluster


async def _delete_namespace(
    kube_client: KubeClient,
    namespace_name: str,
) -> None:
    await kube_client.core_v1.namespace.delete(name=namespace_name)
    async with timeout(300):
        while True:
            try:
                await kube_client.core_v1.namespace.get(name=namespace_name)
            except ResourceNotFound:
                break
            await asyncio.sleep(1)


@asynccontextmanager
async def _vcluster_environment(
    kube_client: KubeClient,
    namespace: str,
    org_name: str,
    project_name: str,
) -> AsyncIterator[tuple[str, str]]:
    local_port = 8444
    with _make_connectable_from_local(
        local_port=local_port,
    ):
        await _create_vcluster(namespace)
        await _wait_vcluster_ready(kube_client, namespace)
        remote_port = await _get_vcluster_service_port(kube_client, namespace)
        port_forward_proc, port_forward_drain_task = await _start_port_forward(
            namespace,
            local_port=local_port,
            remote_port=remote_port,
        )
        try:
            yield org_name, project_name
        finally:
            await _stop_port_forward(port_forward_proc, port_forward_drain_task)
            await _delete_vcluster(namespace)


@contextmanager
def _make_connectable_from_local(local_port: int) -> Iterator[None]:
    """
    Patches a kube selector and ensures that the test-suite
    will be able to connect to a vcluster from localhost
    """

    # new YAML callable that is used by a kube client selector,
    # and will point to a localhost
    def safe_load(raw: bytes) -> dict[str, Any]:
        value = yaml.safe_load(raw)
        value["clusters"][0]["cluster"]["server"] = f"https://127.0.0.1:{local_port}"
        return value

    with patch("apolo_kube_client._vcluster._client_factory.yaml") as yaml_patch:
        yaml_patch.safe_load = safe_load
        yield


async def _create_vcluster(namespace_name: str) -> None:
    """
    Create a vcluster in the given namespace using a temporary config file.
    """
    in_kube_host = f"{VCLUSTER_NAME}.{namespace_name}.svc.cluster.local"
    config = {
        # ensure in-kube services can connect to a vcluster from within a host cluster
        "controlPlane": {"proxy": {"extraSANs": [in_kube_host]}},
        "exportKubeConfig": {
            "server": f"https://{in_kube_host}:443",
            "secret": {"name": VCLUSTER_SECRET_NAME},
        },
        "sync": {
            "fromHost": {
                "ingressClasses": {
                    "enabled": True,
                    "selector": {
                        "matchExpressions": [
                            {
                                "key": "app.kubernetes.io/name",
                                "operator": "In",
                                "values": ["traefik"],
                            }
                        ]
                    },
                }
            },
            "toHost": {
                "secrets": {
                    "enabled": True,
                    "all": True,
                },
                "ingresses": {
                    "enabled": True,
                },
            },
        },
    }

    tmp_config_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            yaml.safe_dump(config, tmp)
            tmp_config_path = Path(tmp.name)

        create_cmd = [
            "vcluster",
            "create",
            VCLUSTER_NAME,
            "-n",
            namespace_name,
            "--connect=false",
            "-f",
            str(tmp_config_path),
        ]

        process = await asyncio.create_subprocess_exec(
            *create_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            err = (
                "Failed to create vcluster "
                f"(namespace={namespace_name}, exit_code={process.returncode}). "
                f"stdout: {stdout.decode()}\nstderr: {stderr.decode()}"
            )
            raise RuntimeError(err)
    finally:
        if tmp_config_path is not None:
            try:
                tmp_config_path.unlink()
            except FileNotFoundError:
                pass


async def _wait_vcluster_ready(
    kube_client: KubeClient,
    namespace_name: str,
) -> None:
    vcluster_pod_name = "vcluster-0"
    async with timeout(VCLUSTER_CREATION_TIMEOUT_S):
        while True:
            try:
                pod = await kube_client.core_v1.pod.get(
                    name=vcluster_pod_name,
                    namespace=namespace_name,
                )
            except ResourceNotFound:
                pass
            else:
                # Ensure that the pod is running and all containers
                # are fully ready (no waiting / terminating states).
                if (pod.status.phase or "") == "Running":
                    container_statuses = pod.status.container_statuses
                    if container_statuses and all(
                        status.ready
                        and status.state.waiting is None
                        and status.state.terminated is None
                        for status in container_statuses
                    ):
                        return

            await asyncio.sleep(VCLUSTER_SECRET_POLL_INTERVAL_S)


async def _get_vcluster_service_port(
    kube_client: KubeClient,
    namespace_name: str,
) -> int:
    service = await kube_client.core_v1.service.get(
        name=VCLUSTER_NAME,
        namespace=namespace_name,
    )
    ports = (service.spec.ports or []) if service.spec is not None else []
    if not ports or ports[0].port is None:
        raise RuntimeError("vcluster service has no ports configured")
    return int(ports[0].port)


async def _start_port_forward(
    namespace_name: str,
    local_port: int,
    remote_port: int,
) -> tuple[asyncio.subprocess.Process, asyncio.Task[None]]:
    port_forward_cmd = [
        "kubectl",
        "port-forward",
        f"svc/{VCLUSTER_NAME}",
        f"{local_port}:{remote_port}",
        "-n",
        namespace_name,
    ]
    process = await asyncio.create_subprocess_exec(
        *port_forward_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert process.stdout

    try:
        first_line = await asyncio.wait_for(
            process.stdout.readline(),
            timeout=PORT_FORWARD_START_TIMEOUT_S,
        )
    except TimeoutError:
        process.terminate()
        await process.wait()
        raise RuntimeError("port-forward timeout") from None

    if process.returncode not in (None, 0):
        output = first_line.decode()
        extra = await process.stdout.read()
        output += extra.decode()
        err = (
            "kubectl port-forward failed to start "
            f"(exit_code={process.returncode}). Output: {output}"
        )
        raise RuntimeError(err)

    drain_task = asyncio.create_task(_drain_output(process.stdout))
    return process, drain_task


async def _drain_output(stream: asyncio.StreamReader) -> None:
    while True:
        line = await stream.readline()
        if not line:
            break


async def _stop_port_forward(
    process: asyncio.subprocess.Process | None,
    drain_task: asyncio.Task[None] | None,
) -> None:
    if process is not None:
        process.terminate()
        try:
            await process.wait()
        except ProcessLookupError:
            pass
    if drain_task is not None:
        drain_task.cancel()


async def _delete_vcluster(namespace_name: str) -> None:
    delete_cmd = [
        "vcluster",
        "delete",
        VCLUSTER_NAME,
        "-n",
        namespace_name,
        "--auto-delete-namespace=false",
        "--delete-context=false",
    ]
    delete_proc = await asyncio.create_subprocess_exec(
        *delete_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await delete_proc.communicate()
