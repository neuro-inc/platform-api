from __future__ import annotations

import asyncio
import itertools
import json
import os
import shlex
import sys
import time
import uuid
from asyncio import timeout
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from operator import attrgetter
from pathlib import PurePath
from typing import Any, cast
from unittest import mock
from unittest.mock import PropertyMock, patch

import aiohttp
import pytest
from aiohttp import web
from apolo_kube_client import (
    CoreV1Event,
    KubeClientException,
    KubeClientProxy,
    KubeClientSelector,
    ResourceNotFound,
    V1EventSource,
    V1Node,
    V1NodeCondition,
    V1NodeSpec,
    V1NodeStatus,
    V1ObjectMeta,
    V1ObjectReference,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1Pod,
    V1Secret,
    V1VolumeResourceRequirements,
)
from neuro_config_client import (
    NvidiaGPU,
    OrchestratorConfig,
    ResourcePoolType,
    ResourcePreset,
    TPUResource,
)
from yarl import URL

from platform_api.config import (
    STORAGE_URI_SCHEME,
    RegistryConfig,
)
from platform_api.old_kube_client.errors import (
    ResourceNotFound as LegacyResourceNotFound,
)
from platform_api.orchestrator.job import (
    DEFAULT_ORPHANED_JOB_OWNER,
    Job,
    JobRecord,
    JobRestartPolicy,
    JobStatusItem,
    JobStatusReason,
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerVolume,
    Disk,
    DiskContainerVolume,
    JobAlreadyExistsException,
    JobError,
    JobNotFoundException,
    JobRequest,
    JobStatus,
    JobUnschedulableException,
    Secret,
    SecretContainerVolume,
)
from platform_api.orchestrator.kube_client import (
    Ingress,
    IngressRule,
    KubeClient,
    NodeResources,
    NodeTaint,
    NodeWatcher,
    PodDescriptor,
    PodWatcher,
    SecretRef,
    Service,
    Toleration,
    create_ingress,
    create_pod,
    create_service,
    delete_ingress,
    delete_pod,
    delete_service,
    get_ingress,
    get_pod,
    get_pod_status,
    get_service,
    list_services,
    wait_pod_is_deleted,
    wait_pod_is_finished,
    wait_pod_is_running,
    wait_pod_is_terminated,
)
from platform_api.orchestrator.kube_config import KubeConfig
from platform_api.orchestrator.kube_orchestrator import (
    INJECT_STORAGE_KEY,
    JobStatusItemFactory,
    KubeOrchestrator,
)
from tests.conftest import random_str
from tests.integration.conftest import (
    ApiAddress,
    ApiRunner,
    MyKubeClient,
    create_local_app_server,
)
from tests.integration.test_api import ApiConfig


class MyJob(Job):
    def __init__(
        self, orchestrator: KubeOrchestrator, *, record: JobRecord, **kwargs: Any
    ) -> None:
        self._orchestrator = orchestrator
        if not record.owner:
            record = replace(record, owner="test-owner")
        super().__init__(
            orchestrator_config=orchestrator.orchestrator_config,
            record=record,
        )

    async def start(self) -> JobStatus:
        status = await self._orchestrator.start_job(self)
        assert status == JobStatus.PENDING
        return status

    async def delete(self) -> JobStatus:
        return await self._orchestrator.delete_job(self)

    async def query_status(self) -> JobStatus:
        return (await self._orchestrator.get_job_status(self)).status


@pytest.fixture
def cluster_name() -> str:
    return "test-cluster"


@pytest.fixture
async def job_nginx(kube_orchestrator: KubeOrchestrator) -> MyJob:
    container = Container(
        image="ubuntu:20.10",
        command="sleep 5",
        resources=ContainerResources(cpu=0.1, memory=256 * 10**6),
    )
    job_request = JobRequest.create(container)
    return MyJob(
        orchestrator=kube_orchestrator,
        record=JobRecord.create(
            request=job_request,
            cluster_name="test-cluster",
            org_name="test-org",
        ),
    )


@pytest.fixture
async def delete_job_later(
    kube_orchestrator: KubeOrchestrator,
) -> AsyncIterator[Callable[[Job], Awaitable[None]]]:
    jobs = []

    async def _add_job(job: Job) -> None:
        jobs.append(job)

    yield _add_job

    for job in jobs:
        try:
            await kube_orchestrator.delete_job(job)
        except Exception:
            pass


class TestKubeOrchestrator:
    @pytest.fixture
    def org_name(self) -> str:
        return "org"

    @pytest.fixture
    def project_name(self) -> str:
        return "project"

    async def _wait_for(
        self,
        job: MyJob,
        interval_s: float = 0.5,
        max_time: float = 180,
        *,
        status_predicate: Callable[[JobStatus], bool],
    ) -> JobStatus:
        t0 = time.monotonic()
        while True:
            status = await job.query_status()
            if status_predicate(status):
                return status

            await asyncio.sleep(max(interval_s, time.monotonic() - t0))
            current_time = time.monotonic() - t0
            if current_time > max_time:
                pytest.fail(f"too long: {current_time:.3f} sec")
            await asyncio.sleep(interval_s)
            interval_s *= 1.5

    async def wait_for_completion(self, *args: Any, **kwargs: Any) -> JobStatus:
        def _predicate(status: JobStatus) -> bool:
            return status.is_finished

        return await self._wait_for(*args, status_predicate=_predicate, **kwargs)

    async def wait_for_failure(self, *args: Any, **kwargs: Any) -> None:
        def _predicate(status: JobStatus) -> bool:
            if not status.is_finished:
                return False
            assert status == JobStatus.FAILED
            return True

        await self._wait_for(*args, status_predicate=_predicate, **kwargs)

    async def wait_for_success(self, *args: Any, **kwargs: Any) -> None:
        def _predicate(status: JobStatus) -> bool:
            if not status.is_finished:
                return False
            assert status == JobStatus.SUCCEEDED
            return True

        await self._wait_for(*args, status_predicate=_predicate, **kwargs)

    async def wait_until_running(self, *args: Any, **kwargs: Any) -> None:
        def _predicate(status: JobStatus) -> bool:
            assert not status.is_finished
            return status == JobStatus.RUNNING

        await self._wait_for(*args, status_predicate=_predicate, **kwargs)

    async def test_start_job_happy_path(
        self,
        job_nginx: MyJob,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        await job_nginx.start()
        await self.wait_for_success(job_nginx)

        async with kube_client_selector.get_client(
            org_name=job_nginx.org_name, project_name=job_nginx.project_name
        ) as kube_client:
            pod = await get_pod(kube_client, job_nginx.id)

        assert pod.image_pull_secrets == [SecretRef(f"neurouser-{job_nginx.owner}")]

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    async def test_start_job_image_pull_secret(
        self,
        job_nginx: MyJob,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        kube_orchestrator._kube_config = replace(
            kube_orchestrator._kube_config, image_pull_secret_name="test-secret"
        )

        await job_nginx.start()
        await self.wait_for_success(job_nginx)

        async with kube_client_selector.get_client(
            org_name=job_nginx.org_name, project_name=job_nginx.project_name
        ) as kube_client:
            pod = await get_pod(kube_client, job_nginx.id)

        assert set(pod.image_pull_secrets) == {
            SecretRef(f"neurouser-{job_nginx.owner}"),
            SecretRef("test-secret"),
        }

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    async def test_start_job_broken_image(
        self, kube_orchestrator: KubeOrchestrator
    ) -> None:
        container = Container(
            image="notsuchdockerimage",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job_request = JobRequest.create(container)
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        await job.start()
        status = await job.delete()
        assert status == JobStatus.PENDING

    async def test_start_job_bad_name(
        self, kube_orchestrator: KubeOrchestrator
    ) -> None:
        job_id = str(uuid.uuid4())
        container = Container(
            image="ubuntu:20.10",
            command="sleep 5",
            resources=ContainerResources(cpu=0.1, memory=256 * 10**6),
        )
        job_request = JobRequest(job_id=job_id, container=container)
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                org_name="test-org",
                owner="invalid_name",
            ),
        )

        with pytest.raises(KubeClientException) as cm:
            await job.start()
        assert "invalid" in str(cm.value)

    async def test_start_job_with_not_unique_id(
        self, kube_orchestrator: KubeOrchestrator, job_nginx: MyJob
    ) -> None:
        await job_nginx.start()
        await self.wait_for_success(job_nginx)

        container = Container(
            image="python", resources=ContainerResources(cpu=0.1, memory=128 * 10**6)
        )
        job_request_second = JobRequest(job_id=job_nginx.id, container=container)
        job_second = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=job_request_second,
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        with pytest.raises(JobAlreadyExistsException):
            await job_second.start()

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    async def test_status_job_not_exist(self, job_nginx: MyJob) -> None:
        with pytest.raises(JobNotFoundException):
            await job_nginx.query_status()

    async def test_delete_job_not_exist(self, job_nginx: MyJob) -> None:
        with pytest.raises(JobNotFoundException):
            await job_nginx.delete()

    async def test_broken_job_id(self, kube_orchestrator: KubeOrchestrator) -> None:
        job_id = "some_BROCKEN_JOB-123@#$%^&*(______------ID"
        container = Container(
            image="python", resources=ContainerResources(cpu=0.1, memory=128 * 10**6)
        )
        job_request = JobRequest(job_id=job_id, container=container)
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=job_request,
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )

        with pytest.raises(JobError):
            await job.start()

    async def test_job_succeeded(self, kube_orchestrator: KubeOrchestrator) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="true",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        try:
            await job.start()
            await self.wait_for_success(job)
        finally:
            await job.delete()

    async def test_job_failed_error(self, kube_orchestrator: KubeOrchestrator) -> None:
        command = 'bash -c "for i in {100..1}; do echo $i; done; false"'
        container = Container(
            image="ubuntu:20.10",
            command=command,
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        try:
            await job.start()
            await self.wait_for_failure(job)

            status_item = await kube_orchestrator.get_job_status(job)
            expected_description = "".join(f"{i}\n" for i in reversed(range(1, 81)))
            assert status_item == JobStatusItem.create(
                JobStatus.FAILED,
                reason=JobStatusReason.ERROR,
                description=expected_description,
                exit_code=1,
            )
        finally:
            await job.delete()

    async def test_job_requires_all_node_resources(
        self,
        kube_client_selector: KubeClientSelector,
        orchestrator_config_factory: Callable[..., OrchestratorConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        orchestrator_config = orchestrator_config_factory(
            resource_pool_types=[
                ResourcePoolType(
                    name="cpu",
                    cpu=0.1,
                    available_cpu=0.1,
                    memory=1025 * 10**6,
                    available_memory=1025 * 10**6,
                )
            ]
        )
        kube_orchestrator = kube_orchestrator_factory(
            orchestrator_config=orchestrator_config
        )
        container = Container(
            image="ubuntu:20.10",
            command="true",
            resources=ContainerResources(cpu=0.1, memory=1025 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=f"job-{uuid.uuid4().hex[:6]}",
                owner="owner1",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )

        await delete_job_later(job)
        await job.start()

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_scheduled(kube_client, job.id)
            job_pod = await kube_client.core_v1.pod.get(job.id)

            assert job_pod.spec is not None
            assert job_pod.spec.containers
            resources = job_pod.spec.containers[0].resources
            # 0.8 of total request
            assert resources.requests == {"cpu": "100m", "memory": "820M"}
            assert resources.limits == {"cpu": "100m", "memory": "1025M"}

    async def test_job_bunch_of_cpu(self, kube_orchestrator: KubeOrchestrator) -> None:
        command = 'bash -c "for i in {100..1}; do echo $i; done; false"'
        container = Container(
            image="ubuntu:20.10",
            command=command,
            resources=ContainerResources(cpu=100, memory=16536 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        try:
            await job.start()

            status_item = await kube_orchestrator.get_job_status(job)
            assert status_item == JobStatusItem.create(
                JobStatus.PENDING, reason=JobStatusReason.SCHEDULING
            )
        finally:
            await job.delete()

    async def test_job_no_memory(self, kube_orchestrator: KubeOrchestrator) -> None:
        command = "true"
        container = Container(
            image="ubuntu:20.10",
            command=command,
            resources=ContainerResources(cpu=1, memory=500_000 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                schedule_timeout=10,
            ),
        )
        await job.start()

        status_item = await kube_orchestrator.get_job_status(job)
        assert status_item.status == JobStatus.PENDING

        t0 = time.time()
        while not status_item.status.is_finished:
            assert status_item.reason == JobStatusReason.SCHEDULING
            t1 = time.time()
            assert t1 - t0 < 30, (
                f"Wait for job failure is timed out "
                f"after {t1 - t0} secs [{status_item}]"
            )
            status_item = await kube_orchestrator.get_job_status(job)

        assert status_item == JobStatusItem.create(
            JobStatus.FAILED,
            reason=JobStatusReason.CLUSTER_SCALE_UP_FAILED,
            description="Failed to scale up the cluster to get more resources",
        )

    async def test_job_no_memory_after_scaleup(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        command = "true"
        container = Container(
            image="ubuntu:20.10",
            command=command,
            resources=ContainerResources(cpu=1, memory=500_000 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                schedule_timeout=10,
            ),
        )
        await job.start()
        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await self._create_triggered_scaleup_event(kube_client, job.id)

        status_item = await kube_orchestrator.get_job_status(job)
        assert status_item.status == JobStatus.PENDING

        t0 = time.monotonic()
        found_scaleup = False
        while not status_item.status.is_finished:
            t1 = time.monotonic()
            if status_item.reason == JobStatusReason.CLUSTER_SCALING_UP:
                found_scaleup = True
            else:
                assert status_item.reason == JobStatusReason.SCHEDULING
            assert t1 - t0 < 30, (
                f"Wait for job failure is timed out "
                f"after {t1 - t0} secs [{status_item}]"
            )
            status_item = await kube_orchestrator.get_job_status(job)

        assert status_item == JobStatusItem.create(
            JobStatus.FAILED,
            reason=JobStatusReason.CLUSTER_SCALE_UP_FAILED,
            description="Failed to scale up the cluster to get more resources",
        )
        assert found_scaleup

    async def test_job_disk_unavailable(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        org_name = "test-org"
        project_name = DEFAULT_ORPHANED_JOB_OWNER
        user_name = self._create_username()
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            disk_id = f"disk-{str(uuid.uuid4())}"
            disk = Disk(disk_id=disk_id, path=user_name, cluster_name=cluster_name)
            await self._create_pvc(kube_client, disk_id, labels={})

            mount_path = PurePath("/mnt/disk")
            container = Container(
                image="ubuntu:20.10",
                command="sleep 10",
                resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
                disk_volumes=[
                    DiskContainerVolume(disk=disk, dst_path=mount_path, read_only=False)
                ],
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(container),
                    cluster_name="test-cluster",
                    org_name="test-org",
                ),
            )
            await delete_job_later(job)
            await job.start()

            status_item = await kube_orchestrator.get_job_status(job)
            assert status_item.status == JobStatus.PENDING

            t0 = time.monotonic()
            while not status_item.status.is_finished:
                await self._create_failed_attach_volume_event(kube_client, job.id)
                t1 = time.monotonic()
                if status_item.reason == JobStatusReason.DISK_UNAVAILABLE:
                    break
                assert t1 - t0 < 30, (
                    f"Wait for job failure is timed out "
                    f"after {t1 - t0} secs [{status_item}]"
                )
                status_item = await kube_orchestrator.get_job_status(job)

            assert status_item == JobStatusItem.create(
                JobStatus.PENDING,
                reason=JobStatusReason.DISK_UNAVAILABLE,
                description="Waiting for another job to release disk resource",
            )

            await kube_orchestrator.delete_job(job)

    async def test_volumes(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        cluster_name: str,
    ) -> None:
        volumes = [
            ContainerVolume(
                uri=URL(f"{STORAGE_URI_SCHEME}://{cluster_name}/org/project1"),
                dst_path=PurePath("/storage1"),
            ),
            ContainerVolume(
                uri=URL(f"{STORAGE_URI_SCHEME}://{cluster_name}/org/project2"),
                dst_path=PurePath("/storage2"),
                read_only=True,
            ),
        ]
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'echo test'",
            volumes=volumes,
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )

        try:
            await job.start()
            async with kube_client_selector.get_client(
                org_name=job.org_name, project_name=job.project_name
            ) as kube_client:
                pod = await kube_client.core_v1.pod.get(job.id)
                annotations = pod.metadata.annotations or {}
                labels = pod.metadata.labels or {}

            assert annotations[INJECT_STORAGE_KEY] == json.dumps(
                [
                    {
                        "storage_uri": f"storage://{cluster_name}/org/project1",
                        "mount_path": "/storage1",
                        "mount_mode": "rw",
                    },
                    {
                        "storage_uri": f"storage://{cluster_name}/org/project2",
                        "mount_path": "/storage2",
                        "mount_mode": "r",
                    },
                ]
            )
            assert labels[INJECT_STORAGE_KEY] == "true"
        finally:
            await job.delete()

    @pytest.mark.parametrize(
        "expected_result,expected_status",
        (("6", JobStatus.SUCCEEDED), ("7", JobStatus.FAILED)),
    )
    async def test_env(
        self,
        kube_orchestrator: KubeOrchestrator,
        expected_result: str,
        expected_status: JobStatus,
    ) -> None:
        product = expected_result
        container = Container(
            image="ubuntu:20.10",
            env={"A": "2", "B": "3"},
            command=rf"""bash -c '[ "$(expr $A \* $B)" == "{product}" ]'""",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )

        try:
            await job.start()
            status = await self.wait_for_completion(job)
            assert status == expected_status
        finally:
            await job.delete()

    async def test_job_pod_metadata_in_env(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        container = Container(
            image="ubuntu:20.10",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name=cluster_name,
                org_name="test-org",
                owner=user_name,
                name="test-job",
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        async with kube_client_selector.get_client(
            org_name=job.org_name, project_name=job.project_name
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            pod = await kube_client.core_v1.pod.get(pod_name)

        assert pod.spec is not None
        container_model = pod.spec.containers[0]

        expected_values = {
            "NEURO_JOB_ID": job.id,
            "NEURO_JOB_NAME": job.name,
            "NEURO_JOB_OWNER": job.owner,
            "NEURO_JOB_CLUSTER": job.cluster_name,
            "NEURO_JOB_INTERNAL_HOSTNAME": job.internal_hostname,
            "NEURO_JOB_INTERNAL_HOSTNAME_NAMED": job.internal_hostname_named,
            "NEURO_JOB_HTTP_PORT": "",
            "NEURO_JOB_HTTP_AUTH": "",
            # Uncomment after https://github.com/neuro-inc/platform-api/pull/1398 merged
            # "NEURO_JOB_PRESET": job.preset_name,
        }
        real_values = {
            env.name: (env.value or "") for env in (container_model.env or [])
        }

        for key, value in expected_values.items():
            assert real_values[key] == value, key

    async def test_working_dir(self, kube_orchestrator: KubeOrchestrator) -> None:
        container = Container(
            image="ubuntu:20.10",
            working_dir="/var/log",
            command="""bash -c '[ "$(pwd)" == "/var/log" ]'""",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )

        try:
            await job.start()
            status = await self.wait_for_completion(job)
            assert status == JobStatus.SUCCEEDED
        finally:
            await job.delete()

    async def test_ingress_create_delete(
        self,
        job_nginx: MyJob,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        name = str(uuid.uuid4())
        rules = [
            IngressRule(host=""),
            IngressRule(host="host1", service_name="service1", service_port=81),
            IngressRule(host="host2", service_name="service2", service_port=82),
            IngressRule(host="host3", service_name="service3", service_port=83),
        ]
        annotations = {"key/1": "value 1", "key/2": "value 2"}
        labels = {"label/1": "label-value-1", "label/2": "label-value-2"}
        expected_ingress = Ingress(
            name=name,
            ingress_class=cast(str, mock.ANY),
            rules=rules,
            annotations=annotations,
            labels=labels,
        )

        ingress = Ingress(
            name=name,
            rules=rules,
            annotations=annotations,
            labels=labels,
        )
        async with kube_client_selector.get_client(
            org_name=job_nginx.org_name,
            project_name=job_nginx.project_name,
        ) as kube_client:
            created_ingress = await create_ingress(
                kube_client,
                ingress,
            )
            assert created_ingress == expected_ingress

            requested_ingress = await get_ingress(kube_client, ingress.name)
            assert requested_ingress == expected_ingress

            await delete_ingress(kube_client, name)
            with pytest.raises(ResourceNotFound):
                await get_ingress(kube_client, name)

    async def test_delete_ingress_failure(
        self, kube_client_selector: KubeClientSelector
    ) -> None:
        async with kube_client_selector.get_client(
            org_name="org", project_name="proj"
        ) as kube_client:
            with pytest.raises(ResourceNotFound):
                await delete_ingress(kube_client, "unknown")

    async def test_service(
        self,
        job_nginx: MyJob,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        service_name = f"job-{uuid.uuid4()}"
        labels = {"label1": f"value-{uuid.uuid4()}", "label2": f"value-{uuid.uuid4()}"}
        service = Service(
            name=service_name,
            target_port=8080,
            labels=labels,
        )
        async with kube_client_selector.get_client(
            org_name=job_nginx.org_name,
            project_name=job_nginx.project_name,
        ) as kube_client:
            try:
                result_service = await create_service(kube_client, service)
                assert result_service.name == service_name
                assert result_service.target_port == 8080
                assert result_service.port == 80
                assert result_service.labels == labels

                service = await get_service(kube_client, service_name)
                assert service.name == service_name
                assert service.target_port == 8080
                assert service.port == 80
                assert service.labels == labels

            finally:
                await delete_service(kube_client, service_name)

    async def test_list_services(
        self,
        job_nginx: MyJob,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        labels1 = {"label": f"value-{uuid.uuid4()}"}
        labels2 = {"label": f"value-{uuid.uuid4()}"}

        def _gen_for_labels(labels: dict[str, str]) -> list[Service]:
            return [
                Service(
                    name=f"job-{uuid.uuid4()}",
                    target_port=8080,
                    labels=labels,
                )
                for _ in range(5)
            ]

        services1 = _gen_for_labels(labels1)
        services2 = _gen_for_labels(labels2)
        async with kube_client_selector.get_client(
            org_name=job_nginx.org_name,
            project_name=job_nginx.project_name,
        ) as kube_client:
            try:
                for service in itertools.chain(services1, services2):
                    await create_service(kube_client, service)

                assert {service.name for service in services1} == {
                    service.name
                    for service in await list_services(kube_client, labels1)
                }

                assert {service.name for service in services2} == {
                    service.name
                    for service in await list_services(kube_client, labels2)
                }
            finally:
                for service in itertools.chain(services1, services2):
                    try:
                        await delete_service(kube_client, service.name)
                    except Exception:
                        pass

    async def test_service_delete_by_uid(
        self,
        job_nginx: MyJob,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        service_name = f"job-{uuid.uuid4()}"
        async with kube_client_selector.get_client(
            org_name=job_nginx.org_name,
            project_name=job_nginx.project_name,
        ) as kube_client:
            service = Service(name=service_name, target_port=8080)
            try:
                service_initial = await create_service(kube_client, service)
                await delete_service(kube_client, service_name)
                await create_service(kube_client, service)
                with pytest.raises(KubeClientException):
                    await delete_service(
                        kube_client, service_name, uid=service_initial.uid
                    )
            finally:
                await delete_service(kube_client, service_name)

    async def _wait_for_job_service(
        self,
        kube_ingress_ip: str,
        host: str,
        job_id: str,
        interval_s: float = 0.5,
        max_time: float = 180,
    ) -> None:
        url = f"http://{kube_ingress_ip}"
        headers = {"Host": host}
        t0 = time.monotonic()
        async with aiohttp.ClientSession() as client:
            while True:
                try:
                    async with client.get(url, headers=headers) as response:
                        if response.status == 200:
                            break
                except (OSError, aiohttp.ClientError) as exc:
                    print(exc)
                await asyncio.sleep(max(interval_s, time.monotonic() - t0))
                if time.monotonic() - t0 > max_time:
                    pytest.fail(f"Failed to connect to job service {job_id}")
                interval_s *= 1.5

    async def _wait_for_job_internal_service(
        self,
        host: str,
        port: int | None,
        kube_orchestrator: KubeOrchestrator,
        delete_job_later: Callable[[Job], Awaitable[None]],
        interval_s: float = 0.5,
        max_time: float = 180,
    ) -> None:
        port = port or 80
        job_name = f"poller-{host.split('.')[0]}"
        cmd = (
            f"timeout {max_time} sh -c "
            f"'until $(nc -zvvw10 {host} {port}); do sleep {interval_s}; done'"
        )
        poller_job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(
                    container=Container(
                        image="busybox",
                        command=cmd,
                        resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
                        http_server=ContainerHTTPServer(port=port),
                    ),
                ),
                cluster_name="test-cluster",
                org_name="test-org",
                name=job_name,
                owner="owner",
            ),
        )
        await poller_job.start()
        await delete_job_later(poller_job)
        t0 = time.monotonic()
        while True:
            poller_job_status = await poller_job.query_status()
            if poller_job_status.is_finished:
                break
            await asyncio.sleep(interval_s)
            if time.monotonic() - t0 > max_time * 1.1:
                # This should not happen, but just in case
                pytest.fail(f"Timeout connecting to job service {host}")

        poller_last_status = await kube_orchestrator.get_job_status(poller_job)
        if poller_last_status.exit_code:
            print(poller_last_status.to_primitive())
            pytest.fail(f"Non-0 exit code from {host} poller (most likely, a timeout)")

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="Kube ingress address is not easily exposable on macOS.",
    )
    async def test_job_with_exposed_http_server_no_job_name(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            try:
                await job.start()

                assert job.http_host is not None
                assert job.http_host_named is None

                await self._wait_for_job_service(
                    kube_ingress_ip, host=job.http_host, job_id=job.id
                )
                ingress = await get_ingress(kube_client, job.id)
                actual_rules_hosts = {rule.host for rule in ingress.rules}
                assert actual_rules_hosts == {job.http_host}

            finally:
                await job.delete()

                # check if ingresses were deleted:
                with pytest.raises(ResourceNotFound):
                    await get_ingress(kube_client, job.id)

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="Kube ingress address is not easily exposable on macOS.",
    )
    async def test_job_with_exposed_http_server_with_job_name(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client_selector: KubeClientSelector,
        org_name: str,
        project_name: str,
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name=org_name,
                project_name=project_name,
                name=f"test-job-name-{random_str()}",
                owner="owner",
            ),
        )
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            try:
                await job.start()
                assert not (job.requires_http_auth), str(job)

                assert job.http_host is not None
                assert job.http_host_named is not None

                await self._wait_for_job_service(
                    kube_ingress_ip, host=job.http_host, job_id=job.id
                )
                await self._wait_for_job_service(
                    kube_ingress_ip, host=job.http_host_named, job_id=job.id
                )

                # job ingress:
                ingress = await get_ingress(kube_client, job.id)
                actual_rules_hosts = {rule.host for rule in ingress.rules}
                assert actual_rules_hosts == {job.http_host, job.http_host_named}

            finally:
                await job.delete()

                # check ingresses were deleted:
                with pytest.raises(ResourceNotFound):
                    await get_ingress(kube_client, job.id)

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="Kube ingress address is not easily exposable on macOS.",
    )
    async def test_job_with_exposed_http_server_with_auth_no_job_name(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            http_server=ContainerHTTPServer(port=80, requires_auth=True),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            try:
                await job.start()

                assert job.http_host is not None
                assert job.http_host_named is None

                await self._wait_for_job_service(
                    kube_ingress_ip, host=job.http_host, job_id=job.id
                )

                # job ingress auth:
                ingress = await get_ingress(kube_client, job.id)
                actual_rules_hosts = {rule.host for rule in ingress.rules}
                assert actual_rules_hosts == {job.http_host}

            finally:
                await job.delete()

                # check ingresses were deleted:

                with pytest.raises(ResourceNotFound):
                    await get_ingress(kube_client, job.id)

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="Kube ingress address is not easily exposable on macOS.",
    )
    async def test_job_with_exposed_http_server_with_auth_with_job_name(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client_selector: KubeClientSelector,
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            http_server=ContainerHTTPServer(port=80, requires_auth=True),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                name=f"test-job-name-{random_str()}",
                owner="owner",
            ),
        )
        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            try:
                await job.start()

                assert job.http_host is not None
                assert job.http_host_named is not None

                await self._wait_for_job_service(
                    kube_ingress_ip, host=job.http_host, job_id=job.id
                )
                await self._wait_for_job_service(
                    kube_ingress_ip, host=job.http_host_named, job_id=job.id
                )

                # job ingress auth:
                ingress = await get_ingress(kube_client, job.id)
                actual_rules_hosts = {rule.host for rule in ingress.rules}
                assert actual_rules_hosts == {job.http_host, job.http_host_named}

            finally:
                await job.delete()

                # check ingresses were deleted:

                with pytest.raises(ResourceNotFound):
                    await get_ingress(kube_client, job.id)

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="Kube ingress address is not easily exposable on macOS.",
    )
    @pytest.mark.parametrize("job_named", (False, True))
    async def test_job_service_lifecycle_with_job_name(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client_selector: KubeClientSelector,
        job_named: bool,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            http_server=ContainerHTTPServer(port=80),
        )
        job_name = f"test-job-name-{random_str()}" if job_named else None
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                name=job_name,
                owner="owner",
            ),
        )
        service_name_id = job.id
        service_name_named = kube_orchestrator._get_service_name_for_named(job)

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            with pytest.raises(ResourceNotFound):
                await get_service(kube_client, service_name_id)
            if job_named:
                with pytest.raises(ResourceNotFound):
                    await get_service(kube_client, service_name_named)

        await job.start()

        assert not (job.requires_http_auth), str(job)
        assert job.http_host is not None
        assert job.internal_hostname is not None
        if not job_named:
            assert job.internal_hostname_named is None
            assert job.http_host_named is None

        await self._wait_for_job_service(
            kube_ingress_ip, host=job.http_host, job_id=job.id
        )
        await self._wait_for_job_internal_service(
            host=job.internal_hostname,
            port=job.request.container.port,
            kube_orchestrator=kube_orchestrator,
            delete_job_later=delete_job_later,
        )
        if job_named:
            assert job.internal_hostname_named is not None
            assert job.http_host_named is not None
            await self._wait_for_job_service(
                kube_ingress_ip, host=job.http_host_named, job_id=job.id
            )
            await self._wait_for_job_internal_service(
                host=job.internal_hostname_named,
                port=job.request.container.port,
                kube_orchestrator=kube_orchestrator,
                delete_job_later=delete_job_later,
            )
        await kube_orchestrator.delete_job(job)

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            with pytest.raises(ResourceNotFound):
                await get_service(kube_client, service_name_id)
            if job_named:
                with pytest.raises(ResourceNotFound):
                    await get_service(kube_client, service_name_named)

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="Kube ingress address is not easily exposable on macOS.",
    )
    async def test_job_named_service_recreated(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        job_name = f"test-job-name-{random_str()}"

        def get_test_job(port: int) -> MyJob:
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(
                        container=Container(
                            image="python",
                            command=f"python -m http.server {port}",
                            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
                            http_server=ContainerHTTPServer(port=port),
                        ),
                    ),
                    cluster_name="test-cluster",
                    org_name="test-org",
                    name=job_name,
                    owner="owner",
                ),
            )

        job1 = get_test_job(80)
        job2 = get_test_job(8000)
        await delete_job_later(job1)
        await delete_job_later(job2)

        await job1.start()
        assert job1.http_host_named is not None
        assert job1.internal_hostname_named is not None

        await self._wait_for_job_service(
            kube_ingress_ip, host=job1.http_host_named, job_id=job1.id
        )
        await self._wait_for_job_internal_service(
            host=job1.internal_hostname_named,
            port=job1.request.container.port,
            kube_orchestrator=kube_orchestrator,
            delete_job_later=delete_job_later,
        )

        await job2.start()
        assert job1.http_host != job2.http_host
        assert job1.internal_hostname != job2.internal_hostname
        assert job1.http_host_named == job2.http_host_named
        assert job1.internal_hostname_named == job2.internal_hostname_named

        await self._wait_for_job_service(
            kube_ingress_ip, host=job2.http_host_named, job_id=job2.id
        )
        await self._wait_for_job_internal_service(
            host=job2.internal_hostname_named,
            port=job2.request.container.port,
            kube_orchestrator=kube_orchestrator,
            delete_job_later=delete_job_later,
        )

    @pytest.fixture
    def create_server_job(
        self, kube_orchestrator: KubeOrchestrator
    ) -> Iterator[Callable[[str | None], MyJob]]:
        def impl(job_name: str | None = None) -> MyJob:
            server_cont = Container(
                image="python:3.13",
                command="python -m http.server 80",
                resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
                http_server=ContainerHTTPServer(port=80),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(server_cont),
                    cluster_name="test-cluster",
                    org_name="test-org",
                    name=job_name,
                ),
            )

        yield impl

    @pytest.fixture
    def create_client_job(
        self, kube_orchestrator: KubeOrchestrator
    ) -> Iterator[Callable[[str], MyJob]]:
        def impl(server_hostname: str) -> MyJob:
            cmd = (
                "curl --fail --connect-timeout 5 --retry 20 --retry-connrefused "
                f"http://{server_hostname}/"
            )
            client_cont = Container(
                image="python:3.13",
                command=cmd,
                resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(client_cont),
                    cluster_name="test-cluster",
                    org_name="test-org",
                ),
            )

        yield impl

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="Kube ingress address is not easily exposable on macOS.",
    )
    async def test_job_check_dns_hostname(
        self,
        kube_config: KubeConfig,
        create_server_job: Callable[..., MyJob],
        create_client_job: Callable[[str], MyJob],
        kube_ingress_ip: str,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        server_job = create_server_job()
        await delete_job_later(server_job)
        await server_job.start()
        server_hostname = server_job.internal_hostname
        assert server_hostname
        await self._wait_for_job_service(
            kube_ingress_ip, host=server_job.http_host, job_id=server_job.id
        )

        client_job = create_client_job(server_hostname)
        await delete_job_later(client_job)
        await client_job.start()
        await self.wait_for_success(job=client_job)

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="Kube ingress address is not easily exposable on macOS.",
    )
    async def test_named_job_check_dns_hostname(
        self,
        kube_config: KubeConfig,
        create_server_job: Callable[..., MyJob],
        create_client_job: Callable[[str], MyJob],
        kube_ingress_ip: str,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        server_job = create_server_job("some-name")
        await delete_job_later(server_job)
        await server_job.start()
        server_hostname = server_job.internal_hostname_named
        assert server_hostname
        await self._wait_for_job_service(
            kube_ingress_ip, host=server_job.http_host, job_id=server_job.id
        )

        client_job = create_client_job(server_hostname)
        await delete_job_later(client_job)
        await client_job.start()
        await self.wait_for_success(job=client_job)

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason="Kube ingress address is not easily exposable on macOS.",
    )
    async def test_job_check_dns_hostname_undeclared_port(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        def create_server_job() -> MyJob:
            server_cont = Container(
                image="python:3.13",
                command="python -m http.server 12345",
                resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(server_cont),
                    cluster_name="test-cluster",
                    org_name="test-org",
                ),
            )

        def create_client_job(server_hostname: str) -> MyJob:
            cmd = (
                "curl --fail --connect-timeout 5 --retry 20 --retry-connrefused "
                f"http://{server_hostname}:12345/"
            )
            client_cont = Container(
                image="python:3.13",
                command=cmd,
                resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(client_cont),
                    cluster_name="test-cluster",
                    org_name="test-org",
                ),
            )

        server_job = create_server_job()
        await delete_job_later(server_job)
        await server_job.start()
        await self.wait_until_running(server_job)

        assert server_job.internal_hostname
        client_job = create_client_job(server_job.internal_hostname)
        await delete_job_later(client_job)
        await client_job.start()
        await self.wait_for_success(client_job)

    async def test_job_pod_labels_and_network_policy(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="cpu-micro",
            ),
        )
        await delete_job_later(job)
        await job.start()

        async with kube_client_selector.get_client(
            org_name=job.org_name, project_name=job.project_name
        ) as kube_client:
            pod = await kube_client.core_v1.pod.get(job.id)
        assert pod.metadata.labels == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/preset": job.preset_name,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/org": "test-org",
            "platform.neuromation.io/project": job.owner,
            "platform.apolo.us/job": job.id,
            "platform.apolo.us/preset": job.preset_name,
            "platform.apolo.us/user": job.owner,
            "platform.apolo.us/org": "test-org",
            "platform.apolo.us/project": job.owner,
        }

    async def test_job_org_pod_labels(
        self,
        kube_config: KubeConfig,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        async with kube_client_selector.get_client(
            org_name=job.org_name, project_name=job.project_name
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)
            pod = await kube_client.core_v1.pod.get(pod_name)

        assert pod.metadata.labels == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/org": job.org_name,
            "platform.neuromation.io/project": job.owner,
            "platform.apolo.us/job": job.id,
            "platform.apolo.us/user": job.owner,
            "platform.apolo.us/org": job.org_name,
            "platform.apolo.us/project": job.owner,
        }

    async def test_job_resource_labels(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="sleep 1h",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        await delete_job_later(job)
        await job.start()

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            pod_name = job.id
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            service_name = job.id
            service = await get_service(kube_client, service_name)
            assert service.labels == {
                "platform.neuromation.io/job": job.id,
                "platform.neuromation.io/user": job.owner,
                "platform.neuromation.io/org": "test-org",
                "platform.neuromation.io/project": job.owner,
                "platform.apolo.us/job": job.id,
                "platform.apolo.us/user": job.owner,
                "platform.apolo.us/org": "test-org",
                "platform.apolo.us/project": job.owner,
            }

            ingress_name = job.id
            ingress = await get_ingress(kube_client, ingress_name)
            assert ingress.labels == {
                "platform.neuromation.io/job": job.id,
                "platform.neuromation.io/user": job.owner,
                "platform.neuromation.io/org": "test-org",
                "platform.neuromation.io/project": job.owner,
                "platform.apolo.us/job": job.id,
                "platform.apolo.us/user": job.owner,
                "platform.apolo.us/org": "test-org",
                "platform.apolo.us/project": job.owner,
            }

    async def test_named_job_resource_labels(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="sleep 1h",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=f"test-{uuid.uuid4().hex[:6]}",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            service_name = job.id
            service = await get_service(kube_client, service_name)
            assert service.labels == {
                "platform.neuromation.io/job": job.id,
                "platform.neuromation.io/user": job.owner,
                "platform.neuromation.io/org": "test-org",
                "platform.neuromation.io/project": job.owner,
                "platform.apolo.us/job": job.id,
                "platform.apolo.us/user": job.owner,
                "platform.apolo.us/org": "test-org",
                "platform.apolo.us/project": job.owner,
            }

            ingress_name = job.id
            ingress = await get_ingress(kube_client, ingress_name)
            assert ingress.labels == {
                "platform.neuromation.io/job": job.id,
                "platform.neuromation.io/job-name": job.name,
                "platform.neuromation.io/user": job.owner,
                "platform.neuromation.io/org": "test-org",
                "platform.neuromation.io/project": job.owner,
                "platform.apolo.us/job": job.id,
                "platform.apolo.us/job-name": job.name,
                "platform.apolo.us/user": job.owner,
                "platform.apolo.us/org": "test-org",
                "platform.apolo.us/project": job.owner,
            }

    async def test_job_check_ingress_annotations_jobs_ingress_class_nginx(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_selector: KubeClientSelector,
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="nginx")
        orchestrator = kube_orchestrator_factory(kube_config=kube_config)
        container = Container(
            image="ubuntu:20.10",
            command="sleep 1h",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            try:
                await job.start()
                pod_name = job.id
                await wait_pod_is_running(
                    kube_client, pod_name=pod_name, timeout_s=60.0
                )
                ingress = await get_ingress(kube_client, pod_name)
                assert ingress.ingress_class == "nginx"
            finally:
                await orchestrator.delete_job(job)

    async def test_job_check_ingress_annotations_jobs_ingress_class_traefik_no_auth(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_selector: KubeClientSelector,
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        orchestrator = kube_orchestrator_factory(kube_config=kube_config)
        container = Container(
            image="ubuntu:20.10",
            command="sleep 1h",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            try:
                await job.start()
                pod_name = job.id
                await wait_pod_is_running(
                    kube_client, pod_name=pod_name, timeout_s=60.0
                )
                ingress = await get_ingress(kube_client, pod_name)
                assert ingress.ingress_class == "traefik"
                assert ingress.annotations == {
                    "traefik.ingress.kubernetes.io/router.middlewares": (
                        "error-page@kubernetescrd"
                    ),
                }
            finally:
                await orchestrator.delete_job(job)

    async def test_job_check_ingress_annotations_jobs_ingress_class_traefik_with_auth(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_selector: KubeClientSelector,
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        orchestrator = kube_orchestrator_factory(kube_config=kube_config)
        container = Container(
            image="ubuntu:20.10",
            command="sleep 1h",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
            http_server=ContainerHTTPServer(port=80, requires_auth=True),
        )
        job = MyJob(
            orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            try:
                await job.start()
                pod_name = job.id
                await wait_pod_is_running(
                    kube_client, pod_name=pod_name, timeout_s=60.0
                )
                ingress = await get_ingress(kube_client, pod_name)
                assert ingress.ingress_class == "traefik"
                assert ingress.annotations == {
                    "traefik.ingress.kubernetes.io/router.middlewares": (
                        "error-page@kubernetescrd,ingress-auth@kubernetescrd"
                    ),
                }
            finally:
                await orchestrator.delete_job(job)

    async def test_job_pod_tolerations(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="sleep 1h",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name, project_name=job.project_name
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            pod = await get_pod(kube_client, pod_name)
            toleration_expected = Toleration(
                key="platform.neuromation.io/job",
                operator="Exists",
                value="",
                effect="NoSchedule",
            )
            assert toleration_expected in pod.tolerations

    def _create_username(self) -> str:
        return f"user-{uuid.uuid4().hex[:6]}"

    async def test_get_missing_secrets(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_selector: KubeClientSelector,
        cluster_name: str,
        org_name: str,
        project_name: str,
    ) -> None:
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            user_name = self._create_username()
            kube_config = kube_config_factory(jobs_ingress_class="traefik")
            orchestrator = kube_orchestrator_factory(
                kube_config=kube_config,
                kube_client_selector=kube_client_selector,
            )
            secret = Secret("key2", user_name, cluster_name)
            await kube_client.core_v1.secret.create_or_update(
                V1Secret(
                    metadata=V1ObjectMeta(name=secret.k8s_secret_name),
                    data={secret.secret_key: "vvvv"},
                )
            )

            container = Container(
                image="ubuntu:20.10",
                command="sleep 1h",
                resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
                http_server=ContainerHTTPServer(port=80),
            )
            job = MyJob(
                orchestrator=orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(container),
                    cluster_name="test-cluster",
                    org_name=org_name,
                    project_name=project_name,
                ),
            )

            missing = await orchestrator.get_missing_secrets(
                job, user_name, secret_names=["key3", "key2", "key1"]
            )
            assert missing == ["key1", "key3"]

    async def test_get_missing_disks(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_selector: KubeClientSelector,
        cluster_name: str,
        org_name: str,
        project_name: str,
    ) -> None:
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            kube_config = kube_config_factory(jobs_ingress_class="traefik")
            orchestrator = kube_orchestrator_factory(
                kube_config=kube_config,
                kube_client_selector=kube_client_selector,
            )
            disk_labels = {
                "platform.neuromation.io/project": project_name,
                "platform.neuromation.io/disk-api-org-name": org_name,
            }
            await self._create_pvc(
                kube_client,
                "disk-1",
                labels={
                    "platform.neuromation.io/user": "user",
                    **disk_labels,
                },
            )
            await self._create_pvc(
                kube_client,
                "disk-2",
                labels={
                    "platform.neuromation.io/user": "another_user",
                    **disk_labels,
                },
            )
            disk1, disk2, disk3, disk4 = [
                Disk(
                    cluster_name=cluster_name,
                    path="user",
                    disk_id="disk-1",
                ),
                Disk(
                    cluster_name=cluster_name,
                    path="test_project",
                    disk_id="disk-2",
                ),
                Disk(
                    cluster_name=cluster_name,
                    path="user",
                    disk_id="disk-2",
                ),
                Disk(
                    cluster_name=cluster_name,
                    path="user",
                    disk_id="disk-3",  # Not existing id
                ),
            ]

            missing = await orchestrator.get_missing_disks(
                namespace=kube_client._namespace,
                org_name=org_name,
                project_name=project_name,
                disks=[disk1, disk2, disk3, disk4],
            )
            assert missing == [disk4]

            await kube_client.core_v1.persistent_volume_claim.delete("disk-1")
            await kube_client.core_v1.persistent_volume_claim.delete("disk-2")

    async def test_job_pod_with_disk_volume_simple_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        org_name = "test-org"
        project_name = user_name
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            disk_id = f"disk-{str(uuid.uuid4())}"
            disk = Disk(disk_id=disk_id, path=user_name, cluster_name=cluster_name)
            await self._create_pvc(kube_client, disk_id, labels={})

            mount_path = PurePath("/mnt/disk")
            container = Container(
                image="ubuntu:20.10",
                command="sleep infinity",
                resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
                http_server=ContainerHTTPServer(port=80),
                disk_volumes=[
                    DiskContainerVolume(disk=disk, dst_path=mount_path, read_only=False)
                ],
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(container),
                    cluster_name=cluster_name,
                    org_name="test-org",
                    owner=user_name,
                ),
            )
            await delete_job_later(job)
            await job.start()

            pod_name = job.id
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            pod = await kube_client.core_v1.pod.get(pod_name)
            assert pod.spec is not None

            disk_volumes_raw = [
                v
                for v in pod.spec.volumes
                if v.persistent_volume_claim is not None
                and v.persistent_volume_claim.claim_name == disk_id
            ]
            assert len(disk_volumes_raw) == 1

            container_raw = pod.spec.containers[0]
            volume_mount = container_raw.volume_mounts[0]
            assert volume_mount.name == disk_volumes_raw[0].name
            assert volume_mount.mount_path == str(mount_path)
            assert volume_mount.sub_path == "."

    async def test_job_pod_with_disk_volumes_same_mounts_fail(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        org_name = "test-org"
        project_name = user_name

        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            disk_id_1, disk_id_2 = (
                f"disk-{str(uuid.uuid4())}",
                f"disk-{str(uuid.uuid4())}",
            )
            disk1 = Disk(disk_id_1, user_name, cluster_name)
            disk2 = Disk(disk_id_2, user_name, cluster_name)

            await self._create_pvc(kube_client, disk_id_1, labels={})
            await self._create_pvc(kube_client, disk_id_2, labels={})

            mount_path = PurePath("/mnt/disk")

            container = Container(
                image="ubuntu:20.10",
                command="sleep infinity",
                resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
                http_server=ContainerHTTPServer(port=80),
                disk_volumes=[
                    DiskContainerVolume(disk=disk1, dst_path=mount_path),
                    DiskContainerVolume(disk=disk2, dst_path=mount_path),
                ],
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(container),
                    cluster_name=cluster_name,
                    org_name="test-org",
                    owner=user_name,
                ),
            )
            with pytest.raises(JobError):
                await job.start()

    async def test_job_pod_with_secret_env_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        org_name = "test-org"
        project_name = user_name

        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            secret_name = "key1"
            secret = Secret(secret_name, user_name, cluster_name)

            await kube_client.core_v1.secret.create_or_update(
                V1Secret(
                    api_version="v1",
                    metadata=V1ObjectMeta(name=secret.k8s_secret_name),
                    data={secret_name: "vvvv"},
                )
            )

            secret_env = {"SECRET_VAR": secret}
            container = Container(
                image="ubuntu:20.10",
                command="sleep infinity",
                resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
                http_server=ContainerHTTPServer(port=80),
                secret_env=secret_env,
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(container),
                    cluster_name=cluster_name,
                    org_name="test-org",
                    owner=user_name,
                ),
            )
            await delete_job_later(job)
            await job.start()

            pod_name = job.id
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            pod = await kube_client.core_v1.pod.get(pod_name)
            assert pod.spec is not None

            pod_container = pod.spec.containers[0]
            env_by_name = {e.name: e for e in pod_container.env}
            assert "SECRET_VAR" in env_by_name
            secret_env_model = env_by_name["SECRET_VAR"]
            assert secret_env_model.value_from.secret_key_ref is not None
            assert secret_env_model.value_from.secret_key_ref.key == secret_name
            assert (
                secret_env_model.value_from.secret_key_ref.name
                == secret.k8s_secret_name
            )

    async def test_job_pod_with_secret_env_same_secret_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        org_name = "test-org"
        project_name = user_name
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            secret_name_1, secret_name_2 = "key1", "key2"
            secret1 = Secret(secret_name_1, user_name, cluster_name)
            secret2 = Secret(secret_name_2, user_name, cluster_name)

            assert secret1.k8s_secret_name == secret2.k8s_secret_name
            k8s_secret_name = secret1.k8s_secret_name

            await kube_client.core_v1.secret.create_or_update(
                V1Secret(
                    api_version="v1",
                    metadata=V1ObjectMeta(name=k8s_secret_name),
                    data={secret_name_1: "vvvv", secret_name_2: "vvvv"},
                )
            )

            secret_env = {"SECRET_VAR_1": secret1, "SECRET_VAR_2": secret2}
            container = Container(
                image="ubuntu:20.10",
                command="sleep infinity",
                resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
                http_server=ContainerHTTPServer(port=80),
                secret_env=secret_env,
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(container),
                    cluster_name=cluster_name,
                    org_name="test-org",
                    owner=user_name,
                ),
            )
            await delete_job_later(job)
            await job.start()

            pod_name = job.id
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            pod = await kube_client.core_v1.pod.get(pod_name)
            assert pod.spec is not None

            container_pod = pod.spec.containers[0]
            env_by_name = {e.name: e for e in container_pod.env}
            assert "SECRET_VAR_1" in env_by_name
            assert "SECRET_VAR_2" in env_by_name

            secret_env_1 = env_by_name["SECRET_VAR_1"]
            assert secret_env_1.value_from.secret_key_ref is not None
            assert secret_env_1.value_from.secret_key_ref.key == secret_name_1
            assert secret_env_1.value_from.secret_key_ref.name == k8s_secret_name

            secret_env_2 = env_by_name["SECRET_VAR_2"]
            assert secret_env_2.value_from.secret_key_ref is not None
            assert secret_env_2.value_from.secret_key_ref.key == secret_name_2
            assert secret_env_2.value_from.secret_key_ref.name == k8s_secret_name

    async def test_job_pod_with_secret_volume_simple_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        org_name = "test-org"
        project_name = user_name

        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            secret_name = "key1"
            secret = Secret(secret_name, user_name, cluster_name)
            await kube_client.core_v1.secret.create_or_update(
                V1Secret(
                    api_version="v1",
                    metadata=V1ObjectMeta(name=secret.k8s_secret_name),
                    data={secret_name: "vvvv"},
                )
            )

            secret_path, secret_file = PurePath("/foo/bar"), "secret.txt"
            container = Container(
                image="ubuntu:20.10",
                command="sleep infinity",
                resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
                http_server=ContainerHTTPServer(port=80),
                secret_volumes=[
                    SecretContainerVolume(
                        secret=secret, dst_path=secret_path / secret_file
                    )
                ],
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(container),
                    cluster_name=cluster_name,
                    org_name="test-org",
                    owner=user_name,
                ),
            )
            await delete_job_later(job)
            await job.start()

            pod_name = job.id
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            pod = await kube_client.core_v1.pod.get(pod_name)
            assert pod.spec is not None

            sec_volumes_raw = [
                v for v in pod.spec.volumes if v.name == secret.k8s_secret_name
            ]
            assert sec_volumes_raw[0].name == secret.k8s_secret_name
            assert sec_volumes_raw[0].secret.secret_name == secret.k8s_secret_name
            assert sec_volumes_raw[0].secret.default_mode == 0o400

            container_raw = pod.spec.containers[0]
            volume_mount = container_raw.volume_mounts[0]
            assert volume_mount.name == secret.k8s_secret_name
            assert volume_mount.read_only is True
            assert volume_mount.mount_path == str(secret_path / secret_file)
            assert volume_mount.sub_path == secret_name

    async def test_job_pod_with_secret_volumes_same_mounts_fail(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        org_name = "test-org"
        project_name = user_name
        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            sec_name_1, sec_name_2 = "key1", "key2"
            sec1 = Secret(sec_name_1, user_name, cluster_name)
            sec2 = Secret(sec_name_2, user_name, cluster_name)
            assert sec1.k8s_secret_name == sec2.k8s_secret_name
            k8s_sec_name = sec1.k8s_secret_name

            await kube_client.core_v1.secret.create_or_update(
                V1Secret(
                    api_version="v1",
                    metadata=V1ObjectMeta(name=k8s_sec_name),
                    data={sec_name_1: "vvvv", sec_name_2: "vvvv"},
                )
            )

            sec_path, sec_file = PurePath("/foo/bar"), "secret.txt"

            container = Container(
                image="ubuntu:20.10",
                command="sleep infinity",
                resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
                http_server=ContainerHTTPServer(port=80),
                secret_volumes=[
                    SecretContainerVolume(secret=sec1, dst_path=sec_path / sec_file),
                    SecretContainerVolume(secret=sec2, dst_path=sec_path / sec_file),
                ],
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(container),
                    cluster_name=cluster_name,
                    org_name="test-org",
                    owner=user_name,
                ),
            )
            with pytest.raises(JobError):
                await job.start()

    async def test_job_pod_with_secret_volumes_overlapping_mounts_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        org_name = "test-org"
        project_name = user_name

        async with kube_client_selector.get_client(
            org_name=org_name, project_name=project_name
        ) as kube_client:
            secret_a = Secret("aaa", user_name, cluster_name)
            secret_b1 = Secret("bbb-1", user_name, cluster_name)
            secret_b2 = Secret("bbb-2", user_name, cluster_name)
            secret_bc = Secret("bbb-ccc", user_name, cluster_name)

            k8s_sec_name = secret_a.k8s_secret_name
            assert all(
                s.k8s_secret_name == k8s_sec_name
                for s in [secret_a, secret_b1, secret_b2, secret_bc]
            )

            await kube_client.core_v1.secret.create_or_update(
                V1Secret(
                    api_version="v1",
                    metadata=V1ObjectMeta(name=k8s_sec_name),
                    data={
                        secret_a.secret_key: "vvvv",
                        secret_b1.secret_key: "vvvv",
                        secret_b2.secret_key: "vvvv",
                        secret_bc.secret_key: "vvvv",
                    },
                )
            )

            path_a = PurePath("/aaa")
            path_b = PurePath("/bbb")
            path_bc = PurePath("/bbb/ccc")

            file_a = "secret1.txt"
            file_b1 = "secret2.txt"
            file_b2 = "secret3.txt"
            file_bc = "secret4.txt"

            container = Container(
                image="ubuntu:20.10",
                command="sleep infinity",
                resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
                http_server=ContainerHTTPServer(port=80),
                secret_volumes=[
                    SecretContainerVolume(secret_a, dst_path=path_a / file_a),
                    SecretContainerVolume(secret_b1, dst_path=path_b / file_b1),
                    SecretContainerVolume(secret_b2, dst_path=path_b / file_b2),
                    SecretContainerVolume(secret_bc, dst_path=path_bc / file_bc),
                ],
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(container),
                    cluster_name=cluster_name,
                    org_name="test-org",
                    owner=user_name,
                ),
            )
            await delete_job_later(job)
            await job.start()

            pod_name = job.id
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            pod = await kube_client.core_v1.pod.get(pod_name)
            assert pod.spec is not None
            sec_volumes_raw = [v for v in pod.spec.volumes if v.name == k8s_sec_name]
            assert sec_volumes_raw[0].name == k8s_sec_name
            assert sec_volumes_raw[0].secret.secret_name == k8s_sec_name
            assert sec_volumes_raw[0].secret.default_mode == 0o400

            container_pod = pod.spec.containers[0]

            for volume_mount in container_pod.volume_mounts:
                assert volume_mount.name == k8s_sec_name
                assert volume_mount.read_only is True

            assert container_pod.volume_mounts[0].mount_path == str(path_a / file_a)
            assert container_pod.volume_mounts[0].sub_path == secret_a.secret_key

            assert container_pod.volume_mounts[1].mount_path == str(path_b / file_b1)
            assert container_pod.volume_mounts[1].sub_path == secret_b1.secret_key

            assert container_pod.volume_mounts[2].mount_path == str(path_b / file_b2)
            assert container_pod.volume_mounts[2].sub_path == secret_b2.secret_key

            assert container_pod.volume_mounts[3].mount_path == str(path_bc / file_bc)
            assert container_pod.volume_mounts[3].sub_path == secret_bc.secret_key

    async def test_cleanup_old_named_ingresses(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="sleep 1h",
            http_server=ContainerHTTPServer(80),
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
        )
        name = f"job-{uuid.uuid4().hex[:6]}"
        job1 = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=name,
                owner="owner1",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        job2 = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=name,
                owner="owner2",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        await delete_job_later(job1)
        await kube_orchestrator.start_job(job1)

        await delete_job_later(job2)
        await kube_orchestrator.start_job(job2)

        async with (
            kube_client_selector.get_client(
                org_name=job1.org_name,
                project_name=job1.project_name,
            ) as kube_client_1,
            kube_client_selector.get_client(
                org_name=job2.org_name,
                project_name=job2.project_name,
            ) as kube_client_2,
        ):
            await get_ingress(kube_client_1, job1.id)
            await get_ingress(kube_client_2, job2.id)

            job3 = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    name=name,
                    owner="owner1",
                    request=JobRequest.create(container),
                    cluster_name="test-cluster",
                    org_name="test-org",
                ),
            )
            async with kube_client_selector.get_client(
                org_name=job3.org_name,
                project_name=job3.project_name,
            ) as kube_client_3:
                await delete_job_later(job3)
                await kube_orchestrator.start_job(job3)

                with pytest.raises(ResourceNotFound):
                    await get_ingress(kube_client_1, job1.id)
                await get_ingress(kube_client_2, job2.id)
                await get_ingress(kube_client_3, job3.id)

    @pytest.fixture
    async def node_resources(
        self, kube_client: KubeClient, kube_node: str
    ) -> NodeResources:
        node = await kube_client.get_node(kube_node)
        return node.status.allocatable_resources

    @pytest.fixture
    async def start_watchers(
        self, kube_client: MyKubeClient, kube_orchestrator: KubeOrchestrator
    ) -> AsyncIterator[None]:
        node_watcher = NodeWatcher(kube_client)
        pod_watcher = PodWatcher(kube_client)
        kube_orchestrator.subscribe_to_kube_events(node_watcher, pod_watcher)
        exit_stack = AsyncExitStack()
        await exit_stack.enter_async_context(node_watcher)
        await exit_stack.enter_async_context(pod_watcher)
        async with exit_stack:
            yield

    @pytest.mark.usefixtures("start_watchers")
    async def test_get_scheduled_jobs(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            resources=ContainerResources(cpu=0.1, memory=64 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=f"job-{uuid.uuid4().hex[:6]}",
                owner="owner1",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )

        scheduled = await kube_orchestrator.get_scheduled_jobs([job])
        assert scheduled == []

        await kube_orchestrator.start_job(job)
        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=job.id, timeout_s=60.0)

        scheduled = await kube_orchestrator.get_scheduled_jobs([job])
        assert scheduled == [job]

    @pytest.mark.usefixtures("start_watchers")
    async def test_get_schedulable_jobs(
        self, kube_orchestrator: KubeOrchestrator, node_resources: NodeResources
    ) -> None:
        # Schedulable
        container = Container(
            image="ubuntu:20.10",
            resources=ContainerResources(cpu=0, memory=128 * 10**6),
        )
        job1 = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=f"job-{uuid.uuid4().hex[:6]}",
                owner="owner1",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        # Not schedulable
        container = Container(
            image="ubuntu:20.10",
            resources=ContainerResources(
                cpu=node_resources.cpu, memory=node_resources.memory
            ),
        )
        job2 = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=f"job-{uuid.uuid4().hex[:6]}",
                owner="owner1",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        # Won't fit into cluster
        container = Container(
            image="ubuntu:20.10",
            resources=ContainerResources(cpu=0.1, memory=10 * 10**6**10),
        )
        job3 = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=f"job-{uuid.uuid4().hex[:6]}",
                owner="owner1",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        jobs = await kube_orchestrator.get_schedulable_jobs([job1, job2, job3])

        assert jobs == [job1]

    async def _create_pvc(
        self, kube_client: KubeClientProxy, name: str, labels: dict[str, str]
    ) -> None:
        await kube_client.core_v1.persistent_volume_claim.create(
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(
                    name=name,
                    namespace=kube_client._namespace,
                    labels=labels,
                ),
                spec=V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    volume_mode="Filesystem",
                    resources=V1VolumeResourceRequirements(
                        requests={"storage": str(1024 * 1024)}
                    ),
                    storage_class_name="test-storage-class",
                ),
            )
        )

    async def _create_failed_attach_volume_event(
        self,
        client_proxy: KubeClientProxy,
        pod_id: str,
    ) -> None:
        namespace = client_proxy._namespace
        now = datetime.now(timezone.utc)  # noqa: UP017
        await client_proxy.core_v1.event.create(
            CoreV1Event(
                api_version="v1",
                count=1,
                first_timestamp=now,
                involved_object=V1ObjectReference(
                    api_version="v1",
                    kind="Pod",
                    name=pod_id,
                    namespace=namespace,
                    resource_version="48102193",
                    uid="eddfe678-86e9-11e9-9d65-42010a800018",
                ),
                kind="Event",
                last_timestamp=now,
                message="FailedAttachVolume",
                metadata=V1ObjectMeta(
                    creation_timestamp=now,
                    name=f"{pod_id}.{uuid.uuid4()}",
                    namespace=namespace,
                    self_link=(
                        f"/api/v1/namespaces/{namespace}/events/{pod_id}.15a870d7e2bb228b"
                    ),
                    uid="cb886f64-8f96-11e9-9251-42010a800038",
                ),
                reason="FailedAttachVolume",
                reporting_component="",
                reporting_instance="",
                source=V1EventSource(component="attachdetach-controller"),
                type="Warning",
            )
        )

    async def _create_triggered_scaleup_event(
        self,
        client_proxy: KubeClientProxy,
        pod_id: str,
    ) -> None:
        namespace = client_proxy._namespace
        now = datetime.now(timezone.utc)  # noqa: UP017
        await client_proxy.core_v1.event.create(
            CoreV1Event(
                api_version="v1",
                count=1,
                first_timestamp=now,
                involved_object=V1ObjectReference(
                    api_version="v1",
                    kind="Pod",
                    name=pod_id,
                    namespace=namespace,
                    resource_version="48102193",
                    uid="eddfe678-86e9-11e9-9d65-42010a800018",
                ),
                kind="Event",
                last_timestamp=now,
                message="TriggeredScaleUp",
                metadata=V1ObjectMeta(
                    creation_timestamp=now,
                    name=f"{pod_id}.{uuid.uuid4()}",
                    namespace=namespace,
                    self_link=(
                        f"/api/v1/namespaces/{namespace}/events/{{pod_id}}.15a870d7e2bb228b"
                    ),
                    uid="cb886f64-8f96-11e9-9251-42010a800038",
                ),
                reason="TriggeredScaleUp",
                reporting_component="",
                reporting_instance="",
                source=V1EventSource(component="cluster-autoscaler"),
                type="Normal",
            )
        )


class TestAffinityFixtures:
    @pytest.fixture
    def kube_orchestrator_factory(
        self,
        registry_config: RegistryConfig,
        orchestrator_config: OrchestratorConfig,
        kube_config: KubeConfig,
        kube_job_nodes_factory: Callable[
            [OrchestratorConfig, KubeConfig], Awaitable[None]
        ],
        kube_client_selector: KubeClientSelector,
    ) -> Callable[[Sequence[ResourcePoolType]], Awaitable[KubeOrchestrator]]:
        kube_config = replace(
            kube_config,
            node_label_job="job",
            node_label_node_pool="nodepool",
            node_label_preemptible="preemptible",
        )

        async def _create(
            resource_pool_types: Sequence[ResourcePoolType],
            presets: Sequence[ResourcePreset] | None = None,
        ) -> KubeOrchestrator:
            orchestrator = replace(
                orchestrator_config, resource_pool_types=resource_pool_types
            )
            if presets is not None:
                orchestrator = replace(orchestrator, resource_presets=presets)
            await kube_job_nodes_factory(orchestrator, kube_config)

            return KubeOrchestrator(
                cluster_name="default",
                registry_config=registry_config,
                orchestrator_config=orchestrator,
                kube_config=kube_config,
                kube_client_selector=kube_client_selector,
            )

        return _create

    @pytest.fixture
    async def kube_orchestrator(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator_factory: Callable[..., Awaitable[KubeOrchestrator]],
    ) -> KubeOrchestrator:
        return await kube_orchestrator_factory(
            [
                ResourcePoolType(
                    name="cpu-small",
                    cpu=2,
                    available_cpu=2,
                    memory=2048 * 10**6,
                    available_memory=2048 * 10**6,
                ),
                ResourcePoolType(
                    name="cpu-small-p",
                    cpu=2,
                    available_cpu=2,
                    memory=2048 * 10**6,
                    available_memory=2048 * 10**6,
                    is_preemptible=True,
                ),
                ResourcePoolType(
                    name="cpu-large-tpu",
                    cpu=3,
                    available_cpu=3,
                    memory=14336 * 10**6,
                    available_memory=14336 * 10**6,
                    tpu=TPUResource(
                        ipv4_cidr_block="1.1.1.1/32",
                        types=("v2-8",),
                        software_versions=("1.14",),
                    ),
                ),
                ResourcePoolType(
                    name="nvidia-gpu",
                    cpu=7,
                    available_cpu=7,
                    memory=61440 * 10**6,
                    available_memory=61440 * 10**6,
                    nvidia_gpu=NvidiaGPU(count=8, model="nvidia-gpu"),
                ),
                ResourcePoolType(
                    name="nvidia-gpu-p",
                    cpu=7,
                    available_cpu=7,
                    memory=61440 * 10**6,
                    available_memory=61440 * 10**6,
                    nvidia_gpu=NvidiaGPU(count=8, model="nvidia-gpu"),
                    is_preemptible=True,
                ),
            ],
            [
                ResourcePreset(
                    name="cpu",
                    credits_per_hour=Decimal("0"),
                    cpu=0.1,
                    memory=100 * 10**6,
                    available_resource_pool_names=["cpu-small"],
                ),
                ResourcePreset(
                    name="unschedulable",
                    credits_per_hour=Decimal("0"),
                    cpu=0.1,
                    memory=100 * 10**6,
                    available_resource_pool_names=[],
                ),
            ],
        )

    @pytest.fixture
    async def kube_orchestrator_gpu(
        self,
        kube_orchestrator_factory: Callable[
            [Sequence[ResourcePoolType]], Awaitable[KubeOrchestrator]
        ],
    ) -> KubeOrchestrator:
        return await kube_orchestrator_factory(
            [
                ResourcePoolType(
                    name="nvidia-gpu",
                    cpu=7,
                    available_cpu=7,
                    memory=61440 * 10**6,
                    available_memory=61440 * 10**6,
                    nvidia_gpu=NvidiaGPU(count=8, model="nvidia-gpu"),
                ),
            ],
        )

    @pytest.fixture
    async def start_job(
        self, kube_client_selector: KubeClientSelector
    ) -> Callable[..., AbstractAsyncContextManager[MyJob]]:
        @asynccontextmanager
        async def _create(
            kube_orchestrator: KubeOrchestrator,
            cpu: float = 0.1,
            memory: int = 128 * 10**6,
            nvidia_gpu: int | None = None,
            amd_gpu: int | None = None,
            intel_gpu: int | None = None,
            scheduler_enabled: bool = False,
            preemptible_node: bool = False,
            preset_name: str | None = None,
        ) -> AsyncIterator[MyJob]:
            container = Container(
                image="ubuntu:20.10",
                command="true",
                resources=ContainerResources(
                    cpu=cpu,
                    memory=memory,
                    nvidia_gpu=nvidia_gpu,
                    amd_gpu=amd_gpu,
                    intel_gpu=intel_gpu,
                ),
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    name=f"job-{uuid.uuid4().hex[:6]}",
                    owner="owner1",
                    request=JobRequest.create(container),
                    cluster_name="test-cluster",
                    org_name="test-org",
                    scheduler_enabled=scheduler_enabled,
                    preemptible_node=preemptible_node,
                    preset_name=preset_name,
                ),
            )
            await kube_orchestrator.start_job(job, tolerate_unreachable_node=True)
            yield job
            await kube_orchestrator.delete_job(job)
            # Sometimes pods stuck in terminating state.
            # Force delete to free node resources.
            async with kube_client_selector.get_client(
                org_name=job.org_name, project_name=job.project_name
            ) as kube_client:
                await delete_pod(kube_client, job.id, force=True)
                await wait_pod_non_existent(kube_client, job.id, timeout_s=10)

        return _create


class TestNodeAffinity(TestAffinityFixtures):
    async def test_unschedulable_job_with_preset(
        self,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        with pytest.raises(JobUnschedulableException, match="Job cannot be scheduled"):
            async with start_job(kube_orchestrator, preset_name="unschedulable"):
                pass

    async def test_job_with_unknown_preset(
        self,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        with pytest.raises(JobUnschedulableException, match="Job cannot be scheduled"):
            async with start_job(kube_orchestrator, preset_name="unknown"):
                pass

    async def test_job_with_preset(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(kube_orchestrator, preset_name="cpu") as job:
            async with kube_client_selector.get_client(
                org_name=job.org_name,
                project_name=job.project_name,
            ) as kube_client:
                await wait_pod_scheduled(kube_client, job.id)

                job_pod = await kube_client.core_v1.pod.get(job.id)
                assert job_pod.spec is not None
                assert (
                    job_pod.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution
                    is not None
                )
                node_selector_term = job_pod.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms[  # noqa: E501
                    0
                ]
                match_expression = node_selector_term.match_expressions[0]
                assert match_expression.key == "nodepool"
                assert match_expression.operator == "In"
                assert match_expression.values == ["cpu-small"]

    async def test_unschedulable_job(
        self,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        with pytest.raises(JobUnschedulableException, match="Job cannot be scheduled"):
            async with start_job(kube_orchestrator, cpu=100, memory=32 * 10**6):
                pass

    async def test_cpu_job(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(kube_orchestrator, cpu=0.1, memory=32 * 10**6) as job:
            async with kube_client_selector.get_client(
                org_name=job.org_name, project_name=job.project_name
            ) as kube_client:
                await wait_pod_scheduled(kube_client, job.id)
                job_pod = await kube_client.core_v1.pod.get(job.id)

            assert job_pod.spec is not None
            assert job_pod.spec.affinity is not None
            node_aff = job_pod.spec.affinity.node_affinity
            assert node_aff
            assert (
                node_aff.required_during_scheduling_ignored_during_execution is not None
            )
            terms = (
                node_aff.required_during_scheduling_ignored_during_execution.node_selector_terms
                or []
            )  # noqa: E501
            actual_values: set[str] = set()
            for term in terms:
                for expr in term.match_expressions or []:
                    if expr.key == "nodepool" and (expr.values or []):
                        actual_values.update(expr.values)
            assert actual_values == {"cpu-small", "cpu-large-tpu"}

    async def test_cpu_job_on_gpu_node(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator_gpu: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(kube_orchestrator_gpu, cpu=0.1, memory=32 * 10**6) as job:
            async with kube_client_selector.get_client(
                org_name=job.org_name, project_name=job.project_name
            ) as kube_client:
                await wait_pod_scheduled(kube_client, job.id, "nvidia-gpu")
                job_pod = await kube_client.core_v1.pod.get(job.id)

            assert job_pod.spec is not None
            node_aff = job_pod.spec.affinity.node_affinity
            assert node_aff
            assert node_aff.required_during_scheduling_ignored_during_execution
            terms = (
                node_aff.required_during_scheduling_ignored_during_execution.node_selector_terms
                or []
            )  # noqa: E501
            actual_values: set[str] = set()
            for term in terms:
                for expr in term.match_expressions or []:
                    if expr.key == "nodepool" and (expr.values or []):
                        actual_values.update(expr.values)
            assert actual_values == {"nvidia-gpu"}

    async def test_gpu_job(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator,
            cpu=0.1,
            memory=32 * 10**6,
            nvidia_gpu=1,
        ) as job:
            async with kube_client_selector.get_client(
                org_name=job.org_name, project_name=job.project_name
            ) as kube_client:
                await wait_pod_scheduled(kube_client, job.id, "nvidia-gpu")
                job_pod = await kube_client.core_v1.pod.get(job.id)

            assert job_pod.spec is not None
            node_aff = job_pod.spec.affinity.node_affinity
            assert node_aff
            assert node_aff.required_during_scheduling_ignored_during_execution
            terms = (
                node_aff.required_during_scheduling_ignored_during_execution.node_selector_terms
                or []
            )  # noqa: E501
            actual_values: set[str] = set()
            for term in terms:
                for expr in term.match_expressions or []:
                    if expr.key == "nodepool" and (expr.values or []):
                        actual_values.update(expr.values)
            assert actual_values == {"nvidia-gpu"}

    async def test_scheduled_job_on_not_preemptible_node(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator,
            cpu=0.1,
            memory=32 * 10**6,
            nvidia_gpu=1,
            scheduler_enabled=True,
        ) as job:
            async with kube_client_selector.get_client(
                org_name=job.org_name, project_name=job.project_name
            ) as kube_client:
                await wait_pod_scheduled(kube_client, job.id, "nvidia-gpu")
                job_pod = await kube_client.core_v1.pod.get(job.id)

            assert job_pod.spec is not None
            node_aff = job_pod.spec.affinity.node_affinity
            assert node_aff
            assert node_aff.required_during_scheduling_ignored_during_execution
            terms = (
                node_aff.required_during_scheduling_ignored_during_execution.node_selector_terms
                or []
            )  # noqa: E501
            actual_values: set[str] = set()
            for term in terms:
                for expr in term.match_expressions or []:
                    if expr.key == "nodepool" and (expr.values or []):
                        actual_values.update(expr.values)
            assert actual_values == {"nvidia-gpu"}
            preferred = (
                node_aff.preferred_during_scheduling_ignored_during_execution or []
            )
            assert preferred == []

    async def test_preemptible_job_on_preemptible_node(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator,
            cpu=0.1,
            memory=32 * 10**6,
            preemptible_node=True,
        ) as job:
            async with kube_client_selector.get_client(
                org_name=job.org_name, project_name=job.project_name
            ) as kube_client:
                await wait_pod_scheduled(kube_client, job.id, "cpu-small-p")
                job_pod = await kube_client.core_v1.pod.get(job.id)

            assert job_pod.spec is not None
            node_aff = job_pod.spec.affinity.node_affinity
            assert node_aff
            assert node_aff.required_during_scheduling_ignored_during_execution
            terms = (
                node_aff.required_during_scheduling_ignored_during_execution.node_selector_terms
                or []
            )  # noqa: E501
            actual_values: set[str] = set()
            for term in terms:
                for expr in term.match_expressions or []:
                    if expr.key == "nodepool" and (expr.values or []):
                        actual_values.update(expr.values)
            assert actual_values == {"cpu-small-p"}


class TestPodAffinity(TestAffinityFixtures):
    async def test_cpu_job(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(kube_orchestrator, cpu=0.1, memory=32 * 10**6) as job:
            async with kube_client_selector.get_client(
                org_name=job.org_name, project_name=job.project_name
            ) as kube_client:
                await wait_pod_scheduled(kube_client, job.id)
                job_pod = await kube_client.core_v1.pod.get(job.id)

            assert job_pod.spec is not None
            affinity = job_pod.spec.affinity
            assert affinity
            assert affinity.pod_affinity
            preferred = (
                affinity.pod_affinity.preferred_during_scheduling_ignored_during_execution
                or []
            )
            found = False
            for item in preferred:
                term = item.pod_affinity_term
                if not term or not term.label_selector:
                    continue
                exprs = term.label_selector.match_expressions or []
                if any(
                    e.key == "platform.neuromation.io/job" and e.operator == "Exists"
                    for e in exprs
                ):
                    assert term.topology_key == "kubernetes.io/hostname"
                    found = True
                    break
            assert found


@pytest.fixture
async def mock_kubernetes_server() -> AsyncIterator[ApiConfig]:
    async def _get_pod(request: web.Request) -> web.Response:
        payload: dict[str, Any] = {
            "kind": "Pod",
            "metadata": {
                "name": "testname",
                "creationTimestamp": "2019-06-20T11:03:32Z",
            },
            "spec": {
                "containers": [{"name": "testname", "image": "testimage"}],
                "nodeName": "whatever",
            },
            "status": {"phase": "Running"},
        }

        return web.json_response(payload)

    async def _stats_summary(request: web.Request) -> web.Response:
        # Explicitly return plain text to trigger ContentTypeError
        return web.Response(content_type="text/plain")

    def _create_app() -> web.Application:
        app = web.Application()
        app.add_routes(
            [
                web.get("/api/v1/namespaces/mock/pods/whatever", _get_pod),
                web.get(
                    "/api/v1/nodes/whatever:10255/proxy/stats/summary", _stats_summary
                ),
            ]
        )
        return app

    app = _create_app()
    runner = ApiRunner(app, port=8080)
    api_address = await runner.run()
    api_config = ApiConfig(host=api_address.host, port=api_address.port, runner=runner)
    yield api_config
    await runner.close()


class TestPodContainerDevShmSettings:
    @pytest.fixture
    async def run_command_get_status(
        self,
        kube_orchestrator: KubeOrchestrator,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> Callable[..., Awaitable[JobStatusItem]]:
        async def _f(
            kube_client: KubeClientProxy, resources: ContainerResources, command: str
        ) -> JobStatusItem:
            container = Container(
                image="ubuntu:20.10", command=command, resources=resources
            )
            job_request = JobRequest.create(container)
            pod = PodDescriptor.from_job_request(job_request)
            await delete_pod_later(pod)
            await create_pod(kube_client, pod)
            await wait_pod_is_terminated(kube_client, pod_name=pod.name, timeout_s=60.0)
            pod_status = await get_pod_status(kube_client, pod.name)
            return JobStatusItemFactory(pod_status).create()

        return _f

    @pytest.fixture
    def command_assert_shm_64_mb(self) -> str:
        df = "/bin/df --block-size M --output=avail /dev/shm"
        return f"/bin/bash -c '{df} | grep -q \"^\\s*64M\"'"

    @pytest.mark.skip(reason="Temporarily skipped - pod termination timeout issues")
    async def test_shm_extended_request_parameter_omitted(
        self,
        run_command_get_status: Callable[..., Awaitable[JobStatusItem]],
        command_assert_shm_64_mb: str,
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory=128 * 10**6)
        status_item = await run_command_get_status(resources, command_assert_shm_64_mb)
        assert status_item.status == JobStatus.SUCCEEDED

    @pytest.mark.skip(reason="Temporarily skipped - pod termination timeout issues")
    async def test_shm_extended_request_parameter_not_requested(
        self,
        run_command_get_status: Callable[..., Awaitable[JobStatusItem]],
        command_assert_shm_64_mb: str,
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory=128 * 10**6, shm=False)
        status_item = await run_command_get_status(resources, command_assert_shm_64_mb)
        assert status_item.status == JobStatus.SUCCEEDED

    @pytest.mark.skip(reason="Temporarily skipped - pod termination timeout issues")
    async def test_shm_extended_request_parameter_requested(
        self,
        run_command_get_status: Callable[..., Awaitable[JobStatusItem]],
        command_assert_shm_64_mb: str,
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory=128 * 10**6, shm=True)
        status_item = await run_command_get_status(resources, command_assert_shm_64_mb)
        assert status_item.status == JobStatus.FAILED

    @pytest.mark.skip(reason="Temporarily skipped - pod termination timeout issues")
    async def test_shm_extended_not_requested_try_create_huge_file(
        self, run_command_get_status: Callable[..., Awaitable[JobStatusItem]]
    ) -> None:
        command = "dd if=/dev/zero of=/dev/zero  bs=999999M  count=1"
        resources = ContainerResources(cpu=0.1, memory=128 * 10**6, shm=False)
        status_actual = await run_command_get_status(resources, command)
        status_expected = JobStatusItem.create(
            status=JobStatus.FAILED,
            reason=JobStatusReason.OOM_KILLED,
            exit_code=137,
            description=mock.ANY,
        )
        assert status_actual == status_expected, f"actual: '{status_actual}'"

    @pytest.mark.skip(reason="Temporarily skipped - pod termination timeout issues")
    async def test_shm_extended_requested_try_create_huge_file(
        self, run_command_get_status: Callable[..., Awaitable[JobStatusItem]]
    ) -> None:
        command = "dd if=/dev/zero of=/dev/shm/test bs=256M  count=1"
        resources = ContainerResources(cpu=0.1, memory=1024 * 10**6, shm=True)
        status_actual = await run_command_get_status(resources, command)
        status_expected = JobStatusItem.create(status=JobStatus.SUCCEEDED, exit_code=0)
        assert status_actual == status_expected, f"actual: '{status_actual}'"

    @pytest.mark.skip(reason="Temporarily skipped - pod termination timeout issues")
    async def test_shm_extended_not_requested_try_create_small_file(
        self, run_command_get_status: Callable[..., Awaitable[JobStatusItem]]
    ) -> None:
        command = "dd if=/dev/zero of=/dev/shm/test  bs=32M  count=1"
        resources = ContainerResources(cpu=0.1, memory=128 * 10**6, shm=False)
        status_actual = await run_command_get_status(resources, command)
        status_expected = JobStatusItem.create(status=JobStatus.SUCCEEDED, exit_code=0)
        assert status_actual == status_expected, f"actual: '{status_actual}'"


class TestPreemption:
    @pytest.fixture
    async def kube_config(
        self, kube_config_factory: Callable[..., KubeConfig]
    ) -> KubeConfig:
        return kube_config_factory(
            node_label_node_pool="nodepool",
            node_label_preemptible="preemptible",
            jobs_pod_preemptible_toleration_key="preemptible-taint",
        )

    @pytest.fixture
    async def kube_client(
        self,
        kube_config: KubeConfig,
    ) -> AsyncIterator[KubeClient]:
        client = MyKubeClient(
            base_url=kube_config.endpoint_url,
            auth_type=kube_config.auth_type,
            cert_authority_data_pem=kube_config.cert_authority_data_pem,
            cert_authority_path=kube_config.cert_authority_path,
            auth_cert_path=kube_config.auth_cert_path,
            auth_cert_key_path=kube_config.auth_cert_key_path,
            namespace=kube_config.namespace,
            conn_timeout_s=kube_config.client_conn_timeout_s,
            read_timeout_s=kube_config.client_read_timeout_s,
            conn_pool_size=kube_config.client_conn_pool_size,
        )
        async with client as cl:
            yield cl

    @pytest.fixture
    async def kube_orchestrator(
        self,
        kube_config: KubeConfig,
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        orchestrator_config_factory: Callable[..., OrchestratorConfig],
    ) -> KubeOrchestrator:
        resources = [
            ResourcePoolType(
                name="cpu-small",
                cpu=2,
                available_cpu=2,
                memory=2048 * 10**6,
                available_memory=2048 * 10**6,
                is_preemptible=False,
            ),
            ResourcePoolType(
                name="cpu-small-p",
                cpu=2,
                available_cpu=2,
                memory=2048 * 10**6,
                available_memory=2048 * 10**6,
                is_preemptible=True,
            ),
        ]
        orchestrator_config = orchestrator_config_factory(resource_pool_types=resources)
        return kube_orchestrator_factory(
            kube_config=kube_config,
            orchestrator_config=orchestrator_config,
        )

    @pytest.fixture
    async def kube_main_node_cpu_regular_labels(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
    ) -> AsyncIterator[None]:
        assert kube_orchestrator.kube_config.node_label_node_pool
        labels = {kube_orchestrator.kube_config.node_label_node_pool: "cpu-small"}
        # driver=docker or driver=none
        kube_client = kube_client_selector.host_client
        try:
            node_name = "minikube"
            node = await kube_client.core_v1.node.get(node_name)
        except ResourceNotFound:
            node_name = os.uname()[1]
            node = await kube_client.core_v1.node.get(node_name)

        node.metadata.labels |= labels
        await kube_client.core_v1.node.update(node)

        yield

        node = await kube_client.core_v1.node.get(node_name)
        node.metadata.labels = {
            k: v for k, v in node.metadata.labels.items() if k not in labels
        }
        await kube_client.core_v1.node.update(node)

    @pytest.fixture
    async def kube_main_node_cpu_preemptible_labels(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
    ) -> AsyncIterator[None]:
        assert kube_orchestrator.kube_config.node_label_node_pool
        assert kube_orchestrator.kube_config.node_label_preemptible

        labels = {
            kube_orchestrator.kube_config.node_label_node_pool: "cpu-small-p",
            kube_orchestrator.kube_config.node_label_preemptible: "true",
        }
        # driver=docker or driver=none
        kube_client = kube_client_selector.host_client
        try:
            node_name = "minikube"
            node = await kube_client.core_v1.node.get(node_name)
        except ResourceNotFound:
            node_name = os.uname()[1]
            node = await kube_client.core_v1.node.get(node_name)

        node.metadata.labels |= labels
        await kube_client.core_v1.node.update(node)

        yield

        node = await kube_client.core_v1.node.get(node_name)
        node.metadata.labels = {
            k: v for k, v in node.metadata.labels.items() if k not in labels
        }
        await kube_client.core_v1.node.update(node)

    @pytest.fixture
    async def kube_node_preemptible(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        delete_node_later: Callable[[str], Awaitable[None]],
        default_node_capacity: dict[str, str],
    ) -> AsyncIterator[str]:
        node_name = str(uuid.uuid4())
        kube_config = kube_orchestrator.kube_config

        assert kube_config.node_label_node_pool
        assert kube_config.node_label_preemptible
        assert kube_config.jobs_pod_preemptible_toleration_key

        await delete_node_later(node_name)

        labels = {
            kube_config.node_label_preemptible: "true",
            kube_config.node_label_node_pool: "cpu-small-p",
        }
        taints = [
            NodeTaint(
                key=kube_config.jobs_pod_preemptible_toleration_key, value="present"
            )
        ]
        await kube_client_selector.host_client.core_v1.node.create(
            V1Node(
                api_version="v1",
                kind="Node",
                metadata=V1ObjectMeta(
                    name=node_name,
                    labels=labels,
                ),
                spec=V1NodeSpec(taints=[taint.to_model() for taint in taints]),
                status=V1NodeStatus(
                    capacity=default_node_capacity,
                    conditions=[V1NodeCondition(status="True", type="Ready")],
                ),
            )
        )
        yield node_name

    async def test_non_preemptible_job(
        self,
        kube_config: KubeConfig,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_main_node_cpu_regular_labels: str,
        kube_node_preemptible: str,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.is_running

            await delete_pod(kube_client, pod_name, force=True)

            # triggering pod recreation
            with pytest.raises(JobNotFoundException, match="was not found"):
                await kube_orchestrator.get_job_status(job)

    async def test_scheduled_job_lost_running_pod(
        self,
        kube_config: KubeConfig,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_main_node_cpu_regular_labels: str,
        kube_node_preemptible: str,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                # marking the job as scheduled
                scheduler_enabled=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.is_running

            await delete_pod(kube_client, pod_name, force=True)

            # triggering pod recreation
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.is_pending

            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.is_running

    async def test_preemptible_job_lost_running_pod(
        self,
        kube_config: KubeConfig,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_main_node_cpu_preemptible_labels: str,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.is_running

            await delete_pod(kube_client, pod_name, force=True)

            # triggering pod recreation
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.is_pending

            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.is_running

    async def test_preemptible_job_lost_node_lost_pod(
        self,
        kube_config: KubeConfig,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_main_node_cpu_regular_labels: str,
        kube_node_preemptible: str,
    ) -> None:
        node_name = kube_node_preemptible
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_scheduled(kube_client, pod_name, node_name)

            await kube_client_selector.host_client.core_v1.node.delete(node_name)
            # deleting node initiates it's pods deletion
            await wait_pod_non_existent(kube_client, pod_name, timeout_s=60.0)

            # triggering pod recreation
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.is_pending

    async def test_preemptible_job_pending_pod_node_not_ready(
        self,
        kube_config: KubeConfig,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_main_node_cpu_regular_labels: str,
        kube_node_preemptible: str,
    ) -> None:
        node_name = kube_node_preemptible
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_scheduled(kube_client, pod_name, node_name)

            raw_pod = await kube_client.core_v1.pod.get(pod_name)

            raw_pod.status.reason = "NodeLost"
            await kube_client.core_v1.pod[pod_name].status.update(raw_pod)

            # re-fetch
            for _attempt in range(100):
                pod = await kube_client.core_v1.pod.get(name=pod_name)
                if pod.status.reason is None:
                    await asyncio.sleep(0.01)
                    continue
                assert pod.status.reason == "NodeLost"
                break
            else:
                raise AssertionError("Waiting for status update has failed")

            # triggering pod recreation
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.is_pending

            await wait_pod_scheduled(kube_client, pod_name, node_name)

            raw_pod = await kube_client.core_v1.pod.get(pod_name)
            assert not raw_pod.status.reason

    async def test_preemptible_job_recreation_failed(
        self,
        kube_config: KubeConfig,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_main_node_cpu_regular_labels: str,
        kube_node_preemptible: str,
    ) -> None:
        node_name = kube_node_preemptible
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_scheduled(kube_client, pod_name, node_name)
            await delete_pod(kube_client, pod_name, force=True)

        # changing the job details to trigger pod creation failure
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory=-128),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest(job_id=job.id, container=container),
                cluster_name="test-cluster",
                org_name="test-org",
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )

        # triggering pod recreation.
        # this will fail because of the negative memory value (raises 422)
        with pytest.raises(JobError):
            await kube_orchestrator.get_job_status(job)


class TestRestartPolicy:
    async def test_restart_failing(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="false",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                restart_policy=JobRestartPolicy.ON_FAILURE,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

        status = await kube_orchestrator.get_job_status(job)
        assert status in (
            JobStatusItem.create(
                status=JobStatus.RUNNING, reason=JobStatusReason.RESTARTING
            ),
            JobStatusItem.create(status=JobStatus.RUNNING),
        )

    async def test_restart_failing_succeeded(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="true",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                restart_policy=JobRestartPolicy.ON_FAILURE,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_terminated(kube_client, pod_name=pod_name, timeout_s=60.0)

        status = await kube_orchestrator.get_job_status(job)
        assert status == JobStatusItem.create(status=JobStatus.SUCCEEDED, exit_code=0)

    async def test_restart_always(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="false",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                restart_policy=JobRestartPolicy.ALWAYS,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_terminated(kube_client, pod_name=pod_name, timeout_s=60.0)

        status = await kube_orchestrator.get_job_status(job)
        assert status in (
            JobStatusItem.create(
                status=JobStatus.RUNNING, reason=JobStatusReason.RESTARTING
            ),
            JobStatusItem.create(status=JobStatus.RUNNING),
        )

    async def test_restart_always_succeeded(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="true",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                restart_policy=JobRestartPolicy.ALWAYS,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_terminated(kube_client, pod_name=pod_name, timeout_s=60.0)

        status = await kube_orchestrator.get_job_status(job)
        assert status in (
            JobStatusItem.create(
                status=JobStatus.RUNNING, reason=JobStatusReason.RESTARTING
            ),
            JobStatusItem.create(status=JobStatus.RUNNING),
        )


class TestJobsPreemption:
    @pytest.fixture(autouse=True)
    async def start_watchers(
        self, kube_client: MyKubeClient, kube_orchestrator: KubeOrchestrator
    ) -> AsyncIterator[None]:
        node_watcher = NodeWatcher(kube_client)
        pod_watcher = PodWatcher(kube_client)
        kube_orchestrator.subscribe_to_kube_events(node_watcher, pod_watcher)
        exit_stack = AsyncExitStack()
        await exit_stack.enter_async_context(node_watcher)
        await exit_stack.enter_async_context(pod_watcher)
        async with exit_stack:
            yield

    @pytest.fixture
    async def job_factory(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> Callable[..., Awaitable[Job]]:
        async def _create(
            cpu: float = 0.1,
            memory: int = 128 * 10**6,
            wait: bool = False,
            wait_timeout_s: float = 60,
        ) -> Job:
            container = Container(
                image="gcr.io/google_containers/pause:3.1",
                resources=ContainerResources(cpu=cpu, memory=memory),
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    owner="owner1",
                    request=JobRequest.create(container),
                    cluster_name="test-cluster",
                    org_name="test-org",
                ),
            )
            await kube_orchestrator.start_job(job)
            await delete_job_later(job)
            if wait:
                async with kube_client_selector.get_client(
                    org_name=job.org_name,
                    project_name=job.project_name,
                ) as kube_client:
                    await wait_pod_is_running(
                        kube_client, job.id, timeout_s=wait_timeout_s
                    )
            return job

        return _create

    @pytest.fixture
    async def node_resources(
        self, kube_client: KubeClient, kube_node: str
    ) -> NodeResources:
        node = await kube_client.get_node(kube_node)
        return node.status.allocatable_resources

    async def test_preempt_jobs(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
        node_resources: NodeResources,
    ) -> None:
        preemptible_job = await job_factory(cpu=node_resources.cpu / 2, wait=True)
        # Node should have less than cpu / 2 left
        job = await job_factory(cpu=node_resources.cpu / 2)
        preempted_jobs = await kube_orchestrator.preempt_jobs([job], [preemptible_job])

        assert preempted_jobs == [preemptible_job]

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_deleted(
                kube_client, preemptible_job.id, timeout_s=60, interval_s=0.1
            )

    async def test_cannot_be_scheduled(
        self,
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_config_factory: Callable[..., KubeConfig],
        kube_client_selector: KubeClientSelector,
        job_factory: Callable[..., Awaitable[Job]],
        node_resources: NodeResources,
    ) -> None:
        kube_config = kube_config_factory(node_label_node_pool="nodepool")
        kube_orchestrator = kube_orchestrator_factory(kube_config=kube_config)
        preemptible_job = await job_factory(cpu=node_resources.cpu / 2, wait=True)
        # Node should have less than cpu / 2 left
        job = await job_factory(cpu=node_resources.cpu / 2)
        preempted = await kube_orchestrator.preempt_jobs([job], [preemptible_job])

        assert preempted == []

        async with kube_client_selector.get_client(
            org_name=job.org_name, project_name=job.project_name
        ) as kube_client:
            job_pod = await get_pod(kube_client, job.id)
            assert job_pod.status
            assert job_pod.status.is_phase_pending

    async def test_not_enough_resources(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
        node_resources: NodeResources,
    ) -> None:
        preemptible_job = await job_factory(cpu=node_resources.cpu / 2, wait=True)
        # Node should have less than cpu / 2 left
        job = await job_factory(cpu=node_resources.cpu)
        preempted = await kube_orchestrator.preempt_jobs([job], [preemptible_job])

        assert preempted == []

        async with kube_client_selector.get_client(
            org_name=job.org_name, project_name=job.project_name
        ) as kube_client:
            job_pod = await get_pod(kube_client, job.id)
            assert job_pod.status
            assert job_pod.status.is_phase_pending

    async def test_running_jobs_ignored(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
    ) -> None:
        job = await job_factory()
        preemptible_job = await job_factory(wait=True)
        preempted = await kube_orchestrator.preempt_jobs([job], [preemptible_job])

        assert preempted == []

        async with kube_client_selector.get_client(
            org_name=preemptible_job.org_name, project_name=preemptible_job.project_name
        ) as kube_client:
            preemptible_pod = await get_pod(kube_client, preemptible_job.id)
            assert preemptible_pod.status
            assert preemptible_pod.status.is_scheduled

    async def test_no_preemptible_jobs(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
        node_resources: NodeResources,
    ) -> None:
        # Should not be scheduled
        job = await job_factory(cpu=node_resources.cpu)
        preempted = await kube_orchestrator.preempt_jobs([job], [])

        assert preempted == []

        async with kube_client_selector.get_client(
            org_name=job.org_name, project_name=job.project_name
        ) as kube_client:
            job_pod = await get_pod(kube_client, job.id)
            assert job_pod.status
            assert job_pod.status.is_phase_pending

    async def test_no_jobs(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
    ) -> None:
        preemptible_job = await job_factory(wait=True)
        preempted = await kube_orchestrator.preempt_jobs([], [preemptible_job])

        assert preempted == []

        async with kube_client_selector.get_client(
            org_name=preemptible_job.org_name, project_name=preemptible_job.project_name
        ) as kube_client:
            preemptible_pod = await get_pod(kube_client, preemptible_job.id)
            assert preemptible_pod.status
            assert preemptible_pod.status.is_scheduled


class TestExternalJobs:
    ApiFactory = Callable[..., AbstractAsyncContextManager[ApiAddress]]

    @pytest.fixture(scope="session")
    def external_job_runner_port(self, unused_tcp_port_factory: Any) -> int:
        return unused_tcp_port_factory()

    @pytest.fixture(scope="session")
    async def external_job_runner_factory(
        self, external_job_runner_port: int
    ) -> ApiFactory:
        @asynccontextmanager
        async def _create(status: dict[str, Any]) -> AsyncIterator[ApiAddress]:
            app = aiohttp.web.Application()

            async def handle_status(req: aiohttp.web.Request) -> aiohttp.web.Response:
                return web.json_response(status)

            app.add_routes(
                [
                    aiohttp.web.get("/api/v1/status", handle_status),
                ]
            )

            async with create_local_app_server(
                app, port=external_job_runner_port
            ) as address:
                # patch job internal hostname so it'll point to our local app service
                with patch.object(
                    Job,
                    "internal_hostname",
                    new_callable=PropertyMock,
                ) as mock_prop:
                    mock_prop.return_value = address.host  # use local app host
                    yield address

        return _create

    @pytest.fixture(scope="session")
    async def orchestrator_config(
        self, orchestrator_config_factory: Callable[..., OrchestratorConfig]
    ) -> OrchestratorConfig:
        return orchestrator_config_factory(
            resource_presets=[
                ResourcePreset(
                    name="vast-ai",
                    credits_per_hour=Decimal("0"),
                    cpu=0.1,
                    memory=100 * 10**6,
                    is_external_job=True,
                    available_resource_pool_names=["cpu"],
                ),
            ]
        )

    @pytest.fixture(scope="session")
    async def kube_config(self, kube_config: KubeConfig) -> KubeConfig:
        return replace(
            kube_config,
            external_job_runner_image="ubuntu:20.10",
            external_job_runner_command=["bash"],
            external_job_runner_args=shlex.split("-c 'eval ${CMD:=sleep 300}'"),
        )

    async def test_job_pod_updated(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="not-used",
            command="not-used",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            pod = await get_pod(kube_client, job.id)

        assert pod.image == "ubuntu:20.10"
        assert pod.command == ["bash"]
        assert pod.args == ["-c", "eval ${CMD:=sleep 300}"]
        assert pod.labels["platform.neuromation.io/external"] == "true"

    async def test_job_external_status_fetched(
        self,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        external_job_runner_port: int,
        external_job_runner_factory: ApiFactory,
    ) -> None:
        container = Container(
            image="not-used",
            command="not-used",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            env={"EXTERNAL_JOB_RUNNER_PORT": str(external_job_runner_port)},
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)

        async with external_job_runner_factory({"status": "running"}):
            job_status = await kube_orchestrator.get_job_status(job)

        assert job_status.status == JobStatus.RUNNING
        assert job_status.reason is None
        assert job_status.description is None

        async with external_job_runner_factory(
            {
                "status": "failed",
                "reason": "external reason",
                "description": "external description",
            }
        ):
            job_status = await kube_orchestrator.get_job_status(job)

        assert job_status.status == JobStatus.FAILED
        assert job_status.reason == "external reason"
        assert job_status.description == "external description"

    async def test_job_pending(
        self,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="not-used",
            command="not-used",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)

        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.status == JobStatus.PENDING

    async def test_job_running_if_pod_is_finishing(
        self,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        external_job_runner_port: int,
        external_job_runner_factory: ApiFactory,
    ) -> None:
        container = Container(
            image="not-used",
            command="not-used",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            env={"EXTERNAL_JOB_RUNNER_PORT": str(external_job_runner_port)},
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)

        async with external_job_runner_factory({"status": "running"}):
            job_status = await kube_orchestrator.get_job_status(job)
            assert job_status.status == JobStatus.RUNNING
            job.status = job_status.status

        # Status endpoint not available, job status should remain running
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.status == JobStatus.RUNNING

    async def test_job_succeeded(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="not-used",
            command="not-used",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            env={"CMD": "true"},
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_finished(kube_client, pod_name)

        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.status == JobStatus.SUCCEEDED

    async def test_job_failed(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="not-used",
            command="not-used",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            env={"CMD": "echo -n 'custom message' > /dev/termination-log && false"},
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_finished(kube_client, pod_name)

        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.status == JobStatus.FAILED
        assert job_status.reason == JobStatusReason.ERROR
        assert job_status.description == "custom message"
        assert job_status.exit_code == 1


class TestExternalJobsPreemption:
    @pytest.fixture
    async def kube_config(
        self, kube_config_factory: Callable[..., KubeConfig]
    ) -> KubeConfig:
        return kube_config_factory(
            node_label_node_pool="nodepool",
            node_label_preemptible="preemptible",
            jobs_pod_preemptible_toleration_key="preemptible-taint",
            external_job_runner_image="ubuntu:20.10",
            external_job_runner_command=shlex.split("bash -c 'sleep 300'"),
        )

    @pytest.fixture
    async def kube_client(
        self,
        kube_config: KubeConfig,
    ) -> AsyncIterator[KubeClient]:
        client = MyKubeClient(
            base_url=kube_config.endpoint_url,
            auth_type=kube_config.auth_type,
            cert_authority_data_pem=kube_config.cert_authority_data_pem,
            cert_authority_path=kube_config.cert_authority_path,
            auth_cert_path=kube_config.auth_cert_path,
            auth_cert_key_path=kube_config.auth_cert_key_path,
            namespace=kube_config.namespace,
            conn_timeout_s=kube_config.client_conn_timeout_s,
            read_timeout_s=kube_config.client_read_timeout_s,
            conn_pool_size=kube_config.client_conn_pool_size,
        )
        async with client as cl:
            yield cl

    @pytest.fixture
    async def kube_orchestrator(
        self,
        kube_config: KubeConfig,
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        orchestrator_config_factory: Callable[..., OrchestratorConfig],
    ) -> KubeOrchestrator:
        resources = [
            ResourcePoolType(
                name="cpu-small",
                cpu=2,
                available_cpu=2,
                memory=2048 * 10**6,
                available_memory=2048 * 10**6,
                is_preemptible=False,
            ),
            ResourcePoolType(
                name="cpu-small-p",
                cpu=2,
                available_cpu=2,
                memory=2048 * 10**6,
                available_memory=2048 * 10**6,
                is_preemptible=True,
            ),
        ]
        presets = [
            ResourcePreset(
                name="vast-ai",
                credits_per_hour=Decimal("0"),
                cpu=0.1,
                memory=100 * 10**6,
                is_external_job=True,
                available_resource_pool_names=["cpu-small"],
            ),
            ResourcePreset(
                name="vast-ai-p",
                credits_per_hour=Decimal("0"),
                cpu=0.1,
                memory=100 * 10**6,
                is_external_job=True,
                preemptible_node=True,
                available_resource_pool_names=["cpu-small-p"],
            ),
        ]
        orchestrator_config = orchestrator_config_factory(
            resource_pool_types=resources, resource_presets=presets
        )
        return kube_orchestrator_factory(
            kube_config=kube_config,
            orchestrator_config=orchestrator_config,
        )

    @pytest.fixture
    async def kube_main_node_cpu_regular_labels(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
    ) -> AsyncIterator[None]:
        assert kube_orchestrator.kube_config.node_label_node_pool
        labels = {kube_orchestrator.kube_config.node_label_node_pool: "cpu-small"}
        # driver=docker or driver=none
        kube_client = kube_client_selector.host_client
        try:
            node_name = "minikube"
            node = await kube_client.core_v1.node.get(node_name)
        except ResourceNotFound:
            node_name = os.uname()[1]
            node = await kube_client.core_v1.node.get(node_name)

        node.metadata.labels |= labels
        await kube_client.core_v1.node.update(node)

        yield

        node = await kube_client.core_v1.node.get(node_name)
        node.metadata.labels = {
            k: v for k, v in node.metadata.labels.items() if k not in labels
        }
        await kube_client.core_v1.node.update(node)

    @pytest.fixture
    async def kube_main_node_cpu_preemptible_labels(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
    ) -> AsyncIterator[None]:
        assert kube_orchestrator.kube_config.node_label_node_pool
        assert kube_orchestrator.kube_config.node_label_preemptible

        labels = {
            kube_orchestrator.kube_config.node_label_node_pool: "cpu-small-p",
            kube_orchestrator.kube_config.node_label_preemptible: "true",
        }
        # driver=docker or driver=none
        kube_client = kube_client_selector.host_client
        try:
            node_name = "minikube"
            node = await kube_client.core_v1.node.get(node_name)
        except LegacyResourceNotFound:
            node_name = os.uname()[1]
            node = await kube_client.core_v1.node.get(node_name)

        node.metadata.labels |= labels
        await kube_client.core_v1.node.update(node)

        yield

        node = await kube_client.core_v1.node.get(node_name)
        node.metadata.labels = {
            k: v for k, v in node.metadata.labels.items() if k not in labels
        }
        await kube_client.core_v1.node.update(node)

    @pytest.fixture
    async def kube_node_preemptible(
        self,
        kube_client_selector: KubeClientSelector,
        kube_orchestrator: KubeOrchestrator,
        delete_node_later: Callable[[str], Awaitable[None]],
        default_node_capacity: dict[str, str],
    ) -> AsyncIterator[str]:
        node_name = str(uuid.uuid4())
        kube_config = kube_orchestrator.kube_config

        assert kube_config.node_label_node_pool
        assert kube_config.node_label_preemptible
        assert kube_config.jobs_pod_preemptible_toleration_key

        await delete_node_later(node_name)

        labels = {
            kube_config.node_label_preemptible: "true",
            kube_config.node_label_node_pool: "cpu-small-p",
        }
        taints = [
            NodeTaint(
                key=kube_config.jobs_pod_preemptible_toleration_key, value="present"
            )
        ]
        await kube_client_selector.host_client.core_v1.node.create(
            V1Node(
                api_version="v1",
                kind="Node",
                metadata=V1ObjectMeta(
                    name=node_name,
                    labels=labels,
                ),
                spec=V1NodeSpec(taints=[taint.to_model() for taint in taints]),
                status=V1NodeStatus(
                    capacity=default_node_capacity,
                    conditions=[V1NodeCondition(status="True", type="Ready")],
                ),
            )
        )
        yield node_name

    async def test_job_lost_running_pod(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_main_node_cpu_regular_labels: None,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep 300'",
            resources=ContainerResources(cpu=0.1, memory=96 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

            # triggering pod recreation
            await delete_pod(kube_client, pod_name, force=True)
            await kube_orchestrator.get_job_status(job)

            await wait_pod_is_running(kube_client, pod_name=pod_name, timeout_s=60.0)

    async def test_job_lost_node_lost_pod(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_node_preemptible: str,
    ) -> None:
        node_name = kube_node_preemptible
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep 300'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai-p",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_scheduled(kube_client, pod_name, node_name)

            await kube_client_selector.host_client.core_v1.node.delete(node_name)
            # deleting node initiates it's pods deletion
            await wait_pod_non_existent(kube_client, pod_name, timeout_s=60.0)

            # triggering pod recreation
            await kube_orchestrator.get_job_status(job)
            await kube_client.core_v1.pod.get(pod_name)  # check pod was recreated

    async def test_job_pending_pod_node_not_ready(
        self,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_client_selector: KubeClientSelector,
        kube_node_preemptible: str,
    ) -> None:
        node_name = kube_node_preemptible
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep 300'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai-p",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name, project_name=job.project_name
        ) as client_proxy:
            await wait_pod_scheduled(client_proxy, pod_name, node_name)

            raw_pod = await client_proxy.core_v1.pod.get(pod_name)

            raw_pod.status.reason = "NodeLost"
            await client_proxy.core_v1.pod[pod_name].status.update(raw_pod)

            # re-fetch
            for _attempt in range(100):
                pod = await client_proxy.core_v1.pod.get(name=pod_name)
                if pod.status.reason is None:
                    await asyncio.sleep(0.01)
                    continue
                assert pod.status.reason == "NodeLost"
                break
            else:
                raise AssertionError("Waiting for status update has failed")

            # triggering pod recreation
            await kube_orchestrator.get_job_status(job)
            await wait_pod_scheduled(client_proxy, pod_name, node_name)

            raw_pod = await client_proxy.core_v1.pod.get(pod_name)
            assert not raw_pod.status.reason

    async def test_job_pod_recreation_failed(
        self,
        kube_client_selector: KubeClientSelector,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_node_preemptible: str,
    ) -> None:
        node_name = kube_node_preemptible
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep 300'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai-p",
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        async with kube_client_selector.get_client(
            org_name=job.org_name,
            project_name=job.project_name,
        ) as kube_client:
            await wait_pod_scheduled(kube_client, pod_name, node_name)
            await delete_pod(kube_client, pod_name, force=True)

        # changing the job details to trigger pod creation failure
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep 300'",
            resources=ContainerResources(cpu=0.1, memory=-128),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest(job_id=job.id, container=container),
                cluster_name="test-cluster",
                org_name="test-org",
                preset_name="vast-ai-p",
            ),
        )

        # triggering pod recreation.
        # this will fail because of the negative memory value (raises 422)
        with pytest.raises(JobError):
            await kube_orchestrator.get_job_status(job)


async def wait_pod_non_existent(
    client_proxy: KubeClientProxy,
    pod_name: str,
    timeout_s: float = 5.0,
    interval_s: float = 1.0,
) -> None:
    try:
        async with timeout(timeout_s):
            while True:
                try:
                    await get_pod(client_proxy, pod_name)
                except JobNotFoundException:
                    return
                await asyncio.sleep(interval_s)
    except TimeoutError:
        pytest.fail("Pod still exists")


async def wait_pod_scheduled(
    client_proxy: KubeClientProxy,
    pod_name: str,
    node_name: str = "",
    timeout_s: float = 5.0,
    interval_s: float = 1.0,
) -> None:
    raw_pod: V1Pod | None = None
    try:
        async with timeout(timeout_s):
            while True:
                raw_pod = await client_proxy.core_v1.pod.get(pod_name)
                assert raw_pod.spec is not None
                if node_name:
                    pod_at_node = raw_pod.spec.node_name
                    if pod_at_node == node_name:
                        pod_has_node = True
                    else:
                        pod_has_node = False
                        print(
                            f"Pod was scheudled to wrong node: {pod_at_node}, "
                            f"expected: {node_name}"
                        )
                else:
                    pod_has_node = bool(raw_pod.spec.node_name)
                pod_is_scheduled = "PodScheduled" in [
                    cond.type
                    for cond in (raw_pod.status.conditions or [])
                    if cond.status == "True"
                ]
                if pod_has_node and pod_is_scheduled:
                    return
                await asyncio.sleep(interval_s)
    except TimeoutError:
        if raw_pod:
            assert raw_pod.spec is not None
            print("Node:", raw_pod.spec.node_name)
            print("Phase:", raw_pod.status.phase)
            print("Status conditions:", raw_pod.status.conditions)
            print("Pods:")
            pod_list = await client_proxy.core_v1.pod.get_list()
            pods = sorted(pod_list.items, key=attrgetter("spec", "node_name"))
            print(f"  {'Name':40s} {'CPU':5s} {'Memory':10s} {'Phase':9s} Node")
            for pod in pods:
                assert pod.spec is not None
                container = pod.spec.containers[0]
                resource_requests = container.resources.requests
                cpu = resource_requests["cpu"]
                memory = resource_requests["memory"]
                print(
                    f"  {pod.metadata.name:40s}",
                    f"{str(cpu):5s}",
                    f"{str(memory):10s}",
                    f"{pod.status.phase:9s}",
                    f"{str(pod.spec.node_name)}",
                )
        pytest.fail("Pod unscheduled")
