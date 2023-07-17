from __future__ import annotations

import asyncio
import itertools
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import replace
from pathlib import PurePath
from typing import Any
from unittest import mock

import aiohttp
import pytest
from aiohttp import web
from yarl import URL

from platform_api.cluster_config import OrchestratorConfig
from platform_api.config import STORAGE_URI_SCHEME, RegistryConfig, StorageConfig
from platform_api.orchestrator.job import (
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
    ContainerTPUResource,
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
    NodeAffinity,
    NodeResources,
    NodeSelectorRequirement,
    NodeSelectorTerm,
    NodeWatcher,
    NotFoundException,
    PodDescriptor,
    PodWatcher,
    SecretRef,
    Service,
    StatusException,
    Toleration,
)
from platform_api.orchestrator.kube_orchestrator import (
    JobStatusItemFactory,
    KubeConfig,
    KubeOrchestrator,
)
from platform_api.resource import GKEGPUModels, ResourcePoolType, TPUResource

from tests.conftest import random_str
from tests.integration.conftest import ApiRunner, MyKubeClient
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
        self.init_job_internal_hostnames()
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
    job = MyJob(
        orchestrator=kube_orchestrator,
        record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
    )
    return job


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
            else:
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
        self, job_nginx: MyJob, kube_orchestrator: KubeOrchestrator
    ) -> None:
        await job_nginx.start()
        await self.wait_for_success(job_nginx)

        pod = await kube_orchestrator._client.get_pod(job_nginx.id)
        assert pod.image_pull_secrets == [SecretRef(f"neurouser-{job_nginx.owner}")]

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    async def test_start_job_image_pull_secret(
        self, job_nginx: MyJob, kube_orchestrator: KubeOrchestrator
    ) -> None:
        kube_orchestrator._kube_config = replace(
            kube_orchestrator._kube_config, image_pull_secret_name="test-secret"
        )

        await job_nginx.start()
        await self.wait_for_success(job_nginx)

        pod = await kube_orchestrator._client.get_pod(job_nginx.id)
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
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
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
                request=job_request, cluster_name="test-cluster", owner="invalid_name"
            ),
        )

        with pytest.raises(StatusException) as cm:
            await job.start()
        assert str(cm.value) == "Invalid"

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
                request=job_request_second, cluster_name="test-cluster"
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
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
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
                request=JobRequest.create(container), cluster_name="test-cluster"
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
                request=JobRequest.create(container), cluster_name="test-cluster"
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
        kube_client: MyKubeClient,
        orchestrator_config_factory: Callable[..., OrchestratorConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        orchestrator_config = orchestrator_config_factory(
            resource_pool_types=[
                ResourcePoolType(
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
            ),
        )

        await delete_job_later(job)
        await job.start()

        await kube_client.wait_pod_scheduled(job.id)
        job_pod = await kube_client.get_raw_pod(job.id)

        assert job_pod["spec"]["containers"]
        assert job_pod["spec"]["containers"][0]["resources"] == {
            "requests": {"cpu": "100m", "memory": "820M"},  # 0.8 of total request
            "limits": {"cpu": "100m", "memory": "1025M"},
        }

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
                request=JobRequest.create(container), cluster_name="test-cluster"
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
                f"after {t1-t0} secs [{status_item}]"
            )
            status_item = await kube_orchestrator.get_job_status(job)

        assert status_item == JobStatusItem.create(
            JobStatus.FAILED,
            reason=JobStatusReason.CLUSTER_SCALE_UP_FAILED,
            description="Failed to scale up the cluster to get more resources",
        )

    async def test_job_no_memory_after_scaleup(
        self, kube_orchestrator: KubeOrchestrator, kube_client: MyKubeClient
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
                schedule_timeout=10,
            ),
        )
        await job.start()
        await kube_client.create_triggered_scaleup_event(job.id)

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
                f"after {t1-t0} secs [{status_item}]"
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
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        ns = kube_config.namespace

        disk_id = f"disk-{str(uuid.uuid4())}"
        disk = Disk(disk_id=disk_id, path=user_name, cluster_name=cluster_name)
        await kube_client.create_pvc(disk_id, ns)

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
            ),
        )
        await delete_job_later(job)
        await job.start()

        status_item = await kube_orchestrator.get_job_status(job)
        assert status_item.status == JobStatus.PENDING

        t0 = time.monotonic()
        while not status_item.status.is_finished:
            await kube_client.create_failed_attach_volume_event(job.id)
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
        storage_config_host: StorageConfig,
        kube_orchestrator: KubeOrchestrator,
        cluster_name: str,
    ) -> None:
        await self._test_volumes(storage_config_host, kube_orchestrator, cluster_name)

    async def test_volumes_nfs(
        self,
        storage_config_nfs: StorageConfig,
        kube_orchestrator_nfs: KubeOrchestrator,
        cluster_name: str,
    ) -> None:
        await self._test_volumes(
            storage_config_nfs, kube_orchestrator_nfs, cluster_name
        )

    async def test_volumes_pvc(
        self,
        storage_config_pvc: StorageConfig,
        kube_orchestrator_pvc: KubeOrchestrator,
        cluster_name: str,
    ) -> None:
        await self._test_volumes(
            storage_config_pvc, kube_orchestrator_pvc, cluster_name
        )

    async def _test_volumes(
        self,
        storage_config: StorageConfig,
        kube_orchestrator: KubeOrchestrator,
        cluster_name: str,
    ) -> None:
        assert storage_config.host_mount_path
        volumes = [
            ContainerVolume(
                uri=URL(f"{STORAGE_URI_SCHEME}://{cluster_name}"),
                dst_path=PurePath("/storage"),
            )
        ]
        file_path = "/storage/" + str(uuid.uuid4())

        write_container = Container(
            image="ubuntu:20.10",
            command=f"""bash -c 'echo "test" > {file_path}'""",
            volumes=volumes,
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        write_job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(write_container), cluster_name="test-cluster"
            ),
        )

        read_container = Container(
            image="ubuntu:20.10",
            command=f"""bash -c '[ "$(cat {file_path})" == "test" ]'""",
            volumes=volumes,
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        read_job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(read_container), cluster_name="test-cluster"
            ),
        )

        try:
            await write_job.start()
            await self.wait_for_success(write_job)
        finally:
            await write_job.delete()

        try:
            await read_job.start()
            await self.wait_for_success(read_job)
        finally:
            await read_job.delete()

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
                request=JobRequest.create(container), cluster_name="test-cluster"
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
        kube_client: MyKubeClient,
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
                owner=user_name,
                name="test-job",
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        raw = await kube_client.get_raw_pod(pod_name)

        container_raw = raw["spec"]["containers"][0]

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
            item.get("name"): item.get("value", "") for item in container_raw["env"]
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
                request=JobRequest.create(container), cluster_name="test-cluster"
            ),
        )

        try:
            await job.start()
            status = await self.wait_for_completion(job)
            assert status == JobStatus.SUCCEEDED
        finally:
            await job.delete()

    @pytest.fixture
    async def ingress(self, kube_client: KubeClient) -> AsyncIterator[Ingress]:
        ingress_name = str(uuid.uuid4())
        ingress = await kube_client.create_ingress(ingress_name)
        yield ingress
        await kube_client.delete_ingress(ingress.name)

    async def test_ingress_create_delete(self, kube_client: KubeClient) -> None:
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
            ingress_class=mock.ANY,
            rules=rules,
            annotations=annotations,
            labels=labels,
        )

        created_ingress = await kube_client.create_ingress(
            name=name, rules=rules, annotations=annotations, labels=labels
        )
        assert created_ingress == expected_ingress

        requested_ingress = await kube_client.get_ingress(name)
        assert requested_ingress == expected_ingress

        await kube_client.delete_ingress(name)
        with pytest.raises(NotFoundException):
            await kube_client.get_ingress(name)

    async def test_ingress_add_rules(
        self, kube_client: KubeClient, ingress: Ingress
    ) -> None:
        await kube_client.add_ingress_rule(ingress.name, IngressRule(host="host1"))
        await kube_client.add_ingress_rule(ingress.name, IngressRule(host="host2"))
        await kube_client.add_ingress_rule(ingress.name, IngressRule(host="host3"))
        result_ingress = await kube_client.get_ingress(ingress.name)
        assert result_ingress == Ingress(
            name=ingress.name,
            ingress_class=mock.ANY,
            rules=[
                IngressRule(host=""),
                IngressRule(host="host1"),
                IngressRule(host="host2"),
                IngressRule(host="host3"),
            ],
        )

        await kube_client.remove_ingress_rule(ingress.name, "host2")
        result_ingress = await kube_client.get_ingress(ingress.name)
        assert result_ingress == Ingress(
            name=ingress.name,
            ingress_class=mock.ANY,
            rules=[
                IngressRule(host=""),
                IngressRule(host="host1"),
                IngressRule(host="host3"),
            ],
        )

    async def test_remove_ingress_rule(
        self, kube_client: KubeClient, ingress: Ingress
    ) -> None:
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.remove_ingress_rule(ingress.name, "unknown")

    async def test_delete_ingress_failure(self, kube_client: KubeClient) -> None:
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.delete_ingress("unknown")

    async def test_service(self, kube_client: KubeClient) -> None:
        service_name = f"job-{uuid.uuid4()}"
        labels = {"label1": f"value-{uuid.uuid4()}", "label2": f"value-{uuid.uuid4()}"}
        service = Service(name=service_name, target_port=8080, labels=labels)
        try:
            result_service = await kube_client.create_service(service)
            assert result_service.name == service_name
            assert result_service.target_port == 8080
            assert result_service.port == 80
            assert result_service.labels == labels

            service = await kube_client.get_service(service_name)
            assert service.name == service_name
            assert service.target_port == 8080
            assert service.port == 80
            assert service.labels == labels

        finally:
            await kube_client.delete_service(service_name)

    async def test_list_services(self, kube_client: KubeClient) -> None:
        labels1 = {"label": f"value-{uuid.uuid4()}"}
        labels2 = {"label": f"value-{uuid.uuid4()}"}

        def _gen_for_labels(labels: dict[str, str]) -> list[Service]:
            return [
                Service(name=f"job-{uuid.uuid4()}", target_port=8080, labels=labels)
                for _ in range(5)
            ]

        services1 = _gen_for_labels(labels1)
        services2 = _gen_for_labels(labels2)
        try:
            for service in itertools.chain(services1, services2):
                await kube_client.create_service(service)

            assert {service.name for service in services1} == {
                service.name for service in await kube_client.list_services(labels1)
            }

            assert {service.name for service in services2} == {
                service.name for service in await kube_client.list_services(labels2)
            }
        finally:
            for service in itertools.chain(services1, services2):
                try:
                    await kube_client.delete_service(service.name)
                except Exception:
                    pass

    async def test_service_delete_by_uid(self, kube_client: KubeClient) -> None:
        service_name = f"job-{uuid.uuid4()}"
        service = Service(name=service_name, target_port=8080)
        try:
            service_initial = await kube_client.create_service(service)
            await kube_client.delete_service(service_name)
            await kube_client.create_service(service)
            with pytest.raises(NotFoundException):
                await kube_client.delete_service(service_name, uid=service_initial.uid)
        finally:
            await kube_client.delete_service(service_name)

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

    async def test_job_with_exposed_http_server_no_job_name(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
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
                request=JobRequest.create(container), cluster_name="test-cluster"
            ),
        )
        try:
            await job.start()

            assert job.http_host is not None
            assert job.http_host_named is None

            await self._wait_for_job_service(
                kube_ingress_ip, host=job.http_host, job_id=job.id
            )

            ingress = await kube_client.get_ingress(job.id)
            actual_rules_hosts = {rule.host for rule in ingress.rules}
            assert actual_rules_hosts == {job.http_host}

        finally:
            await job.delete()

            # check if ingresses were deleted:

            with pytest.raises(NotFoundException):
                await kube_client.get_ingress(job.id)

    async def test_job_with_exposed_http_server_with_job_name(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
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
                name=f"test-job-name-{random_str()}",
                owner="owner",
            ),
        )
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
            ingress = await kube_client.get_ingress(job.id)
            actual_rules_hosts = {rule.host for rule in ingress.rules}
            assert actual_rules_hosts == {job.http_host, job.http_host_named}

        finally:
            await job.delete()

            # check ingresses were deleted:

            with pytest.raises(NotFoundException):
                await kube_client.get_ingress(job.id)

    async def test_job_with_exposed_http_server_with_auth_no_job_name(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
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
                request=JobRequest.create(container), cluster_name="test-cluster"
            ),
        )
        try:
            await job.start()

            assert job.http_host is not None
            assert job.http_host_named is None

            await self._wait_for_job_service(
                kube_ingress_ip, host=job.http_host, job_id=job.id
            )

            # job ingress auth:
            ingress = await kube_client.get_ingress(job.id)
            actual_rules_hosts = {rule.host for rule in ingress.rules}
            assert actual_rules_hosts == {job.http_host}

        finally:
            await job.delete()

            # check ingresses were deleted:

            with pytest.raises(NotFoundException):
                await kube_client.get_ingress(job.id)

    async def test_job_with_exposed_http_server_with_auth_with_job_name(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
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
                name=f"test-job-name-{random_str()}",
                owner="owner",
            ),
        )
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
            ingress = await kube_client.get_ingress(job.id)
            actual_rules_hosts = {rule.host for rule in ingress.rules}
            assert actual_rules_hosts == {job.http_host, job.http_host_named}

        finally:
            await job.delete()

            # check ingresses were deleted:

            with pytest.raises(NotFoundException):
                await kube_client.get_ingress(job.id)

    @pytest.mark.parametrize("job_named", (False, True))
    async def test_job_service_lifecycle_with_job_name(
        self,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
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
                name=job_name,
                owner="owner",
            ),
        )
        service_name_id = job.id
        service_name_named = kube_orchestrator._get_service_name_for_named(job)

        with pytest.raises(NotFoundException):
            await kube_client.get_service(service_name_id)
        if job_named:
            with pytest.raises(NotFoundException):
                await kube_client.get_service(service_name_named)

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

        with pytest.raises(NotFoundException):
            await kube_client.get_service(service_name_id)
        if job_named:
            with pytest.raises(NotFoundException):
                await kube_client.get_service(service_name_named)

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
                image="python",
                command="python -m http.server 80",
                resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
                http_server=ContainerHTTPServer(port=80),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(server_cont),
                    cluster_name="test-cluster",
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
                "curl --fail --connect-timeout 5 --retry 20 --retry-connrefuse "
                f"http://{server_hostname}/"
            )
            client_cont = Container(
                image="python",
                command=cmd,
                resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(client_cont), cluster_name="test-cluster"
                ),
            )

        yield impl

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

    async def test_job_check_dns_hostname_undeclared_port(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        def create_server_job() -> MyJob:
            server_cont = Container(
                image="python",
                command="python -m http.server 12345",
                resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(server_cont), cluster_name="test-cluster"
                ),
            )

        def create_client_job(server_hostname: str) -> MyJob:
            cmd = (
                "curl --fail --connect-timeout 5 --retry 20 --retry-connrefuse "
                f"http://{server_hostname}:12345/"
            )
            client_cont = Container(
                image="python",
                command=cmd,
                resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(client_cont), cluster_name="test-cluster"
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
        kube_client: MyKubeClient,
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
                preset_name="preseet",
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
        raw_pod = await kube_client.get_raw_pod(pod_name)
        assert raw_pod["metadata"]["labels"] == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/preset": job.preset_name,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/org": "no_org",
            "platform.neuromation.io/project": job.owner,
        }

        policy_name = "neurouser-" + job.owner
        raw_policy = await kube_client.get_network_policy(policy_name)
        assert raw_policy["spec"]["podSelector"]["matchLabels"] == {
            "platform.neuromation.io/user": job.owner
        }

    async def test_job_org_pod_labels(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
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
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
        raw_pod = await kube_client.get_raw_pod(pod_name)

        assert raw_pod["metadata"]["labels"] == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/org": job.org_name,
            "platform.neuromation.io/project": job.owner,
        }

    async def test_gpu_job_pod_labels(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
        kube_node_gpu: str,
    ) -> None:
        node_name = kube_node_gpu
        container = Container(
            image="ubuntu:20.10",
            command="true",
            resources=ContainerResources(
                cpu=0.1, memory=128 * 10**6, gpu=1, gpu_model_id="gpumodel"
            ),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container), cluster_name="test-cluster"
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id

        await kube_client.wait_pod_scheduled(
            pod_name=pod_name, node_name=node_name, timeout_s=60.0
        )
        raw_pod = await kube_client.get_raw_pod(pod_name)

        assert raw_pod["metadata"]["labels"] == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/org": "no_org",
            "platform.neuromation.io/gpu-model": job.gpu_model_id,
            "platform.neuromation.io/project": job.owner,
        }

    async def test_job_resource_labels(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
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
                request=JobRequest.create(container), cluster_name="test-cluster"
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        service_name = job.id
        service = await kube_client.get_service(service_name)
        assert service.labels == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/org": "no_org",
            "platform.neuromation.io/project": job.owner,
        }

        ingress_name = job.id
        ingress = await kube_client.get_ingress(ingress_name)
        assert ingress.labels == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/org": "no_org",
            "platform.neuromation.io/project": job.owner,
        }

    async def test_named_job_resource_labels(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
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
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        service_name = job.id
        service = await kube_client.get_service(service_name)
        assert service.labels == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/org": "no_org",
            "platform.neuromation.io/project": job.owner,
        }

        ingress_name = job.id
        ingress = await kube_client.get_ingress(ingress_name)
        assert ingress.labels == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/org": "no_org",
            "platform.neuromation.io/job-name": job.name,
            "platform.neuromation.io/project": job.owner,
        }

    async def test_job_check_ingress_annotations_jobs_ingress_class_nginx(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_factory: Callable[..., MyKubeClient],
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="nginx")
        async with kube_client_factory(kube_config) as kube_client:
            orchestrator = kube_orchestrator_factory(
                kube_config=kube_config, kube_client=kube_client
            )
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
                ),
            )
            try:
                await job.start()
                pod_name = job.id
                await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
                ingress = await kube_client.get_ingress(pod_name)
                assert ingress.ingress_class == "nginx"
            finally:
                await orchestrator.delete_job(job)

    async def test_job_check_ingress_annotations_jobs_ingress_class_traefik_no_auth(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_factory: Callable[..., MyKubeClient],
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        async with kube_client_factory(kube_config) as kube_client:
            orchestrator = kube_orchestrator_factory(
                kube_config=kube_config, kube_client=kube_client
            )
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
                ),
            )
            try:
                await job.start()
                pod_name = job.id
                await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
                ingress = await kube_client.get_ingress(pod_name)
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
        kube_client_factory: Callable[..., MyKubeClient],
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        async with kube_client_factory(kube_config) as kube_client:
            orchestrator = kube_orchestrator_factory(
                kube_config=kube_config, kube_client=kube_client
            )
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
                ),
            )
            try:
                await job.start()
                pod_name = job.id
                await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
                ingress = await kube_client.get_ingress(pod_name)
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
        kube_client: MyKubeClient,
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
                request=JobRequest.create(container), cluster_name="test-cluster"
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        pod = await kube_client.get_pod(pod_name)
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
        kube_client_factory: Callable[..., MyKubeClient],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        async with kube_client_factory(kube_config) as kube_client:
            orchestrator = kube_orchestrator_factory(
                kube_config=kube_config, kube_client=kube_client
            )
            secret = Secret("key2", user_name, cluster_name)
            await kube_client.update_or_create_secret(  # type: ignore
                secret.k8s_secret_name,
                kube_config.namespace,
                data={secret.secret_key: "vvvv"},
            )

            missing = await orchestrator.get_missing_secrets(
                user_name, secret_names=["key3", "key2", "key1"]
            )
            assert missing == ["key1", "key3"]

    async def test_get_missing_disks(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_factory: Callable[..., MyKubeClient],
        cluster_name: str,
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        async with kube_client_factory(kube_config) as kube_client:
            orchestrator = kube_orchestrator_factory(
                kube_config=kube_config, kube_client=kube_client
            )
            await kube_client.create_pvc(  # type: ignore
                "disk-1",
                kube_config.namespace,
                labels={
                    "platform.neuromation.io/user": "user",
                },
            )
            await kube_client.create_pvc(  # type: ignore
                "disk-2",
                kube_config.namespace,
                labels={
                    "platform.neuromation.io/user": "another_user",
                    "platform.neuromation.io/project": "test_project",
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
                    path="user",  # Wrong username
                    disk_id="disk-2",
                ),
                Disk(
                    cluster_name=cluster_name,
                    path="user",
                    disk_id="disk-3",  # Not existing id
                ),
            ]

            missing = await orchestrator.get_missing_disks(
                disks=[disk1, disk2, disk3, disk4]
            )
            assert missing == [disk3, disk4]

    async def test_job_pod_with_disk_volume_simple_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        ns = kube_config.namespace

        disk_id = f"disk-{str(uuid.uuid4())}"
        disk = Disk(disk_id=disk_id, path=user_name, cluster_name=cluster_name)
        await kube_client.create_pvc(disk_id, ns)

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
                owner=user_name,
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        raw = await kube_client.get_raw_pod(pod_name)

        disk_volumes_raw = [
            v
            for v in raw["spec"]["volumes"]
            if v.get("persistentVolumeClaim", {}).get("claimName") == disk_id
        ]
        assert len(disk_volumes_raw) == 1

        container_raw = raw["spec"]["containers"][0]
        assert container_raw["volumeMounts"] == [
            {
                "name": disk_volumes_raw[0]["name"],
                "mountPath": str(mount_path),
                "subPath": ".",
            }
        ]

    async def test_job_pod_with_disk_volumes_same_mounts_fail(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        ns = kube_config.namespace

        disk_id_1, disk_id_2 = f"disk-{str(uuid.uuid4())}", f"disk-{str(uuid.uuid4())}"
        disk1 = Disk(disk_id_1, user_name, cluster_name)
        disk2 = Disk(disk_id_2, user_name, cluster_name)

        await kube_client.create_pvc(disk_id_1, ns)
        await kube_client.create_pvc(disk_id_2, ns)

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
                owner=user_name,
            ),
        )
        with pytest.raises(JobError):
            await job.start()

    async def test_job_pod_with_secret_env_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        ns = kube_config.namespace

        secret_name = "key1"
        secret = Secret(secret_name, user_name, cluster_name)
        await kube_client.update_or_create_secret(
            secret.k8s_secret_name, ns, data={secret_name: "vvvv"}
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
                owner=user_name,
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        raw = await kube_client.get_raw_pod(pod_name)

        container_raw = raw["spec"]["containers"][0]
        assert {
            "name": "SECRET_VAR",
            "valueFrom": {
                "secretKeyRef": {"key": secret_name, "name": secret.k8s_secret_name}
            },
        } in container_raw["env"]

    async def test_job_pod_with_secret_env_same_secret_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        ns = kube_config.namespace

        secret_name_1, secret_name_2 = "key1", "key2"
        secret1 = Secret(secret_name_1, user_name, cluster_name)
        secret2 = Secret(secret_name_2, user_name, cluster_name)

        assert secret1.k8s_secret_name == secret2.k8s_secret_name
        k8s_secret_name = secret1.k8s_secret_name
        await kube_client.update_or_create_secret(
            k8s_secret_name, ns, data={secret_name_1: "vvvv", secret_name_2: "vvvv"}
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
                owner=user_name,
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        raw = await kube_client.get_raw_pod(pod_name)

        container_raw = raw["spec"]["containers"][0]
        for item in [
            {
                "name": "SECRET_VAR_1",
                "valueFrom": {
                    "secretKeyRef": {"key": secret_name_1, "name": k8s_secret_name}
                },
            },
            {
                "name": "SECRET_VAR_2",
                "valueFrom": {
                    "secretKeyRef": {"key": secret_name_2, "name": k8s_secret_name}
                },
            },
        ]:
            assert item in container_raw["env"]

    async def test_job_pod_with_secret_volume_simple_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        ns = kube_config.namespace

        secret_name = "key1"
        secret = Secret(secret_name, user_name, cluster_name)
        await kube_client.update_or_create_secret(
            secret.k8s_secret_name, ns, data={secret_name: "vvvv"}
        )

        secret_path, secret_file = PurePath("/foo/bar"), "secret.txt"
        container = Container(
            image="ubuntu:20.10",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
            http_server=ContainerHTTPServer(port=80),
            secret_volumes=[
                SecretContainerVolume(secret=secret, dst_path=secret_path / secret_file)
            ],
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name=cluster_name,
                owner=user_name,
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        raw = await kube_client.get_raw_pod(pod_name)

        sec_volumes_raw = [
            v for v in raw["spec"]["volumes"] if v["name"] == secret.k8s_secret_name
        ]
        assert sec_volumes_raw == [
            {
                "name": secret.k8s_secret_name,
                "secret": {"secretName": secret.k8s_secret_name, "defaultMode": 0o400},
            }
        ]

        container_raw = raw["spec"]["containers"][0]
        assert container_raw["volumeMounts"] == [
            {
                "name": secret.k8s_secret_name,
                "readOnly": True,
                "mountPath": str(secret_path / secret_file),
                "subPath": secret_name,
            }
        ]

    async def test_job_pod_with_secret_volumes_same_mounts_fail(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        ns = kube_config.namespace

        sec_name_1, sec_name_2 = "key1", "key2"
        sec1 = Secret(sec_name_1, user_name, cluster_name)
        sec2 = Secret(sec_name_2, user_name, cluster_name)
        assert sec1.k8s_secret_name == sec2.k8s_secret_name
        k8s_sec_name = sec1.k8s_secret_name

        await kube_client.update_or_create_secret(
            k8s_sec_name, ns, data={sec_name_1: "vvvv", sec_name_2: "vvvv"}
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
                owner=user_name,
            ),
        )
        with pytest.raises(JobError):
            await job.start()

    async def test_job_pod_with_secret_volumes_overlapping_mounts_ok(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()
        ns = kube_config.namespace

        secret_a = Secret("aaa", user_name, cluster_name)
        secret_b1 = Secret("bbb-1", user_name, cluster_name)
        secret_b2 = Secret("bbb-2", user_name, cluster_name)
        secret_bc = Secret("bbb-ccc", user_name, cluster_name)

        k8s_sec_name = secret_a.k8s_secret_name
        assert all(
            s.k8s_secret_name == k8s_sec_name
            for s in [secret_a, secret_b1, secret_b2, secret_bc]
        )

        await kube_client.update_or_create_secret(
            k8s_sec_name,
            ns,
            data={
                secret_a.secret_key: "vvvv",
                secret_b1.secret_key: "vvvv",
                secret_b2.secret_key: "vvvv",
                secret_bc.secret_key: "vvvv",
            },
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
                owner=user_name,
            ),
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        raw = await kube_client.get_raw_pod(pod_name)
        sec_volumes_raw = [
            v for v in raw["spec"]["volumes"] if v["name"] == k8s_sec_name
        ]
        assert sec_volumes_raw == [
            {
                "name": k8s_sec_name,
                "secret": {"secretName": k8s_sec_name, "defaultMode": 0o400},
            },
        ]

        container_raw = raw["spec"]["containers"][0]
        assert container_raw["volumeMounts"] == [
            {
                "name": k8s_sec_name,
                "readOnly": True,
                "mountPath": str(path_a / file_a),
                "subPath": secret_a.secret_key,
            },
            {
                "name": k8s_sec_name,
                "readOnly": True,
                "mountPath": str(path_b / file_b1),
                "subPath": secret_b1.secret_key,
            },
            {
                "name": k8s_sec_name,
                "readOnly": True,
                "mountPath": str(path_b / file_b2),
                "subPath": secret_b2.secret_key,
            },
            {
                "name": k8s_sec_name,
                "readOnly": True,
                "mountPath": str(path_bc / file_bc),
                "subPath": secret_bc.secret_key,
            },
        ]

    async def test_delete_all_job_resources(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="sleep 1h",
            http_server=ContainerHTTPServer(80),
            resources=ContainerResources(
                cpu=0.1,
                memory=128 * 10**6,
                tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
            ),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container), cluster_name="test-cluster"
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)

        pod_name = ingress_name = service_name = networkpolicy_name = job.id

        assert await kube_client.get_pod(pod_name)
        await kube_client.get_ingress(ingress_name)
        await kube_client.get_service(service_name)
        await kube_client.get_network_policy(networkpolicy_name)

        await kube_orchestrator.delete_all_job_resources(job.id)

        await kube_client.wait_pod_non_existent(pod_name, timeout_s=60.0)
        with pytest.raises(JobNotFoundException):
            await kube_client.get_pod(pod_name)

        with pytest.raises(NotFoundException):
            await kube_client.get_ingress(ingress_name)

        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.get_service(service_name)

        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.get_network_policy(networkpolicy_name)

    async def test_cleanup_old_named_ingresses(
        self,
        kube_client: MyKubeClient,
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
            ),
        )
        job2 = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=name,
                owner="owner2",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
            ),
        )
        await delete_job_later(job1)
        await kube_orchestrator.start_job(job1)

        await delete_job_later(job2)
        await kube_orchestrator.start_job(job2)

        await kube_client.get_ingress(job1.id)
        await kube_client.get_ingress(job2.id)

        job3 = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                name=name,
                owner="owner1",
                request=JobRequest.create(container),
                cluster_name="test-cluster",
            ),
        )
        await delete_job_later(job3)
        await kube_orchestrator.start_job(job3)

        with pytest.raises(NotFoundException):
            await kube_client.get_ingress(job1.id)
        await kube_client.get_ingress(job2.id)
        await kube_client.get_ingress(job3.id)

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
        self, kube_client: MyKubeClient, kube_orchestrator: KubeOrchestrator
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
            ),
        )

        scheduled = await kube_orchestrator.get_scheduled_jobs([job])
        assert scheduled == []

        await kube_orchestrator.start_job(job)
        await kube_client.wait_pod_is_running(pod_name=job.id, timeout_s=60.0)

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
            ),
        )
        jobs = await kube_orchestrator.get_schedulable_jobs([job1, job2, job3])

        assert jobs == [job1]


class TestNodeAffinity:
    @pytest.fixture
    def kube_orchestrator_factory(
        self,
        storage_config_host: StorageConfig,
        registry_config: RegistryConfig,
        orchestrator_config: OrchestratorConfig,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        kube_job_nodes_factory: Callable[
            [OrchestratorConfig, KubeConfig], Awaitable[None]
        ],
    ) -> Callable[[Sequence[ResourcePoolType]], Awaitable[KubeOrchestrator]]:
        kube_config = replace(
            kube_config,
            node_label_job="job",
            node_label_gpu="gpu",
            node_label_node_pool="nodepool",
            node_label_preemptible="preemptible",
        )

        async def _create(
            resource_pool_types: Sequence[ResourcePoolType],
        ) -> KubeOrchestrator:
            orchestrator = replace(
                orchestrator_config, resource_pool_types=resource_pool_types
            )
            await kube_job_nodes_factory(orchestrator, kube_config)
            return KubeOrchestrator(
                cluster_name="default",
                storage_configs=[storage_config_host],
                registry_config=registry_config,
                orchestrator_config=orchestrator,
                kube_config=kube_config,
                kube_client=kube_client,
            )

        return _create

    @pytest.fixture
    async def kube_orchestrator(
        self,
        kube_orchestrator_factory: Callable[
            [Sequence[ResourcePoolType]], Awaitable[KubeOrchestrator]
        ],
    ) -> KubeOrchestrator:
        return await kube_orchestrator_factory(
            [
                ResourcePoolType(
                    name="cpu-small", available_cpu=2, available_memory=2048 * 10**6
                ),
                ResourcePoolType(
                    name="cpu-small-p",
                    available_cpu=2,
                    available_memory=2048 * 10**6,
                    is_preemptible=True,
                ),
                ResourcePoolType(
                    name="cpu-large-tpu",
                    available_cpu=3,
                    available_memory=14336 * 10**6,
                    tpu=TPUResource(
                        ipv4_cidr_block="1.1.1.1/32",
                        types=("v2-8",),
                        software_versions=("1.14",),
                    ),
                ),
                ResourcePoolType(
                    name="gpu-k80",
                    available_cpu=7,
                    available_memory=61440 * 10**6,
                    gpu=8,
                    gpu_model=GKEGPUModels.K80.value.id,
                ),
                ResourcePoolType(
                    name="gpu-v100",
                    available_cpu=7,
                    available_memory=61440 * 10**6,
                    gpu=8,
                    gpu_model=GKEGPUModels.V100.value.id,
                ),
                ResourcePoolType(
                    name="gpu-v100-p",
                    available_cpu=7,
                    available_memory=61440 * 10**6,
                    gpu=8,
                    gpu_model=GKEGPUModels.V100.value.id,
                    is_preemptible=True,
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
                    name="gpu-k80",
                    available_cpu=7,
                    available_memory=61440 * 10**6,
                    gpu=8,
                    gpu_model=GKEGPUModels.K80.value.id,
                ),
            ],
        )

    @pytest.fixture
    async def start_job(
        self, kube_client: MyKubeClient
    ) -> Callable[..., AbstractAsyncContextManager[MyJob]]:
        @asynccontextmanager
        async def _create(
            kube_orchestrator: KubeOrchestrator,
            cpu: float,
            memory: int,
            gpu: int | None = None,
            gpu_model: str | None = None,
            scheduler_enabled: bool = False,
            preemptible_node: bool = False,
        ) -> AsyncIterator[MyJob]:
            container = Container(
                image="ubuntu:20.10",
                command="true",
                resources=ContainerResources(
                    cpu=cpu,
                    memory=memory,
                    gpu=gpu,
                    gpu_model_id=gpu_model,
                ),
            )
            job = MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    name=f"job-{uuid.uuid4().hex[:6]}",
                    owner="owner1",
                    request=JobRequest.create(container),
                    cluster_name="test-cluster",
                    scheduler_enabled=scheduler_enabled,
                    preemptible_node=preemptible_node,
                ),
            )
            await kube_orchestrator.start_job(job, tolerate_unreachable_node=True)
            yield job
            await kube_orchestrator.delete_job(job)
            # Sometimes pods stuck in terminating state.
            # Force delete to free node resources.
            await kube_client.delete_pod(job.id, force=True)
            await kube_client.wait_pod_non_existent(job.id, timeout_s=10)

        return _create

    async def test_unschedulable_job(
        self,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        with pytest.raises(
            JobUnschedulableException, match="Job will not fit into cluster"
        ):
            async with start_job(kube_orchestrator, cpu=100, memory=32 * 10**6):
                pass

    async def test_cpu_job(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(kube_orchestrator, cpu=0.1, memory=32 * 10**6) as job:
            await kube_client.wait_pod_scheduled(job.id, "cpu-small")

            job_pod = await kube_client.get_raw_pod(job.id)
            assert (
                job_pod["spec"]["affinity"]["nodeAffinity"]
                == NodeAffinity(
                    required=[
                        NodeSelectorTerm(
                            [NodeSelectorRequirement.create_in("nodepool", "cpu-small")]
                        )
                    ]
                ).to_primitive()
            )

    async def test_cpu_job_on_gpu_node(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator_gpu: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator_gpu, cpu=0.1, memory=32 * 10**6
        ) as job:
            await kube_client.wait_pod_scheduled(job.id, "gpu-k80")

            job_pod = await kube_client.get_raw_pod(job.id)
            assert (
                job_pod["spec"]["affinity"]["nodeAffinity"]
                == NodeAffinity(
                    required=[
                        NodeSelectorTerm(
                            [NodeSelectorRequirement.create_in("nodepool", "gpu-k80")]
                        )
                    ]
                ).to_primitive()
            )

    async def test_cpu_job_on_tpu_node(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(kube_orchestrator, cpu=3, memory=32 * 10**6) as job:
            await kube_client.wait_pod_scheduled(job.id, "cpu-large-tpu")

            job_pod = await kube_client.get_raw_pod(job.id)
            assert (
                job_pod["spec"]["affinity"]["nodeAffinity"]
                == NodeAffinity(
                    required=[
                        NodeSelectorTerm(
                            [
                                NodeSelectorRequirement.create_in(
                                    "nodepool", "cpu-large-tpu"
                                )
                            ]
                        )
                    ]
                ).to_primitive()
            )

    async def test_cpu_job_not_scheduled_on_gpu_node(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        with pytest.raises(
            JobUnschedulableException, match="Job will not fit into cluster"
        ):
            async with start_job(kube_orchestrator, cpu=7, memory=32 * 10**6):
                pass

    async def test_gpu_job(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator,
            cpu=0.1,
            memory=32 * 10**6,
            gpu=1,
            gpu_model="nvidia-tesla-k80",
        ) as job:
            await kube_client.wait_pod_scheduled(job.id, "gpu-k80")

            job_pod = await kube_client.get_raw_pod(job.id)
            assert (
                job_pod["spec"]["affinity"]["nodeAffinity"]
                == NodeAffinity(
                    required=[
                        NodeSelectorTerm(
                            [NodeSelectorRequirement.create_in("nodepool", "gpu-k80")]
                        )
                    ]
                ).to_primitive()
            )

    async def test_scheduled_job_on_not_preemptible_node(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator,
            cpu=0.1,
            memory=32 * 10**6,
            gpu=1,
            gpu_model="nvidia-tesla-v100",
            scheduler_enabled=True,
        ) as job:
            await kube_client.wait_pod_scheduled(job.id, "gpu-v100")

            job_pod = await kube_client.get_raw_pod(job.id)
            assert (
                job_pod["spec"]["affinity"]["nodeAffinity"]
                == NodeAffinity(
                    required=[
                        NodeSelectorTerm(
                            [NodeSelectorRequirement.create_in("nodepool", "gpu-v100")]
                        ),
                    ],
                    preferred=[],
                ).to_primitive()
            )

    async def test_preemptible_job_on_preemptible_node(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AbstractAsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator,
            cpu=0.1,
            memory=32 * 10**6,
            preemptible_node=True,
        ) as job:
            await kube_client.wait_pod_scheduled(job.id, "cpu-small-p")

            job_pod = await kube_client.get_raw_pod(job.id)
            assert (
                job_pod["spec"]["affinity"]["nodeAffinity"]
                == NodeAffinity(
                    required=[
                        NodeSelectorTerm(
                            [
                                NodeSelectorRequirement.create_in(
                                    "nodepool", "cpu-small-p"
                                )
                            ]
                        ),
                    ]
                ).to_primitive()
            )


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
        kube_client: KubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> Callable[..., Awaitable[JobStatusItem]]:
        async def _f(resources: ContainerResources, command: str) -> JobStatusItem:
            container = Container(
                image="ubuntu:20.10", command=command, resources=resources
            )
            job_request = JobRequest.create(container)
            pod = PodDescriptor.from_job_request(job_request)
            await delete_pod_later(pod)
            await kube_client.create_pod(pod)
            await kube_client.wait_pod_is_terminated(pod_name=pod.name, timeout_s=60.0)
            pod_status = await kube_client.get_pod_status(pod.name)
            return JobStatusItemFactory(pod_status).create()

        return _f

    @pytest.fixture
    def command_assert_shm_64_mb(self) -> str:
        df = "/bin/df --block-size M --output=avail /dev/shm"
        return f"/bin/bash -c '{df} | grep -q \"^\\s*64M\"'"

    async def test_shm_extended_request_parameter_omitted(
        self,
        run_command_get_status: Callable[..., Awaitable[JobStatusItem]],
        command_assert_shm_64_mb: str,
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory=128 * 10**6)
        status_item = await run_command_get_status(resources, command_assert_shm_64_mb)
        assert status_item.status == JobStatus.SUCCEEDED

    async def test_shm_extended_request_parameter_not_requested(
        self,
        run_command_get_status: Callable[..., Awaitable[JobStatusItem]],
        command_assert_shm_64_mb: str,
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory=128 * 10**6, shm=False)
        status_item = await run_command_get_status(resources, command_assert_shm_64_mb)
        assert status_item.status == JobStatus.SUCCEEDED

    async def test_shm_extended_request_parameter_requested(
        self,
        run_command_get_status: Callable[..., Awaitable[JobStatusItem]],
        command_assert_shm_64_mb: str,
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory=128 * 10**6, shm=True)
        status_item = await run_command_get_status(resources, command_assert_shm_64_mb)
        assert status_item.status == JobStatus.FAILED

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

    async def test_shm_extended_requested_try_create_huge_file(
        self, run_command_get_status: Callable[..., Awaitable[JobStatusItem]]
    ) -> None:
        command = "dd if=/dev/zero of=/dev/shm/test bs=256M  count=1"
        resources = ContainerResources(cpu=0.1, memory=1024 * 10**6, shm=True)
        status_actual = await run_command_get_status(resources, command)
        status_expected = JobStatusItem.create(status=JobStatus.SUCCEEDED, exit_code=0)
        assert status_actual == status_expected, f"actual: '{status_actual}'"

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
    async def kube_orchestrator(
        self,
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_config_node_preemptible: KubeConfig,
    ) -> KubeOrchestrator:
        return kube_orchestrator_factory(kube_config=kube_config_node_preemptible)

    async def test_non_preemptible_job(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container), cluster_name="test-cluster"
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_running

        await kube_client.delete_pod(pod_name, force=True)

        # triggering pod recreation
        with pytest.raises(JobNotFoundException, match="was not found"):
            await kube_orchestrator.get_job_status(job)

    async def test_scheduled_job_lost_running_pod(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
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
                # marking the job as scheduled
                scheduler_enabled=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_running

        await kube_client.delete_pod(pod_name, force=True)

        # triggering pod recreation
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_pending

        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_running

    async def test_preemptible_job_lost_running_pod(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
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
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_running

        await kube_client.delete_pod(pod_name, force=True)

        # triggering pod recreation
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_pending

        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_running

    async def test_preemptible_job_lost_node_lost_pod(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
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
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_scheduled(pod_name, node_name)

        await kube_client.delete_node(node_name)
        # deleting node initiates it's pods deletion
        await kube_client.wait_pod_non_existent(pod_name, timeout_s=60.0)

        # triggering pod recreation
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_pending

    async def test_preemptible_job_pending_pod_node_not_ready(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
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
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_scheduled(pod_name, node_name)

        raw_pod = await kube_client.get_raw_pod(pod_name)

        raw_pod["status"]["reason"] = "NodeLost"
        await kube_client.set_raw_pod_status(pod_name, raw_pod)

        raw_pod = await kube_client.get_raw_pod(pod_name)
        assert raw_pod["status"]["reason"] == "NodeLost"

        # triggering pod recreation
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_pending

        await kube_client.wait_pod_scheduled(pod_name, node_name)

        raw_pod = await kube_client.get_raw_pod(pod_name)
        assert not raw_pod["status"].get("reason")

    async def test_preemptible_job_recreation_failed(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
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
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_scheduled(pod_name, node_name)

        await kube_client.delete_pod(pod_name, force=True)

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
                # marking the job as preemptible
                preemptible_node=True,
            ),
        )

        # triggering pod recreation
        with pytest.raises(
            JobNotFoundException, match=f"Pod '{pod_name}' not found. Job '{job.id}'"
        ):
            await kube_orchestrator.get_job_status(job)


class TestRestartPolicy:
    async def test_restart_failing(
        self,
        kube_client: MyKubeClient,
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
                restart_policy=JobRestartPolicy.ON_FAILURE,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)

        status = await kube_orchestrator.get_job_status(job)
        assert status in (
            JobStatusItem.create(
                status=JobStatus.RUNNING, reason=JobStatusReason.RESTARTING
            ),
            JobStatusItem.create(status=JobStatus.RUNNING),
        )

    async def test_restart_failing_succeeded(
        self,
        kube_client: MyKubeClient,
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
                restart_policy=JobRestartPolicy.ON_FAILURE,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_is_terminated(pod_name=pod_name, timeout_s=60.0)

        status = await kube_orchestrator.get_job_status(job)
        assert status == JobStatusItem.create(status=JobStatus.SUCCEEDED, exit_code=0)

    async def test_restart_always(
        self,
        kube_client: MyKubeClient,
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
                restart_policy=JobRestartPolicy.ALWAYS,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_is_terminated(pod_name=pod_name, timeout_s=60.0)

        status = await kube_orchestrator.get_job_status(job)
        assert status in (
            JobStatusItem.create(
                status=JobStatus.RUNNING, reason=JobStatusReason.RESTARTING
            ),
            JobStatusItem.create(status=JobStatus.RUNNING),
        )

    async def test_restart_always_succeeded(
        self,
        kube_client: MyKubeClient,
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
                restart_policy=JobRestartPolicy.ALWAYS,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_is_terminated(pod_name=pod_name, timeout_s=60.0)

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
        kube_client: MyKubeClient,
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
                ),
            )
            await kube_orchestrator.start_job(job)
            await delete_job_later(job)
            if wait:
                await kube_client.wait_pod_is_running(job.id, timeout_s=wait_timeout_s)
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
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
        node_resources: NodeResources,
    ) -> None:
        preemptible_job = await job_factory(cpu=node_resources.cpu / 2, wait=True)
        # Node should have less than cpu / 2 left
        job = await job_factory(cpu=node_resources.cpu / 2)
        preempted_jobs = await kube_orchestrator.preempt_jobs([job], [preemptible_job])

        assert preempted_jobs == [preemptible_job]

        await kube_client.wait_pod_is_deleted(
            preemptible_job.id, timeout_s=60, interval_s=0.1
        )

    async def test_cannot_be_scheduled(
        self,
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_config_factory: Callable[..., KubeConfig],
        kube_client_factory: Callable[..., MyKubeClient],
        job_factory: Callable[..., Awaitable[Job]],
        node_resources: NodeResources,
    ) -> None:
        kube_config = kube_config_factory(node_label_node_pool="nodepool")
        async with kube_client_factory(kube_config) as kube_client:
            kube_orchestrator = kube_orchestrator_factory(
                kube_config=kube_config, kube_client=kube_client
            )
            preemptible_job = await job_factory(cpu=node_resources.cpu / 2, wait=True)
            # Node should have less than cpu / 2 left
            job = await job_factory(cpu=node_resources.cpu / 2)
            preempted = await kube_orchestrator.preempt_jobs([job], [preemptible_job])

            assert preempted == []

            job_pod = await kube_client.get_pod(job.id)
            assert job_pod.status
            assert job_pod.status.is_phase_pending

    async def test_not_enough_resources(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
        node_resources: NodeResources,
    ) -> None:
        preemptible_job = await job_factory(cpu=node_resources.cpu / 2, wait=True)
        # Node should have less than cpu / 2 left
        job = await job_factory(cpu=node_resources.cpu)
        preempted = await kube_orchestrator.preempt_jobs([job], [preemptible_job])

        assert preempted == []

        job_pod = await kube_client.get_pod(job.id)
        assert job_pod.status
        assert job_pod.status.is_phase_pending

    async def test_running_jobs_ignored(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
    ) -> None:
        job = await job_factory()
        preemptible_job = await job_factory(wait=True)
        preempted = await kube_orchestrator.preempt_jobs([job], [preemptible_job])

        assert preempted == []

        preemptible_pod = await kube_client.get_pod(preemptible_job.id)
        assert preemptible_pod.status
        assert preemptible_pod.status.is_scheduled

    async def test_no_preemptible_jobs(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
        node_resources: NodeResources,
    ) -> None:
        # Should not be scheduled
        job = await job_factory(cpu=node_resources.cpu)
        preempted = await kube_orchestrator.preempt_jobs([job], [])

        assert preempted == []

        job_pod = await kube_client.get_pod(job.id)
        assert job_pod.status
        assert job_pod.status.is_phase_pending

    async def test_no_jobs(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        job_factory: Callable[..., Awaitable[Job]],
    ) -> None:
        preemptible_job = await job_factory(wait=True)
        preempted = await kube_orchestrator.preempt_jobs([], [preemptible_job])

        assert preempted == []

        preemptible_pod = await kube_client.get_pod(preemptible_job.id)
        assert preemptible_pod.status
        assert preemptible_pod.status.is_scheduled
