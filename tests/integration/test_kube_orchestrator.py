import asyncio
import shlex
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import PurePath
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    Optional,
)
from unittest import mock

import aiohttp
import pytest
from aiohttp import web
from yarl import URL

from platform_api.cluster_config import RegistryConfig, StorageConfig
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
    Secret,
    SecretContainerVolume,
)
from platform_api.orchestrator.kube_client import (
    AlreadyExistsException,
    DockerRegistrySecret,
    Ingress,
    IngressRule,
    KubeClient,
    NodeAffinity,
    NodeSelectorRequirement,
    NodeSelectorTerm,
    PodDescriptor,
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
from tests.integration.test_api import ApiConfig

from .conftest import ApiRunner, MyKubeClient


class MyJob(Job):
    def __init__(
        self, orchestrator: KubeOrchestrator, *, record: JobRecord, **kwargs: Any
    ) -> None:
        self._orchestrator = orchestrator
        if not record.owner:
            record = replace(record, owner="test-owner")
        super().__init__(
            storage_config=orchestrator.storage_config,
            orchestrator_config=orchestrator.config,
            record=record,
        )

    async def start(self) -> JobStatus:
        await self._orchestrator.prepare_job(self)
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
        image="ubuntu",
        command="sleep 5",
        resources=ContainerResources(cpu=0.1, memory_mb=256),
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

    @pytest.mark.asyncio
    async def test_start_job_happy_path(
        self, job_nginx: MyJob, kube_orchestrator: KubeOrchestrator
    ) -> None:
        await job_nginx.start()
        await self.wait_for_success(job_nginx)

        pod = await kube_orchestrator._client.get_pod(job_nginx.id)
        assert pod.image_pull_secrets == [SecretRef(f"neurouser-{job_nginx.owner}")]

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_start_job_broken_image(
        self, kube_orchestrator: KubeOrchestrator
    ) -> None:
        container = Container(
            image="notsuchdockerimage",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )
        await job.start()
        status = await job.delete()
        assert status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_start_job_bad_name(
        self, kube_orchestrator: KubeOrchestrator
    ) -> None:
        job_id = str(uuid.uuid4())
        container = Container(
            image="ubuntu",
            command="sleep 5",
            resources=ContainerResources(cpu=0.1, memory_mb=256),
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

    @pytest.mark.asyncio
    async def test_start_job_with_not_unique_id(
        self, kube_orchestrator: KubeOrchestrator, job_nginx: MyJob
    ) -> None:
        await job_nginx.start()
        await self.wait_for_success(job_nginx)

        container = Container(
            image="python", resources=ContainerResources(cpu=0.1, memory_mb=128)
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

    @pytest.mark.asyncio
    async def test_status_job_not_exist(self, job_nginx: MyJob) -> None:
        with pytest.raises(JobNotFoundException):
            await job_nginx.query_status()

    @pytest.mark.asyncio
    async def test_delete_job_not_exist(self, job_nginx: MyJob) -> None:
        with pytest.raises(JobNotFoundException):
            await job_nginx.delete()

    @pytest.mark.asyncio
    async def test_broken_job_id(self, kube_orchestrator: KubeOrchestrator) -> None:
        job_id = "some_BROCKEN_JOB-123@#$%^&*(______------ID"
        container = Container(
            image="python", resources=ContainerResources(cpu=0.1, memory_mb=128)
        )
        job_request = JobRequest(job_id=job_id, container=container)
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(request=job_request, cluster_name="test-cluster"),
        )

        with pytest.raises(JobError):
            await job.start()

    @pytest.mark.asyncio
    async def test_job_succeeded(self, kube_orchestrator: KubeOrchestrator) -> None:
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
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

    @pytest.mark.asyncio
    async def test_job_failed_error(self, kube_orchestrator: KubeOrchestrator) -> None:
        command = 'bash -c "for i in {100..1}; do echo $i; done; false"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
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

    @pytest.mark.asyncio
    async def test_job_bunch_of_cpu(self, kube_orchestrator: KubeOrchestrator) -> None:
        command = 'bash -c "for i in {100..1}; do echo $i; done; false"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=100, memory_mb=16536),
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

    @pytest.mark.asyncio
    async def test_job_no_memory(self, kube_orchestrator: KubeOrchestrator) -> None:
        command = "true"
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=1, memory_mb=500_000),
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

    @pytest.mark.asyncio
    async def test_job_no_memory_after_scaleup(
        self, kube_orchestrator: KubeOrchestrator, kube_client: MyKubeClient
    ) -> None:
        command = "true"
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=1, memory_mb=500_000),
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

    @pytest.mark.asyncio
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
        disk = Disk(disk_id=disk_id, user_name=user_name, cluster_name=cluster_name)
        await kube_client.create_pvc(disk_id, ns)

        mount_path = PurePath("/mnt/disk")
        container = Container(
            image="ubuntu",
            command="sleep 10",
            resources=ContainerResources(cpu=0.1, memory_mb=32),
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
        await kube_client.create_failed_attach_volume_event(job.id)

        status_item = await kube_orchestrator.get_job_status(job)
        assert status_item.status == JobStatus.PENDING

        t0 = time.monotonic()
        while not status_item.status.is_finished:
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

    @pytest.mark.asyncio
    async def test_volumes(
        self,
        storage_config_host: StorageConfig,
        kube_orchestrator: KubeOrchestrator,
        cluster_name: str,
    ) -> None:
        await self._test_volumes(storage_config_host, kube_orchestrator, cluster_name)

    @pytest.mark.asyncio
    async def test_volumes_nfs(
        self,
        storage_config_nfs: StorageConfig,
        kube_orchestrator_nfs: KubeOrchestrator,
        cluster_name: str,
    ) -> None:
        await self._test_volumes(
            storage_config_nfs, kube_orchestrator_nfs, cluster_name
        )

    @pytest.mark.asyncio
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
                uri=URL(
                    f"{storage_config.uri_scheme}://"
                    f"{cluster_name}/{storage_config.host_mount_path}"
                ),
                src_path=storage_config.host_mount_path,
                dst_path=PurePath("/storage"),
            )
        ]
        file_path = "/storage/" + str(uuid.uuid4())

        write_container = Container(
            image="ubuntu",
            command=f"""bash -c 'echo "test" > {file_path}'""",
            volumes=volumes,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        write_job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(write_container), cluster_name="test-cluster"
            ),
        )

        read_container = Container(
            image="ubuntu",
            command=f"""bash -c '[ "$(cat {file_path})" == "test" ]'""",
            volumes=volumes,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            env={"A": "2", "B": "3"},
            command=fr"""bash -c '[ "$(expr $A \* $B)" == "{product}" ]'""",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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

    @pytest.mark.asyncio
    async def test_working_dir(self, kube_orchestrator: KubeOrchestrator) -> None:
        container = Container(
            image="ubuntu",
            working_dir="/var/log",
            command="""bash -c '[ "$(pwd)" == "/var/log" ]'""",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
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

    @pytest.mark.asyncio
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
            name=name, rules=rules, annotations=annotations, labels=labels
        )

        created_ingress = await kube_client.create_ingress(
            name=name, rules=rules, annotations=annotations, labels=labels
        )
        assert created_ingress == expected_ingress

        requested_ingress = await kube_client.get_ingress(name)
        assert requested_ingress == expected_ingress

        await kube_client.delete_ingress(name)
        # NOTE: should be another exception, see issue #792
        with pytest.raises(JobNotFoundException, match="not found"):
            await kube_client.get_ingress(name)

    @pytest.mark.asyncio
    async def test_ingress_add_rules(
        self, kube_client: KubeClient, ingress: Ingress
    ) -> None:

        await kube_client.add_ingress_rule(ingress.name, IngressRule(host="host1"))
        await kube_client.add_ingress_rule(ingress.name, IngressRule(host="host2"))
        await kube_client.add_ingress_rule(ingress.name, IngressRule(host="host3"))
        result_ingress = await kube_client.get_ingress(ingress.name)
        assert result_ingress == Ingress(
            name=ingress.name,
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
            rules=[
                IngressRule(host=""),
                IngressRule(host="host1"),
                IngressRule(host="host3"),
            ],
        )

    @pytest.mark.asyncio
    async def test_remove_ingress_rule(
        self, kube_client: KubeClient, ingress: Ingress
    ) -> None:
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.remove_ingress_rule(ingress.name, "unknown")

    @pytest.mark.asyncio
    async def test_delete_ingress_failure(self, kube_client: KubeClient) -> None:
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.delete_ingress("unknown")

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
    async def test_job_with_exposed_http_server_no_job_name(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
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

            # NOTE: should be another exception, see issue #792
            with pytest.raises(JobNotFoundException, match="not found"):
                await kube_client.get_ingress(job.id)

    @pytest.mark.asyncio
    async def test_job_with_exposed_http_server_with_job_name(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
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

            # NOTE: should be another exception, see issue #792
            with pytest.raises(JobNotFoundException, match="not found"):
                await kube_client.get_ingress(job.id)

    @pytest.mark.asyncio
    async def test_job_with_exposed_http_server_with_auth_no_job_name(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
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

            # NOTE: should be another exception, see issue #792
            with pytest.raises(JobNotFoundException, match="not found"):
                await kube_client.get_ingress(job.id)

    @pytest.mark.asyncio
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
            resources=ContainerResources(cpu=0.1, memory_mb=128),
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

            # NOTE: should be another exception, see issue #792
            with pytest.raises(JobNotFoundException, match="not found"):
                await kube_client.get_ingress(job.id)

    @pytest.fixture
    def create_server_job(
        self, kube_orchestrator: KubeOrchestrator
    ) -> Iterator[Callable[[Optional[str]], MyJob]]:
        def impl(job_name: Optional[str] = None) -> MyJob:
            server_cont = Container(
                image="python",
                command="python -m http.server 80",
                resources=ContainerResources(cpu=0.1, memory_mb=128),
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
                resources=ContainerResources(cpu=0.1, memory_mb=128),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                record=JobRecord.create(
                    request=JobRequest.create(client_cont), cluster_name="test-cluster"
                ),
            )

        yield impl

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
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
                resources=ContainerResources(cpu=0.1, memory_mb=128),
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
                resources=ContainerResources(cpu=0.1, memory_mb=128),
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

    @pytest.mark.asyncio
    async def test_job_pod_labels_and_network_policy(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
        raw_pod = await kube_client.get_raw_pod(pod_name)
        assert raw_pod["metadata"]["labels"] == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
        }

        policy_name = "neurouser-" + job.owner
        raw_policy = await kube_client.get_network_policy(policy_name)
        assert raw_policy["spec"]["podSelector"]["matchLabels"] == {
            "platform.neuromation.io/user": job.owner
        }

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="true",
            resources=ContainerResources(
                cpu=0.1, memory_mb=128, gpu=1, gpu_model_id="gpumodel"
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
            "platform.neuromation.io/gpu-model": job.gpu_model_id,
        }

    @pytest.mark.asyncio
    async def test_job_resource_labels(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="sleep 1h",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
        }

        ingress_name = job.id
        ingress = await kube_client.get_ingress(ingress_name)
        assert ingress.labels == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
        }

    @pytest.mark.asyncio
    async def test_named_job_resource_labels(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="sleep 1h",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
        }

        ingress_name = job.id
        ingress = await kube_client.get_ingress(ingress_name)
        assert ingress.labels == {
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
            "platform.neuromation.io/job-name": job.name,
        }

    @pytest.mark.asyncio
    async def test_job_check_ingress_annotations_jobs_ingress_class_nginx(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_factory: Callable[..., MyKubeClient],
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="nginx")
        async with kube_orchestrator_factory(kube_config=kube_config) as orchestrator:
            async with kube_client_factory(kube_config) as kube_client:
                container = Container(
                    image="ubuntu",
                    command="sleep 1h",
                    resources=ContainerResources(cpu=0.1, memory_mb=16),
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
                    await kube_client.wait_pod_is_running(
                        pod_name=pod_name, timeout_s=60.0
                    )
                    ingress = await kube_client.get_ingress(pod_name)
                    assert ingress.annotations == dict()
                finally:
                    await orchestrator.delete_job(job)

    @pytest.mark.asyncio
    async def test_job_check_ingress_annotations_jobs_ingress_class_traefik_no_auth(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_factory: Callable[..., MyKubeClient],
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        async with kube_orchestrator_factory(kube_config=kube_config) as orchestrator:
            async with kube_client_factory(kube_config) as kube_client:
                container = Container(
                    image="ubuntu",
                    command="sleep 1h",
                    resources=ContainerResources(cpu=0.1, memory_mb=16),
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
                    await kube_client.wait_pod_is_running(
                        pod_name=pod_name, timeout_s=60.0
                    )
                    ingress = await kube_client.get_ingress(pod_name)
                    assert ingress.annotations == {
                        "kubernetes.io/ingress.class": "traefik",
                        "traefik.ingress.kubernetes.io/error-pages": (
                            "default:\n"
                            "  status:\n"
                            '  - "500-600"\n'
                            "  backend: error-pages\n"
                            "  query: /"
                        ),
                    }
                finally:
                    await orchestrator.delete_job(job)

    @pytest.mark.asyncio
    async def test_job_check_ingress_annotations_jobs_ingress_class_traefik_with_auth(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_factory: Callable[..., MyKubeClient],
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        async with kube_orchestrator_factory(kube_config=kube_config) as orchestrator:
            async with kube_client_factory(kube_config) as kube_client:
                container = Container(
                    image="ubuntu",
                    command="sleep 1h",
                    resources=ContainerResources(cpu=0.1, memory_mb=16),
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
                    await kube_client.wait_pod_is_running(
                        pod_name=pod_name, timeout_s=60.0
                    )
                    ingress = await kube_client.get_ingress(pod_name)
                    assert ingress.annotations == {
                        "kubernetes.io/ingress.class": "traefik",
                        "traefik.ingress.kubernetes.io/error-pages": (
                            "default:\n"
                            "  status:\n"
                            '  - "500-600"\n'
                            "  backend: error-pages\n"
                            "  query: /"
                        ),
                        "ingress.kubernetes.io/auth-type": "forward",
                        "ingress.kubernetes.io/auth-trust-headers": "true",
                        "ingress.kubernetes.io/auth-url": (
                            "https://neu.ro/oauth/authorize"
                        ),
                    }
                finally:
                    await orchestrator.delete_job(job)

    @pytest.mark.asyncio
    async def test_job_pod_tolerations(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="sleep 1h",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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

    @pytest.mark.asyncio
    async def test_get_missing_secrets(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_factory: Callable[..., MyKubeClient],
        cluster_name: str,
    ) -> None:
        user_name = self._create_username()

        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        async with kube_orchestrator_factory(kube_config=kube_config) as orchestrator:
            async with kube_client_factory(kube_config) as kube_client:

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

    @pytest.mark.asyncio
    async def test_get_missing_disks(
        self,
        kube_config_factory: Callable[..., KubeConfig],
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_client_factory: Callable[..., MyKubeClient],
        cluster_name: str,
    ) -> None:
        kube_config = kube_config_factory(jobs_ingress_class="traefik")
        async with kube_orchestrator_factory(kube_config=kube_config) as orchestrator:
            async with kube_client_factory(kube_config) as kube_client:
                await kube_client.create_pvc(  # type: ignore
                    "disk-2",
                    kube_config.namespace,
                )

                missing = await orchestrator.get_missing_disks(
                    disk_names=["disk-3", "disk-2", "disk-1"]
                )
                assert missing == ["disk-1", "disk-3"]

    @pytest.mark.asyncio
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
        disk = Disk(disk_id=disk_id, user_name=user_name, cluster_name=cluster_name)
        await kube_client.create_pvc(disk_id, ns)

        mount_path = PurePath("/mnt/disk")
        container = Container(
            image="ubuntu",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
            if v.get("persistentVolumeClaim", dict()).get("claimName") == disk_id
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="sleep infinity",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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

    @pytest.mark.asyncio
    async def test_delete_all_job_resources(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="sleep 1h",
            http_server=ContainerHTTPServer(80),
            resources=ContainerResources(
                cpu=0.1,
                memory_mb=128,
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
        await kube_orchestrator.prepare_job(job)
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

        with pytest.raises(JobNotFoundException):
            await kube_client.get_ingress(ingress_name)

        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.get_service(service_name)

        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.get_network_policy(networkpolicy_name)

    @pytest.mark.asyncio
    async def test_cleanup_old_named_ingresses(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="sleep 1h",
            http_server=ContainerHTTPServer(80),
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
        await kube_orchestrator.prepare_job(job1)
        await kube_orchestrator.start_job(job1)

        await delete_job_later(job2)
        await kube_orchestrator.prepare_job(job2)
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
        await kube_orchestrator.prepare_job(job3)
        await kube_orchestrator.start_job(job3)

        with pytest.raises(JobNotFoundException):
            await kube_client.get_ingress(job1.id)
        await kube_client.get_ingress(job2.id)
        await kube_client.get_ingress(job3.id)


class TestNodeAffinity:
    @pytest.fixture
    async def kube_orchestrator(
        self,
        storage_config_host: StorageConfig,
        registry_config: RegistryConfig,
        kube_config: KubeConfig,
        kube_job_nodes_factory: Callable[[KubeConfig], Awaitable[None]],
    ) -> AsyncIterator[KubeOrchestrator]:
        kube_config = replace(
            kube_config,
            node_label_job="job",
            node_label_gpu="gpu",
            node_label_node_pool="nodepool",
            node_label_preemptible="preemptible",
            resource_pool_types=[
                ResourcePoolType(
                    name="cpu-small", available_cpu=2, available_memory_mb=2048
                ),
                ResourcePoolType(
                    name="cpu-small-p",
                    available_cpu=2,
                    available_memory_mb=2048,
                    is_preemptible=True,
                ),
                ResourcePoolType(
                    name="cpu-large-tpu",
                    available_cpu=3,
                    available_memory_mb=14336,
                    tpu=TPUResource(
                        ipv4_cidr_block="1.1.1.1/32",
                        types=("v2-8",),
                        software_versions=("1.14",),
                    ),
                ),
                ResourcePoolType(
                    name="gpu-v100",
                    available_cpu=7,
                    available_memory_mb=61440,
                    gpu=1,
                    gpu_model=GKEGPUModels.V100.value.id,
                ),
                ResourcePoolType(
                    name="gpu-v100-p",
                    available_cpu=7,
                    available_memory_mb=61440,
                    gpu=1,
                    gpu_model=GKEGPUModels.V100.value.id,
                    is_preemptible=True,
                ),
            ],
        )
        await kube_job_nodes_factory(kube_config)
        async with KubeOrchestrator(
            storage_config=storage_config_host,
            registry_config=registry_config,
            kube_config=kube_config,
        ) as kube_orchestrator:
            yield kube_orchestrator

    @pytest.fixture
    async def start_job(
        self, kube_client: MyKubeClient
    ) -> Callable[..., AsyncContextManager[MyJob]]:
        @asynccontextmanager
        async def _create(
            kube_orchestrator: KubeOrchestrator,
            cpu: float,
            memory_mb: int,
            gpu: Optional[int] = None,
            gpu_model: Optional[str] = None,
            is_preemptible: bool = False,
            is_preemptible_node_required: bool = False,
        ) -> AsyncIterator[MyJob]:
            container = Container(
                image="ubuntu",
                command="true",
                resources=ContainerResources(
                    cpu=cpu,
                    memory_mb=memory_mb,
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
                    is_preemptible=is_preemptible,
                    is_preemptible_node_required=is_preemptible_node_required,
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

    @pytest.mark.asyncio
    async def test_unschedulable_job(
        self,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AsyncContextManager[MyJob]],
    ) -> None:
        with pytest.raises(ValueError, match="Job will not fit into cluster"):
            async with start_job(kube_orchestrator, cpu=100, memory_mb=16):
                pass

    @pytest.mark.asyncio
    async def test_cpu_job(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(kube_orchestrator, cpu=0.1, memory_mb=16) as job:
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

    @pytest.mark.asyncio
    async def test_cpu_job_on_tpu_node(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(kube_orchestrator, cpu=3, memory_mb=16) as job:
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

    @pytest.mark.asyncio
    async def test_cpu_job_not_scheduled_on_gpu_node(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AsyncContextManager[MyJob]],
    ) -> None:
        with pytest.raises(ValueError, match="Job will not fit into cluster"):
            async with start_job(kube_orchestrator, cpu=7, memory_mb=16):
                pass

    @pytest.mark.asyncio
    async def test_gpu_job(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator,
            cpu=0.1,
            memory_mb=16,
            gpu=1,
            gpu_model="nvidia-tesla-v100",
        ) as job:
            await kube_client.wait_pod_scheduled(job.id, "gpu-v100")

            job_pod = await kube_client.get_raw_pod(job.id)
            assert (
                job_pod["spec"]["affinity"]["nodeAffinity"]
                == NodeAffinity(
                    required=[
                        NodeSelectorTerm(
                            [NodeSelectorRequirement.create_in("nodepool", "gpu-v100")]
                        )
                    ]
                ).to_primitive()
            )

    @pytest.mark.asyncio
    async def test_preemptible_job_on_any_node(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator,
            cpu=0.1,
            memory_mb=16,
            gpu=1,
            gpu_model="nvidia-tesla-v100",
            is_preemptible=True,
        ) as job:
            await kube_client.wait_pod_scheduled(job.id)

            job_pod = await kube_client.get_raw_pod(job.id)
            assert (
                job_pod["spec"]["affinity"]["nodeAffinity"]
                == NodeAffinity(
                    required=[
                        NodeSelectorTerm(
                            [NodeSelectorRequirement.create_in("nodepool", "gpu-v100")]
                        ),
                        NodeSelectorTerm(
                            [
                                NodeSelectorRequirement.create_in(
                                    "nodepool", "gpu-v100-p"
                                )
                            ]
                        ),
                    ]
                ).to_primitive()
            )

    @pytest.mark.asyncio
    async def test_preemptible_job_on_preemptible_node(
        self,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        start_job: Callable[..., AsyncContextManager[MyJob]],
    ) -> None:
        async with start_job(
            kube_orchestrator,
            cpu=0.1,
            memory_mb=16,
            is_preemptible=True,
            is_preemptible_node_required=True,
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
async def delete_pod_later(
    kube_client: KubeClient,
) -> AsyncIterator[Callable[[PodDescriptor], Awaitable[None]]]:
    pods = []

    async def _add_pod(pod: PodDescriptor) -> None:
        pods.append(pod)

    yield _add_pod

    for pod in pods:
        try:
            await kube_client.delete_pod(pod.name)
        except Exception:
            pass


@pytest.fixture
async def delete_ingress_later(
    kube_client: KubeClient,
) -> AsyncIterator[Callable[[Ingress], Awaitable[None]]]:
    ingresses = []

    async def _add(ingress: Ingress) -> None:
        ingresses.append(ingress)

    yield _add

    for ingress in ingresses:
        try:
            await kube_client.delete_ingress(ingress.name)
        except Exception:
            pass


@pytest.fixture
async def delete_service_later(
    kube_client: KubeClient,
) -> AsyncIterator[Callable[[Service], Awaitable[None]]]:
    services = []

    async def _add(service: Service) -> None:
        services.append(service)

    yield _add

    for service in services:
        try:
            await kube_client.delete_service(service.name)
        except Exception:
            pass


class TestKubeClient:
    @pytest.mark.asyncio
    async def test_wait_pod_is_running_not_found(self, kube_client: KubeClient) -> None:
        with pytest.raises(JobNotFoundException):
            await kube_client.wait_pod_is_running(pod_name="unknown")

    @pytest.mark.asyncio
    async def test_wait_pod_is_running_timed_out(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_orchestrator.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        with pytest.raises(asyncio.TimeoutError):
            await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=0.1)

    @pytest.mark.asyncio
    async def test_wait_pod_is_running(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_orchestrator.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)
        pod_status = await kube_client.get_pod_status(pod.name)
        assert pod_status.phase in ("Running", "Succeeded")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "entrypoint,command",
        [(None, "/bin/echo false"), ("/bin/echo false", None), ("/bin/echo", "false")],
    )
    async def test_run_check_entrypoint_and_command(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
        entrypoint: str,
        command: str,
    ) -> None:
        container = Container(
            image="ubuntu",
            entrypoint=entrypoint,
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_orchestrator.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_terminated(pod_name=pod.name, timeout_s=60.0)

        pod_finished = await kube_client.get_pod(pod.name)

        # check that "/bin/echo" was not lost anywhere (and "false" was not executed):
        assert pod_finished.status
        assert pod_finished.status.phase == "Succeeded"

        if entrypoint is None:
            assert pod_finished.command is None
        else:
            assert pod_finished.command == shlex.split(entrypoint)

        if command is None:
            assert pod_finished.args is None
        else:
            assert pod_finished.args == shlex.split(command)

    @pytest.mark.asyncio
    async def test_create_docker_secret_non_existent_namespace(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=name,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.create_docker_secret(docker_secret)

    @pytest.mark.asyncio
    async def test_create_docker_secret_already_exists(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        try:
            await kube_client.create_docker_secret(docker_secret)

            with pytest.raises(StatusException, match="AlreadyExists"):
                await kube_client.create_docker_secret(docker_secret)
        finally:
            await kube_client.delete_secret(name, kube_config.namespace)

    @pytest.mark.asyncio
    async def test_update_docker_secret_already_exists(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        try:
            await kube_client.create_docker_secret(docker_secret)
            await kube_client.update_docker_secret(docker_secret)
        finally:
            await kube_client.delete_secret(name, kube_config.namespace)

    @pytest.mark.asyncio
    async def test_update_docker_secret_non_existent(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.update_docker_secret(docker_secret)

    @pytest.mark.asyncio
    async def test_update_docker_secret_create_non_existent(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        await kube_client.update_docker_secret(docker_secret, create_non_existent=True)
        await kube_client.update_docker_secret(docker_secret)

    @pytest.mark.asyncio
    async def test_get_raw_secret(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.get_raw_secret(name, kube_config.namespace)

        await kube_client.update_docker_secret(docker_secret, create_non_existent=True)
        raw = await kube_client.get_raw_secret(name, kube_config.namespace)
        assert raw["metadata"]["name"] == name
        assert raw["metadata"]["namespace"] == kube_config.namespace
        assert raw["data"] == docker_secret.to_primitive()["data"]

    @pytest.fixture
    async def delete_network_policy_later(
        self, kube_client: KubeClient
    ) -> AsyncIterator[Callable[[str], Awaitable[None]]]:
        names = []

        async def _add_name(name: str) -> None:
            names.append(name)

        yield _add_name

        for name in names:
            try:
                await kube_client.delete_network_policy(name)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_create_default_network_policy(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_network_policy_later: Callable[[str], Awaitable[None]],
    ) -> None:
        name = str(uuid.uuid4())
        await delete_network_policy_later(name)
        payload = await kube_client.create_default_network_policy(
            name, {"testlabel": name}, namespace_name=kube_config.namespace
        )
        assert payload["metadata"]["name"] == name

    @pytest.mark.asyncio
    async def test_create_default_network_policy_twice(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_network_policy_later: Callable[[str], Awaitable[None]],
    ) -> None:
        name = str(uuid.uuid4())
        await delete_network_policy_later(name)
        payload = await kube_client.create_default_network_policy(
            name, {"testlabel": name}, namespace_name=kube_config.namespace
        )
        assert payload["metadata"]["name"] == name
        with pytest.raises(AlreadyExistsException):
            await kube_client.create_default_network_policy(
                name, {"testlabel": name}, namespace_name=kube_config.namespace
            )

    @pytest.mark.asyncio
    async def test_get_network_policy_not_found(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.get_network_policy(name)

    @pytest.mark.asyncio
    async def test_delete_network_policy_not_found(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.delete_network_policy(name)

    @pytest.mark.asyncio
    async def test_get_pod_events(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_orchestrator.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_terminated(pod.name)

        events = await kube_client.get_pod_events(pod.name, kube_config.namespace)

        assert events
        for event in events:
            involved_object = event.involved_object
            assert involved_object["kind"] == "Pod"
            assert involved_object["namespace"] == kube_config.namespace
            assert involved_object["name"] == pod.name

    @pytest.mark.asyncio
    async def test_get_pod_events_empty(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        pod_name = str(uuid.uuid4())
        events = await kube_client.get_pod_events(pod_name, kube_config.namespace)

        assert not events

    @pytest.mark.asyncio
    async def test_service_account_not_available(
        self,
        kube_client: KubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        container = Container(
            image="lachlanevenson/k8s-kubectl:v1.10.3",
            command="get pods",
            resources=ContainerResources(cpu=0.2, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_orchestrator.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_terminated(pod_name=pod.name, timeout_s=60.0)
        pod_status = await kube_client.get_pod_status(pod.name)

        assert pod_status.container_status.exit_code != 0

    @pytest.fixture
    async def create_pod(
        self,
        kube_client: KubeClient,
        kube_orchestrator: KubeOrchestrator,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> Callable[[str], Awaitable[PodDescriptor]]:
        async def _f(job_id: str) -> PodDescriptor:
            labels = {"platform.neuromation.io/job": job_id}
            container = Container(
                image="ubuntu",
                command="true",
                resources=ContainerResources(cpu=0.1, memory_mb=128),
            )
            pod = PodDescriptor.from_job_request(
                kube_orchestrator.create_storage_volume(),
                JobRequest.create(container),
                labels=labels,
            )
            await delete_pod_later(pod)
            await kube_client.create_pod(pod)
            return pod

        return _f

    @pytest.mark.asyncio
    async def test_get_all_job_resources_links_pod(
        self,
        kube_client: MyKubeClient,
        create_pod: Callable[[str], Awaitable[PodDescriptor]],
    ) -> None:
        job_id = f"job-{uuid.uuid4()}"
        pod = await create_pod(job_id)
        link = URL(kube_client._generate_pod_url(pod.name)).path

        resources = [r async for r in kube_client.get_all_job_resources_links(job_id)]
        assert resources == [link]

    @pytest.mark.asyncio
    async def test_delete_resource_by_link_pod(
        self,
        kube_client: MyKubeClient,
        create_pod: Callable[[str], Awaitable[PodDescriptor]],
    ) -> None:
        job_id = f"job-{uuid.uuid4()}"
        pod = await create_pod(job_id)
        assert await kube_client.get_pod(pod.name)
        link = URL(kube_client._generate_pod_url(pod.name)).path

        await kube_client.delete_resource_by_link(link)
        await kube_client.wait_pod_non_existent(pod.name, timeout_s=60.0)

        with pytest.raises(JobNotFoundException):
            await kube_client.get_pod(pod.name)

    @pytest.fixture
    async def create_ingress(
        self,
        kube_client: KubeClient,
        delete_ingress_later: Callable[[Ingress], Awaitable[None]],
    ) -> Callable[[str], Awaitable[Ingress]]:
        async def _f(job_id: str) -> Ingress:
            ingress_name = f"ingress-{uuid.uuid4().hex[:6]}"
            labels = {"platform.neuromation.io/job": job_id}
            ingress = await kube_client.create_ingress(ingress_name, labels=labels)
            await delete_ingress_later(ingress)
            return ingress

        return _f

    @pytest.mark.asyncio
    async def test_get_all_job_resources_links_ingress(
        self,
        kube_client: KubeClient,
        create_ingress: Callable[[str], Awaitable[Ingress]],
    ) -> None:
        job_id = f"job-{uuid.uuid4()}"
        ingress = await create_ingress(job_id)
        link = URL(kube_client._generate_ingress_url(ingress.name)).path

        resources = [r async for r in kube_client.get_all_job_resources_links(job_id)]
        assert resources == [link]

    @pytest.mark.asyncio
    async def test_delete_resource_by_link_ingress(
        self,
        kube_client: KubeClient,
        create_ingress: Callable[[str], Awaitable[Ingress]],
    ) -> None:
        job_id = f"job-{uuid.uuid4()}"
        ingress = await create_ingress(job_id)
        assert await kube_client.get_ingress(ingress.name)

        link = URL(kube_client._generate_ingress_url(ingress.name)).path
        await kube_client.delete_resource_by_link(link)

        with pytest.raises(JobNotFoundException):
            await kube_client.get_ingress(ingress.name)

    @pytest.fixture
    async def create_service(
        self,
        kube_client: KubeClient,
        delete_service_later: Callable[[Service], Awaitable[None]],
    ) -> Callable[[str], Awaitable[Service]]:
        async def _f(job_id: str) -> Service:
            service_name = f"service-{uuid.uuid4().hex[:6]}"
            labels = {"platform.neuromation.io/job": job_id}
            service = Service(name=service_name, target_port=8080, labels=labels)
            service = await kube_client.create_service(service)
            await delete_service_later(service)
            return service

        return _f

    @pytest.mark.asyncio
    async def test_get_all_job_resources_links_service(
        self,
        kube_client: KubeClient,
        create_service: Callable[[str], Awaitable[Service]],
    ) -> None:
        job_id = f"job-{uuid.uuid4()}"
        service = await create_service(job_id)
        link = URL(kube_client._generate_service_url(service.name)).path

        resources = [r async for r in kube_client.get_all_job_resources_links(job_id)]
        assert resources == [link]

    @pytest.mark.asyncio
    async def test_delete_resource_by_link_service(
        self,
        kube_client: KubeClient,
        create_service: Callable[[str], Awaitable[Service]],
    ) -> None:
        job_id = f"job-{uuid.uuid4()}"
        service = await create_service(job_id)
        assert await kube_client.get_service(service.name)

        link = URL(kube_client._generate_service_url(service.name)).path
        await kube_client.delete_resource_by_link(link)

        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.get_service(service.name)

    @pytest.fixture
    async def create_network_policy(
        self,
        kube_client: KubeClient,
        delete_network_policy_later: Callable[[str], Awaitable[None]],
    ) -> Callable[[str], Awaitable[Dict[str, Any]]]:
        async def _f(job_id: str) -> Dict[str, Any]:
            np_name = f"networkpolicy-{uuid.uuid4().hex[:6]}"
            labels = {"platform.neuromation.io/job": job_id}

            await delete_network_policy_later(np_name)
            payload = await kube_client.create_egress_network_policy(
                np_name, pod_labels=labels, labels=labels, rules=[{}]
            )
            return payload

        return _f

    @pytest.mark.asyncio
    async def test_get_all_job_resources_links_job_network_policy(
        self,
        kube_client: KubeClient,
        create_network_policy: Callable[[str], Awaitable[Dict[str, Any]]],
    ) -> None:
        job_id = f"job-{uuid.uuid4()}"
        payload = await create_network_policy(job_id)
        link = payload["metadata"]["selfLink"]

        resources = [r async for r in kube_client.get_all_job_resources_links(job_id)]
        assert resources == [link]

    @pytest.mark.asyncio
    async def test_delete_resource_by_link_network_policy(
        self,
        kube_client: KubeClient,
        create_network_policy: Callable[[str], Awaitable[Dict[str, Any]]],
    ) -> None:
        job_id = f"job-{uuid.uuid4()}"
        payload = await create_network_policy(job_id)
        link = payload["metadata"]["selfLink"]
        np_name = payload["metadata"]["name"]

        await kube_client.delete_resource_by_link(link)

        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.get_network_policy(np_name)


@pytest.fixture
async def mock_kubernetes_server() -> AsyncIterator[ApiConfig]:
    async def _get_pod(request: web.Request) -> web.Response:
        payload: Dict[str, Any] = {
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
            container = Container(image="ubuntu", command=command, resources=resources)
            job_request = JobRequest.create(container)
            pod = PodDescriptor.from_job_request(
                kube_orchestrator.create_storage_volume(), job_request
            )
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

    @pytest.mark.asyncio
    async def test_shm_extended_request_parameter_omitted(
        self,
        run_command_get_status: Callable[..., Awaitable[JobStatusItem]],
        command_assert_shm_64_mb: str,
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory_mb=128)
        status_item = await run_command_get_status(resources, command_assert_shm_64_mb)
        assert status_item.status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_shm_extended_request_parameter_not_requested(
        self,
        run_command_get_status: Callable[..., Awaitable[JobStatusItem]],
        command_assert_shm_64_mb: str,
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=False)
        status_item = await run_command_get_status(resources, command_assert_shm_64_mb)
        assert status_item.status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_shm_extended_request_parameter_requested(
        self,
        run_command_get_status: Callable[..., Awaitable[JobStatusItem]],
        command_assert_shm_64_mb: str,
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=True)
        status_item = await run_command_get_status(resources, command_assert_shm_64_mb)
        assert status_item.status == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_shm_extended_not_requested_try_create_huge_file(
        self, run_command_get_status: Callable[..., Awaitable[JobStatusItem]]
    ) -> None:
        command = "dd if=/dev/zero of=/dev/zero  bs=999999M  count=1"
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=False)
        status_actual = await run_command_get_status(resources, command)
        status_expected = JobStatusItem.create(
            status=JobStatus.FAILED,
            reason=JobStatusReason.OOM_KILLED,
            exit_code=137,
            description=mock.ANY,
        )
        assert status_actual == status_expected, f"actual: '{status_actual}'"

    @pytest.mark.asyncio
    async def test_shm_extended_requested_try_create_huge_file(
        self, run_command_get_status: Callable[..., Awaitable[JobStatusItem]]
    ) -> None:
        command = "dd if=/dev/zero of=/dev/shm/test bs=256M  count=1"
        resources = ContainerResources(cpu=0.1, memory_mb=1024, shm=True)
        status_actual = await run_command_get_status(resources, command)
        status_expected = JobStatusItem.create(status=JobStatus.SUCCEEDED, exit_code=0)
        assert status_actual == status_expected, f"actual: '{status_actual}'"

    @pytest.mark.asyncio
    async def test_shm_extended_not_requested_try_create_small_file(
        self, run_command_get_status: Callable[..., Awaitable[JobStatusItem]]
    ) -> None:
        command = "dd if=/dev/zero of=/dev/shm/test  bs=32M  count=1"
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=False)
        status_actual = await run_command_get_status(resources, command)
        status_expected = JobStatusItem.create(status=JobStatus.SUCCEEDED, exit_code=0)
        assert status_actual == status_expected, f"actual: '{status_actual}'"


class TestPreemption:
    @pytest.fixture
    async def kube_orchestrator(
        self,
        kube_orchestrator_factory: Callable[..., KubeOrchestrator],
        kube_config_node_preemptible: KubeConfig,
    ) -> AsyncIterator[KubeOrchestrator]:
        async with kube_orchestrator_factory(
            kube_config=kube_config_node_preemptible
        ) as kube_orchestrator:
            yield kube_orchestrator

    @pytest.mark.asyncio
    async def test_non_preemptible_job(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container), cluster_name="test-cluster"
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.prepare_job(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_running

        await kube_client.delete_pod(pod_name, force=True)

        # triggering pod recreation
        with pytest.raises(JobNotFoundException, match="was not found"):
            await kube_orchestrator.get_job_status(job)

    @pytest.mark.asyncio
    async def test_preemptible_job_lost_running_pod(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                # marking the job as preemptible
                is_preemptible=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.prepare_job(job)
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                # marking the job as preemptible
                is_preemptible=True,
                is_preemptible_node_required=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.prepare_job(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_scheduled(pod_name, node_name)

        await kube_client.delete_node(node_name)
        # deleting node initiates it's pods deletion
        await kube_client.wait_pod_non_existent(pod_name, timeout_s=60.0)

        # triggering pod recreation
        job_status = await kube_orchestrator.get_job_status(job)
        assert job_status.is_pending

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                # marking the job as preemptible
                is_preemptible=True,
                is_preemptible_node_required=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.prepare_job(job)
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

    @pytest.mark.asyncio
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
            image="ubuntu",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest.create(container),
                cluster_name="test-cluster",
                # marking the job as preemptible
                is_preemptible=True,
                is_preemptible_node_required=True,
            ),
        )
        await delete_job_later(job)
        await kube_orchestrator.prepare_job(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_scheduled(pod_name, node_name)

        await kube_client.delete_pod(pod_name, force=True)

        # changing the job details to trigger pod creation failure
        container = Container(
            image="ubuntu",
            command="bash -c 'sleep infinity'",
            resources=ContainerResources(cpu=0.1, memory_mb=-128),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            record=JobRecord.create(
                request=JobRequest(job_id=job.id, container=container),
                cluster_name="test-cluster",
                # marking the job as preemptible
                is_preemptible=True,
                is_preemptible_node_required=True,
            ),
        )

        # triggering pod recreation
        with pytest.raises(
            JobNotFoundException, match=f"Pod '{pod_name}' not found. Job '{job.id}'"
        ):
            await kube_orchestrator.get_job_status(job)


class TestRestartPolicy:
    @pytest.mark.asyncio
    async def test_restart_failing(
        self,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu",
            command="false",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
        await kube_orchestrator.prepare_job(job)
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

    @pytest.mark.asyncio
    async def test_restart_failing_succeeded(
        self,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
        await kube_orchestrator.prepare_job(job)
        await kube_orchestrator.start_job(job)
        pod_name = job.id

        await kube_client.wait_pod_is_terminated(pod_name=pod_name, timeout_s=60.0)

        status = await kube_orchestrator.get_job_status(job)
        assert status == JobStatusItem.create(status=JobStatus.SUCCEEDED, exit_code=0)

    @pytest.mark.asyncio
    async def test_restart_always(
        self,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu",
            command="false",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
        await kube_orchestrator.prepare_job(job)
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

    @pytest.mark.asyncio
    async def test_restart_always_succeeded(
        self,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
        kube_orchestrator: KubeOrchestrator,
    ) -> None:
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
        await kube_orchestrator.prepare_job(job)
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
