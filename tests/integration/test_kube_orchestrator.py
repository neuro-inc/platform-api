import asyncio
import io
import time
import uuid
from pathlib import PurePath
from typing import Any, Awaitable, Callable, Dict, Iterator, Optional, Sequence

import pytest
from async_timeout import timeout
from elasticsearch import AuthenticationException
from yarl import URL

from platform_api.elasticsearch import (
    Elasticsearch,
    ElasticsearchConfig,
    create_elasticsearch_client,
)
from platform_api.orchestrator import (
    Job,
    JobError,
    JobNotFoundException,
    JobRequest,
    JobStatus,
    KubeConfig,
    KubeOrchestrator,
    LogReader,
    Orchestrator,
)
from platform_api.orchestrator.job import JobStatusItem
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerVolume,
)
from platform_api.orchestrator.kube_client import (
    KubeClient,
    PodDescriptor,
    PodStatus,
    SecretRef,
    StatusException,
)
from platform_api.orchestrator.kube_orchestrator import (
    JobStatusItemFactory,
    build_pod_descriptior,
)
from platform_api.orchestrator.logs import ElasticsearchLogReader, PodContainerLogReader
from tests.conftest import random_str

from .conftest import MyKubeClient


class MyJob(Job):
    def __init__(self, orchestrator: Orchestrator, *args: Any, **kwargs: Any) -> None:
        self._orchestrator = orchestrator
        kwargs.setdefault("owner", "test-owner")
        if args:
            super().__init__(orchestrator.config, *args, **kwargs)
        else:
            super().__init__(orchestrator_config=orchestrator.config, **kwargs)

    async def start(self) -> JobStatus:
        status = await self._orchestrator.start_job(self, "test-token")
        assert status == JobStatus.PENDING
        return status

    async def delete(self) -> JobStatus:
        return await self._orchestrator.delete_job(self)

    async def query_status(self) -> JobStatus:
        return (await self._orchestrator.get_job_status(self)).status


@pytest.fixture
async def job_nginx(kube_orchestrator: KubeOrchestrator) -> MyJob:
    container = Container(
        image="ubuntu",
        command="sleep 5",
        resources=ContainerResources(cpu=0.1, memory_mb=256),
    )
    job_request = JobRequest.create(container)
    job = MyJob(orchestrator=kube_orchestrator, job_request=job_request)
    return job


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
        job = MyJob(orchestrator=kube_orchestrator, job_request=job_request)
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
            job_request=job_request,
            owner="invalid_name",
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
            orchestrator=kube_orchestrator, job_request=job_request_second
        )
        with pytest.raises(JobError):
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
        job = MyJob(orchestrator=kube_orchestrator, job_request=job_request)

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
            orchestrator=kube_orchestrator, job_request=JobRequest.create(container)
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
            orchestrator=kube_orchestrator, job_request=JobRequest.create(container)
        )
        try:
            await job.start()
            await self.wait_for_failure(job)

            status_item = await kube_orchestrator.get_job_status(job)
            expected_description = "".join(f"{i}\n" for i in reversed(range(1, 81)))
            expected_description += "\nExit code: 1"
            assert status_item == JobStatusItem.create(
                JobStatus.FAILED, reason="Error", description=expected_description
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
            orchestrator=kube_orchestrator, job_request=JobRequest.create(container)
        )
        try:
            await job.start()

            status_item = await kube_orchestrator.get_job_status(job)
            assert status_item == JobStatusItem.create(
                JobStatus.PENDING,
                reason="Cluster doesn't have resources to fulfill request.",
            )
        finally:
            await job.delete()

    @pytest.mark.asyncio
    async def test_volumes(
        self, kube_config: KubeConfig, kube_orchestrator: KubeOrchestrator
    ) -> None:
        await self._test_volumes(kube_config, kube_orchestrator)

    @pytest.mark.asyncio
    async def test_volumes_nfs(
        self, kube_config_nfs: KubeConfig, kube_orchestrator_nfs: KubeOrchestrator
    ) -> None:
        await self._test_volumes(kube_config_nfs, kube_orchestrator_nfs)

    async def _test_volumes(
        self, kube_config: KubeConfig, kube_orchestrator: KubeOrchestrator
    ) -> None:
        volumes = [
            ContainerVolume(
                uri=URL(
                    kube_config.storage.uri_scheme
                    + "://"
                    + str(kube_config.storage_mount_path)
                ),
                src_path=PurePath(kube_config.storage_mount_path),
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
            job_request=JobRequest.create(write_container),
        )

        read_container = Container(
            image="ubuntu",
            command=f"""bash -c '[ "$(cat {file_path})" == "test" ]'""",
            volumes=volumes,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        read_job = MyJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(read_container),
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
            orchestrator=kube_orchestrator, job_request=JobRequest.create(container)
        )

        try:
            await job.start()
            status = await self.wait_for_completion(job)
            assert status == expected_status
        finally:
            await job.delete()

    @pytest.mark.asyncio
    async def test_job_with_exposed_http_server_no_job_name(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
        wait_for_job_service: Callable[..., Awaitable[None]],
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator=kube_orchestrator, job_request=JobRequest.create(container)
        )
        try:
            await job.start()

            assert job.http_host_named is None

            await wait_for_job_service(
                kube_ingress_ip, host=job.http_host, job_id=job.id
            )

            ingress = await kube_client.get_ingress(kube_config.jobs_ingress_name)
            assert ingress.find_rule_index_by_host(job.http_host) >= 0

            ingress = await kube_client.get_ingress(kube_config.jobs_ingress_auth_name)
            assert ingress.find_rule_index_by_host(job.http_host) == -1
        finally:
            await job.delete()

    @pytest.mark.asyncio
    async def test_job_with_exposed_http_server_with_job_name(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
        wait_for_job_service: Callable[..., Awaitable[None]],
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(container),
            name=f"job-{uuid.uuid4().hex[:5]}",
            owner="owner",
        )
        try:
            await job.start()
            assert not (job.requires_http_auth), str(job)

            for host in [job.http_host, job.http_host_named]:
                assert host
                await wait_for_job_service(kube_ingress_ip, host=host, job_id=job.id)
                ingress = await kube_client.get_ingress(kube_config.jobs_ingress_name)
                assert ingress.find_rule_index_by_host(host) >= 0

                ingress = await kube_client.get_ingress(
                    kube_config.jobs_ingress_auth_name
                )
                assert ingress.find_rule_index_by_host(host) == -1

        finally:
            await job.delete()

    @pytest.mark.asyncio
    async def test_job_with_exposed_http_server_with_auth_no_job_name(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
        wait_for_job_service: Callable[..., Awaitable[None]],
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
            http_server=ContainerHTTPServer(port=80, requires_auth=True),
        )
        job = MyJob(
            orchestrator=kube_orchestrator, job_request=JobRequest.create(container)
        )
        try:
            await job.start()

            assert job.http_host_named is None

            await wait_for_job_service(
                kube_ingress_ip, host=job.http_host, job_id=job.id
            )

            ingress = await kube_client.get_ingress(kube_config.jobs_ingress_name)
            assert ingress.find_rule_index_by_host(job.http_host) == -1

            ingress = await kube_client.get_ingress(kube_config.jobs_ingress_auth_name)
            assert ingress.find_rule_index_by_host(job.http_host) >= 0
        finally:
            await job.delete()

    @pytest.mark.asyncio
    async def test_job_with_exposed_http_server_with_auth_with_job_name(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_ingress_ip: str,
        kube_client: KubeClient,
        wait_for_job_service: Callable[..., Awaitable[None]],
    ) -> None:
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
            http_server=ContainerHTTPServer(port=80, requires_auth=True),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(container),
            name=f"job-{uuid.uuid4().hex[:5]}",
            owner="owner",
        )
        try:
            await job.start()

            for http_host in [job.http_host, job.http_host_named]:
                assert http_host
                await wait_for_job_service(
                    kube_ingress_ip, host=http_host, job_id=job.id
                )

                ingress = await kube_client.get_ingress(kube_config.jobs_ingress_name)
                assert ingress.find_rule_index_by_host(http_host) == -1

                ingress = await kube_client.get_ingress(
                    kube_config.jobs_ingress_auth_name
                )
                assert ingress.find_rule_index_by_host(http_host) >= 0
        finally:
            await job.delete()

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
                job_request=JobRequest.create(server_cont),
                name=job_name,
            )

        yield impl

    @pytest.fixture
    def create_client_job(
        self, kube_orchestrator: KubeOrchestrator
    ) -> Iterator[Callable[[str], MyJob]]:
        def impl(server_hostname: str) -> MyJob:
            client_cont = Container(
                image="ubuntu",
                command=f"curl --silent --fail http://{server_hostname}/",
                resources=ContainerResources(cpu=0.1, memory_mb=128),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                job_request=JobRequest.create(client_cont),
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
        wait_for_job_service: Callable[..., Awaitable[None]],
    ) -> None:
        server_job = create_server_job()
        await delete_job_later(server_job)
        await server_job.start()
        server_hostname = server_job.internal_hostname
        assert server_hostname
        await wait_for_job_service(
            kube_ingress_ip, host=server_job.http_host, job_id=server_job.id
        )

        client_job = create_client_job(server_hostname)
        await delete_job_later(client_job)
        await client_job.start()
        assert self.wait_for_success(job=client_job)

    @pytest.mark.asyncio
    async def test_job_check_http_hostname_no_job_name(
        self,
        kube_config: KubeConfig,
        create_server_job: Callable[..., MyJob],
        create_client_job: Callable[[str], MyJob],
        kube_ingress_ip: str,
        delete_job_later: Callable[[Job], Awaitable[None]],
        wait_for_job_service: Callable[..., Awaitable[None]],
    ) -> None:
        server_job = create_server_job()
        await delete_job_later(server_job)
        await server_job.start()

        http_host = server_job.http_host
        await wait_for_job_service(
            kube_ingress_ip, host=http_host, job_id=server_job.id
        )
        client_job = create_client_job(http_host)
        await delete_job_later(client_job)
        await client_job.start()
        assert self.wait_for_success(job=client_job)

        assert server_job.http_host_named is None

    @pytest.mark.asyncio
    async def test_job_check_http_hostname_with_job_name(
        self,
        kube_config: KubeConfig,
        create_server_job: Callable[..., MyJob],
        create_client_job: Callable[[str], MyJob],
        kube_ingress_ip: str,
        delete_job_later: Callable[[Job], Awaitable[None]],
        wait_for_job_service: Callable[..., Awaitable[None]],
    ) -> None:
        server_job_name = f"server-job-{random_str()}"
        server_job = create_server_job(job_name=server_job_name)
        await delete_job_later(server_job)
        await server_job.start()

        http_host = server_job.http_host
        await wait_for_job_service(
            kube_ingress_ip, host=http_host, job_id=server_job.id
        )
        client_job = create_client_job(http_host)
        await delete_job_later(client_job)
        await client_job.start()
        assert self.wait_for_success(job=client_job)

        assert server_job.http_host_named
        http_host = server_job.http_host_named
        await wait_for_job_service(
            kube_ingress_ip, host=http_host, job_id=server_job.id
        )
        client_job = create_client_job(http_host)
        await delete_job_later(client_job)
        await client_job.start()
        assert self.wait_for_success(job=client_job)

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
                job_request=JobRequest.create(server_cont),
            )

        def create_client_job(server_hostname: str) -> MyJob:
            client_cont = Container(
                image="ubuntu",
                command=f"curl --silent --fail http://{server_hostname}:12345/",
                resources=ContainerResources(cpu=0.1, memory_mb=128),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                job_request=JobRequest.create(client_cont),
            )

        server_job = create_server_job()
        await delete_job_later(server_job)
        await server_job.start()
        await self.wait_until_running(server_job)

        assert server_job.internal_hostname
        client_job = create_client_job(server_job.internal_hostname)
        await delete_job_later(client_job)
        await client_job.start()
        assert self.wait_for_success(client_job)


class TestLogReader:
    async def _consume_log_reader(
        self, log_reader: LogReader, chunk_size: int = -1
    ) -> bytes:
        istream = io.BytesIO()
        try:
            async with log_reader:
                while True:
                    chunk = await log_reader.read(chunk_size)
                    if not chunk:
                        break
                    assert chunk_size < 0 or len(chunk) <= chunk_size
                    istream.write(chunk)
        except asyncio.CancelledError:
            pass
        istream.flush()
        istream.seek(0)
        return istream.read()

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
            orchestrator=kube_orchestrator, job_request=JobRequest.create(container)
        )
        await delete_job_later(job)
        await job.start()

        pod_name = job.id
        await kube_client.wait_pod_is_running(pod_name=pod_name, timeout_s=60.0)
        raw_pod = await kube_client.get_raw_pod(pod_name)
        assert raw_pod["metadata"]["labels"] == {
            "job": job.id,
            "platform.neuromation.io/job": job.id,
            "platform.neuromation.io/user": job.owner,
        }

        policy_name = "neurouser-" + job.owner
        raw_policy = await kube_client.get_network_policy(policy_name)
        assert raw_policy["spec"]["podSelector"]["matchLabels"] == {
            "platform.neuromation.io/user": job.owner
        }

    @pytest.mark.asyncio
    async def test_read_instantly_succeeded(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = build_pod_descriptior(kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""

    @pytest.mark.asyncio
    async def test_read_instantly_failed(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        command = 'bash -c "echo -n Failure!; false"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = build_pod_descriptior(kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"Failure!"

    @pytest.mark.asyncio
    async def test_read_timed_out(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        command = 'bash -c "sleep 5; echo -n Success!"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = build_pod_descriptior(kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=pod.name,
            container_name=pod.name,
            client_read_timeout_s=1,
        )
        with pytest.raises(asyncio.TimeoutError):
            await self._consume_log_reader(log_reader)

    @pytest.mark.asyncio
    async def test_read_succeeded(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = build_pod_descriptior(kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        payload = await self._consume_log_reader(log_reader, chunk_size=1)
        expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
        assert payload == expected_payload.encode()

    @pytest.mark.asyncio
    async def test_read_cancelled(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        command = 'bash -c "for i in {1..60}; do echo $i; sleep 1; done"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = build_pod_descriptior(kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        task = asyncio.ensure_future(self._consume_log_reader(log_reader, chunk_size=1))
        await asyncio.sleep(10)
        task.cancel()
        payload = await task
        expected_payload = "\n".join(str(i) for i in range(1, 6))
        assert payload.startswith(expected_payload.encode())

    @pytest.mark.asyncio
    async def test_create_elasticsearch_client_no_auth_header_fail(
        self, es_hosts_auth: Sequence[str]
    ) -> None:
        es_config = ElasticsearchConfig(hosts=es_hosts_auth)
        with pytest.raises(AuthenticationException):
            async with create_elasticsearch_client(config=es_config) as es_client:
                await es_client.ping()

    @pytest.mark.asyncio
    async def test_create_elasticsearch_client_wrong_auth_fail(
        self, es_hosts_auth: Sequence[str]
    ) -> None:
        es_config = ElasticsearchConfig(
            hosts=es_hosts_auth, user="wrong-user", password="wrong-pw"
        )
        with pytest.raises(AuthenticationException):
            async with create_elasticsearch_client(es_config) as es_client:
                await es_client.ping()

    @pytest.mark.asyncio
    async def test_create_elasticsearch_client_correct_credentials(
        self, es_hosts_auth: Sequence[str]
    ) -> None:
        es_config = ElasticsearchConfig(
            hosts=es_hosts_auth, user="testuser", password="password"
        )
        async with create_elasticsearch_client(es_config) as es_client:
            await es_client.ping()

    @pytest.mark.asyncio
    async def test_elasticsearch_log_reader(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
        es_client: Elasticsearch,
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = build_pod_descriptior(kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_terminated(pod.name)

        await self._check_kube_logs(
            kube_client,
            namespace_name=kube_config.namespace,
            pod_name=pod.name,
            container_name=pod.name,
            expected_payload=expected_payload,
        )

        await self._check_es_logs(
            es_client,
            namespace_name=kube_config.namespace,
            pod_name=pod.name,
            container_name=pod.name,
            expected_payload=expected_payload,
        )

    async def _check_kube_logs(
        self,
        kube_client: KubeClient,
        namespace_name: str,
        pod_name: str,
        container_name: str,
        expected_payload: Any,
    ) -> None:
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod_name, container_name=container_name
        )
        payload = await self._consume_log_reader(log_reader, chunk_size=1)
        assert payload == expected_payload, "Pod logs did not match."

    async def _check_es_logs(
        self,
        es_client: Elasticsearch,
        namespace_name: str,
        pod_name: str,
        container_name: str,
        expected_payload: Any,
        timeout_s: float = 120.0,
        interval_s: float = 1.0,
    ) -> None:
        payload = b""
        try:
            async with timeout(timeout_s):
                while True:
                    log_reader = ElasticsearchLogReader(
                        es_client,
                        namespace_name=namespace_name,
                        pod_name=pod_name,
                        container_name=container_name,
                    )
                    payload = await self._consume_log_reader(log_reader, chunk_size=1)
                    if payload == expected_payload:
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail(f"Pod logs did not match. Last payload: {payload}")

    @pytest.mark.asyncio
    async def test_elasticsearch_log_reader_empty(
        self, es_client: Elasticsearch
    ) -> None:
        namespace_name = pod_name = container_name = str(uuid.uuid4())
        log_reader = ElasticsearchLogReader(
            es_client,
            namespace_name=namespace_name,
            pod_name=pod_name,
            container_name=container_name,
        )
        payload = await self._consume_log_reader(log_reader, chunk_size=1)
        assert payload == b""

    @pytest.mark.asyncio
    async def test_get_job_log_reader(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: MyKubeClient,
        delete_job_later: Callable[[Job], Awaitable[None]],
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job = MyJob(
            orchestrator=kube_orchestrator, job_request=JobRequest.create(container)
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job, token="test-token")
        pod_name = job.id

        await kube_client.wait_pod_is_terminated(pod_name)

        log_reader = await kube_orchestrator.get_job_log_reader(job)
        assert isinstance(log_reader, PodContainerLogReader)

        await kube_client.delete_pod(pod_name)

        timeout_s = 120.0
        interval_s = 1.0
        payload = b""
        try:
            async with timeout(timeout_s):
                while True:
                    log_reader = await kube_orchestrator.get_job_log_reader(job)
                    assert isinstance(log_reader, ElasticsearchLogReader)
                    payload = await self._consume_log_reader(log_reader, chunk_size=1)
                    if payload == expected_payload:
                        break
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail(f"Pod logs did not match. Last payload: {payload}")


class TestPodContainerDevShmSettings:
    async def _consume_log_reader(
        self, log_reader: LogReader, chunk_size: int = -1
    ) -> bytes:
        istream = io.BytesIO()
        try:
            async with log_reader:
                while True:
                    chunk = await log_reader.read(chunk_size)
                    if not chunk:
                        break
                    assert chunk_size < 0 or len(chunk) <= chunk_size
                    istream.write(chunk)
        except asyncio.CancelledError:
            pass
        istream.flush()
        istream.seek(0)
        return istream.read()

    async def run_command_get_logs(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
        resources: ContainerResources,
    ) -> bytes:
        command = "/bin/df --block-size M --output=avail /dev/shm"
        container = Container(image="ubuntu", command=command, resources=resources)
        job_request = JobRequest.create(container)
        pod = build_pod_descriptior(kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        return await self._consume_log_reader(log_reader, chunk_size=1)

    async def _get_non_pending_status_for_pod(
        self,
        kube_client: KubeClient,
        pod_name: str,
        interval_s: float = 0.5,
        max_time: float = 180,
    ) -> PodStatus:
        t0 = time.monotonic()
        while True:
            pod_status = await kube_client.get_pod_status(pod_name)
            if pod_status != "pending":
                return pod_status
            await asyncio.sleep(max(interval_s, time.monotonic() - t0))
            if time.monotonic() - t0 > max_time:
                pytest.fail("too long")
            interval_s *= 1.5

    async def run_command_get_status(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
        resources: ContainerResources,
        command: str,
    ) -> JobStatusItem:
        container = Container(image="ubuntu", command=command, resources=resources)
        job_request = JobRequest.create(container)
        pod = build_pod_descriptior(kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        await self._consume_log_reader(log_reader)
        pod_status = await self._get_non_pending_status_for_pod(kube_client, pod.name)
        return JobStatusItemFactory(pod_status).create()

    @pytest.mark.asyncio
    async def test_shm_extended_request_parameter_omitted(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory_mb=128)
        run_output = await self.run_command_get_logs(
            kube_config, kube_client, delete_pod_later, resources
        )
        assert b"64M" in run_output

    @pytest.mark.asyncio
    async def test_shm_extended_request_parameter_not_requested(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=False)
        run_output = await self.run_command_get_logs(
            kube_config, kube_client, delete_pod_later, resources
        )
        assert b"64M" in run_output

    @pytest.mark.asyncio
    async def test_shm_extended_request_parameter_requested(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=True)
        run_output = await self.run_command_get_logs(
            kube_config, kube_client, delete_pod_later, resources
        )
        assert b"64M" not in run_output

    @pytest.mark.asyncio
    async def test_shm_extended_not_requested_try_create_huge_file(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        command = "dd if=/dev/zero of=/dev/zero  bs=999999M  count=1"
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=False)
        run_output = await self.run_command_get_status(
            kube_config, kube_client, delete_pod_later, resources, command
        )
        job_status = JobStatusItem.create(
            status=JobStatus.FAILED, reason="OOMKilled", description="\nExit code: 137"
        )
        assert job_status == run_output, f"actual: '{run_output}'"

    @pytest.mark.asyncio
    async def test_shm_extended_requested_try_create_huge_file(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        command = "dd if=/dev/zero of=/dev/shm/test bs=256M  count=1"
        resources = ContainerResources(cpu=0.1, memory_mb=1024, shm=True)
        run_output = await self.run_command_get_status(
            kube_config, kube_client, delete_pod_later, resources, command
        )
        assert JobStatusItem.create(status=JobStatus.SUCCEEDED) == run_output

    @pytest.mark.asyncio
    async def test_shm_extended_not_requested_try_create_small_file(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        command = "dd if=/dev/zero of=/dev/shm/test  bs=32M  count=1"
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=False)
        run_output = await self.run_command_get_status(
            kube_config, kube_client, delete_pod_later, resources, command
        )
        assert JobStatusItem.create(status=JobStatus.SUCCEEDED) == run_output


class TestNodeSelector:
    @pytest.mark.asyncio
    async def test_pod_node_selector(
        self,
        kube_config: KubeConfig,
        kube_client: MyKubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
        delete_node_later: Callable[[str], Awaitable[None]],
        default_node_capacity: Dict[str, Any],
    ) -> None:
        node_name = str(uuid.uuid4())
        await delete_node_later(node_name)

        labels = {"gpu": f"{node_name}-gpu"}
        await kube_client.create_node(
            node_name, capacity=default_node_capacity, labels=labels
        )

        pod_name = str(uuid.uuid4())
        pod = PodDescriptor(name=pod_name, image="ubuntu:latest", node_selector=labels)

        await delete_pod_later(pod)
        await kube_client.create_pod(pod)

        await kube_client.wait_pod_scheduled(pod_name, node_name)

    @pytest.mark.asyncio
    async def test_gpu(
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
            resources=ContainerResources(cpu=0.1, memory_mb=128, gpu=1),
        )
        job = MyJob(
            orchestrator=kube_orchestrator, job_request=JobRequest.create(container)
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job, token="test-token")
        pod_name = job.id

        await kube_client.wait_pod_scheduled(pod_name, node_name)


class TestPreemption:
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
            job_request=JobRequest.create(container),
            # marking the job as preemptible
            is_preemptible=True,
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job, token="test-token")
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
            job_request=JobRequest.create(container),
            # marking the job as preemptible
            is_preemptible=True,
            is_forced_to_preemptible_pool=True,
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job, token="test-token")
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
            job_request=JobRequest.create(container),
            # marking the job as preemptible
            is_preemptible=True,
            is_forced_to_preemptible_pool=True,
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job, token="test-token")
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
            job_request=JobRequest.create(container),
            # marking the job as preemptible
            is_preemptible=True,
            is_forced_to_preemptible_pool=True,
        )
        await delete_job_later(job)
        await kube_orchestrator.start_job(job, token="test-token")
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
            job_request=JobRequest(job_id=job.id, container=container),
            # marking the job as preemptible
            is_preemptible=True,
            is_forced_to_preemptible_pool=True,
        )

        # triggering pod recreation
        with pytest.raises(
            JobNotFoundException, match=f"Pod '{pod_name}' not found. Job '{job.id}'"
        ):
            await kube_orchestrator.get_job_status(job)
