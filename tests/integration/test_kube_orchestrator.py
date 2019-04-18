import asyncio
import io
import time
import uuid
from pathlib import PurePath
from typing import Callable, Optional
from unittest import mock
from uuid import uuid4

import aiohttp
import pytest
from async_timeout import timeout
from yarl import URL

from platform_api.orchestrator import (
    Job,
    JobError,
    JobNotFoundException,
    JobRequest,
    JobStatus,
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
from platform_api.orchestrator.kube_orchestrator import (
    AlreadyExistsException,
    DockerRegistrySecret,
    Ingress,
    IngressRule,
    JobStatusItemFactory,
    KubeClientException,
    PodContainerStats,
    PodDescriptor,
    SecretRef,
    Service,
    StatusException,
)
from platform_api.orchestrator.logs import ElasticsearchLogReader, PodContainerLogReader


class MyJob(Job):
    def __init__(self, orchestrator: Orchestrator, *args, **kwargs) -> None:
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
async def job_nginx(kube_orchestrator):
    container = Container(
        image="ubuntu",
        command="sleep 5",
        resources=ContainerResources(cpu=0.1, memory_mb=256),
    )
    job_request = JobRequest.create(container)
    job = MyJob(orchestrator=kube_orchestrator, job_request=job_request)
    return job


@pytest.fixture
async def delete_job_later(kube_orchestrator):
    jobs = []

    async def _add_job(job):
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
    ):
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

    async def wait_for_completion(self, *args, **kwargs):
        def _predicate(status: JobStatus) -> bool:
            return status.is_finished

        return await self._wait_for(*args, **kwargs, status_predicate=_predicate)

    async def wait_for_failure(self, *args, **kwargs):
        def _predicate(status: JobStatus) -> bool:
            if not status.is_finished:
                return False
            assert status == JobStatus.FAILED
            return True

        await self._wait_for(*args, **kwargs, status_predicate=_predicate)

    async def wait_for_success(self, *args, **kwargs):
        def _predicate(status: JobStatus) -> bool:
            if not status.is_finished:
                return False
            assert status == JobStatus.SUCCEEDED
            return True

        await self._wait_for(*args, **kwargs, status_predicate=_predicate)

    async def wait_until_running(self, *args, **kwargs):
        def _predicate(status: JobStatus) -> bool:
            assert not status.is_finished
            return status == JobStatus.RUNNING

        await self._wait_for(*args, **kwargs, status_predicate=_predicate)

    @pytest.mark.asyncio
    async def test_start_job_happy_path(self, job_nginx, kube_orchestrator):
        await job_nginx.start()
        await self.wait_for_success(job_nginx)

        pod = await kube_orchestrator._client.get_pod(job_nginx.id)
        assert pod.image_pull_secrets == [SecretRef(f"neurouser-{job_nginx.owner}")]

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_start_job_broken_image(self, kube_orchestrator):
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
    async def test_start_job_bad_name(self, kube_orchestrator):
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
    async def test_start_job_with_not_unique_id(self, kube_orchestrator, job_nginx):
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
    async def test_status_job_not_exist(self, job_nginx):
        with pytest.raises(JobNotFoundException):
            await job_nginx.query_status()

    @pytest.mark.asyncio
    async def test_delete_job_not_exist(self, job_nginx):
        with pytest.raises(JobNotFoundException):
            await job_nginx.delete()

    @pytest.mark.asyncio
    async def test_broken_job_id(self, kube_orchestrator):
        job_id = "some_BROCKEN_JOB-123@#$%^&*(______------ID"
        container = Container(
            image="python", resources=ContainerResources(cpu=0.1, memory_mb=128)
        )
        job_request = JobRequest(job_id=job_id, container=container)
        job = MyJob(orchestrator=kube_orchestrator, job_request=job_request)

        with pytest.raises(JobError):
            await job.start()

    @pytest.mark.asyncio
    async def test_job_succeeded(self, kube_orchestrator):
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
    async def test_job_failed_error(self, kube_orchestrator):
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
    async def test_job_bunch_of_cpu(self, kube_orchestrator):
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
    async def test_volumes(self, kube_config, kube_orchestrator):
        await self._test_volumes(kube_config, kube_orchestrator)

    @pytest.mark.asyncio
    async def test_volumes_nfs(self, kube_config_nfs, kube_orchestrator_nfs):
        await self._test_volumes(kube_config_nfs, kube_orchestrator_nfs)

    async def _test_volumes(self, kube_config, kube_orchestrator):
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
    async def test_env(self, kube_orchestrator, expected_result, expected_status):
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

    @pytest.fixture
    async def ingress(self, kube_client):
        ingress_name = str(uuid.uuid4())
        ingress = await kube_client.create_ingress(ingress_name)
        yield ingress
        await kube_client.delete_ingress(ingress.name)

    @pytest.mark.asyncio
    async def test_ingress(self, kube_client, ingress):
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
    async def test_remove_ingress_rule(self, kube_client, ingress):
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.remove_ingress_rule(ingress.name, "unknown")

    @pytest.mark.asyncio
    async def test_delete_ingress_failure(self, kube_client):
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.delete_ingress("unknown")

    @pytest.mark.asyncio
    async def test_service(self, kube_client):
        service_name = f"job-{uuid.uuid4()}"
        service = Service(name=service_name, target_port=8080)
        try:
            result_service = await kube_client.create_service(service)
            assert result_service.name == service_name
            assert result_service.target_port == 8080
            assert result_service.port == 80
        finally:
            await kube_client.delete_service(service_name)

    async def _wait_for_job_service(
        self,
        kube_ingress_ip: str,
        host: str,
        job_id: str,
        interval_s: float = 0.5,
        max_time: float = 180,
    ):
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
        self, kube_config, kube_orchestrator, kube_ingress_ip, kube_client
    ):
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

            await self._wait_for_job_service(
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
        self, kube_config, kube_orchestrator, kube_ingress_ip, kube_client
    ):
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
            http_server=ContainerHTTPServer(port=80),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(container),
            name=f"test-job-name-{uuid4()}",
            owner="owner",
        )
        try:
            await job.start()
            assert not (job.requires_http_auth), str(job)

            for host in [job.http_host, job.http_host_named]:
                assert host
                await self._wait_for_job_service(
                    kube_ingress_ip, host=host, job_id=job.id
                )
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
        self, kube_config, kube_orchestrator, kube_ingress_ip, kube_client
    ):
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

            await self._wait_for_job_service(
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
        self, kube_config, kube_orchestrator, kube_ingress_ip, kube_client
    ):
        container = Container(
            image="python",
            command="python -m http.server 80",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
            http_server=ContainerHTTPServer(port=80, requires_auth=True),
        )
        job = MyJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(container),
            name=f"test-job-name-{uuid4()}",
            owner="owner",
        )
        try:
            await job.start()

            for http_host in [job.http_host, job.http_host_named]:
                assert http_host
                await self._wait_for_job_service(
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
    def create_server_job(self, kube_orchestrator):
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
    def create_client_job(self, kube_orchestrator):
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
        kube_config,
        create_server_job,
        create_client_job,
        kube_ingress_ip,
        delete_job_later,
    ):
        server_job = create_server_job()
        await delete_job_later(server_job)
        await server_job.start()
        server_hostname = server_job.internal_hostname
        await self._wait_for_job_service(
            kube_ingress_ip, host=server_job.http_host, job_id=server_job.id
        )

        client_job = create_client_job(server_hostname)
        await delete_job_later(client_job)
        await client_job.start()
        assert self.wait_for_success(job=client_job)

    @pytest.mark.asyncio
    async def test_job_check_http_hostname_no_job_name(
        self,
        kube_config,
        create_server_job,
        create_client_job,
        kube_ingress_ip,
        delete_job_later,
    ):
        server_job = create_server_job()
        await delete_job_later(server_job)
        await server_job.start()

        http_host = server_job.http_host
        await self._wait_for_job_service(
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
        kube_config,
        create_server_job,
        create_client_job,
        kube_ingress_ip,
        delete_job_later,
    ):
        server_job_name = f"server-job-{uuid4()}"
        server_job = create_server_job(job_name=server_job_name)
        await delete_job_later(server_job)
        await server_job.start()

        http_host = server_job.http_host
        await self._wait_for_job_service(
            kube_ingress_ip, host=http_host, job_id=server_job.id
        )
        client_job = create_client_job(http_host)
        await delete_job_later(client_job)
        await client_job.start()
        assert self.wait_for_success(job=client_job)

        http_host = server_job.http_host_named
        await self._wait_for_job_service(
            kube_ingress_ip, host=http_host, job_id=server_job.id
        )
        client_job = create_client_job(http_host)
        await delete_job_later(client_job)
        await client_job.start()
        assert self.wait_for_success(job=client_job)

    @pytest.mark.asyncio
    async def test_job_check_dns_hostname_undeclared_port(
        self, kube_config, kube_orchestrator, kube_ingress_ip, delete_job_later
    ):
        def create_server_job():
            server_cont = Container(
                image="python",
                command="python -m http.server 12345",
                resources=ContainerResources(cpu=0.1, memory_mb=128),
            )
            return MyJob(
                orchestrator=kube_orchestrator,
                job_request=JobRequest.create(server_cont),
            )

        def create_client_job(server_hostname: str):
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

        client_job = create_client_job(server_job.internal_hostname)
        await delete_job_later(client_job)
        await client_job.start()
        assert self.wait_for_success(client_job)

    @pytest.mark.asyncio
    async def test_job_pod_labels_and_network_policy(
        self, kube_config, kube_orchestrator, kube_client, delete_job_later
    ):
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


@pytest.fixture
async def delete_pod_later(kube_client):
    pods = []

    async def _add_pod(pod):
        pods.append(pod)

    yield _add_pod

    for pod in pods:
        try:
            await kube_client.delete_pod(pod.name)
        except Exception:
            pass


class TestKubeClient:
    @pytest.mark.asyncio
    async def test_wait_pod_is_running_not_found(self, kube_client):
        with pytest.raises(JobNotFoundException):
            await kube_client.wait_pod_is_running(pod_name="unknown")

    @pytest.mark.asyncio
    async def test_wait_pod_is_running_timed_out(
        self, kube_config, kube_client, delete_pod_later
    ):
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        with pytest.raises(asyncio.TimeoutError):
            await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=0.1)

    @pytest.mark.asyncio
    async def test_wait_pod_is_running(
        self, kube_config, kube_client, delete_pod_later
    ):
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)
        pod_status = await kube_client.get_pod_status(pod.name)
        assert pod_status.phase in ("Running", "Succeeded")

    @pytest.mark.asyncio
    async def test_create_log_stream_not_found(self, kube_client):
        with pytest.raises(KubeClientException):
            async with kube_client.create_pod_container_logs_stream(
                pod_name="unknown", container_name="unknown"
            ):
                pass

    @pytest.mark.asyncio
    async def test_create_log_stream_creating(
        self, kube_config, kube_client, delete_pod_later
    ):
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)

        async with timeout(5.0):
            while True:
                try:
                    stream_cm = kube_client.create_pod_container_logs_stream(
                        pod_name=pod.name, container_name=pod.name
                    )
                    with pytest.raises(KubeClientException, match="ContainerCreating"):
                        async with stream_cm:
                            pass
                    break
                except AssertionError as exc:
                    if "Pattern" not in str(exc):
                        raise
                await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_create_log_stream(self, kube_config, kube_client, delete_pod_later):
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)
        stream_cm = kube_client.create_pod_container_logs_stream(
            pod_name=pod.name, container_name=pod.name
        )
        async with stream_cm as stream:
            payload = await stream.read()
            assert payload == b""

    @pytest.mark.asyncio
    async def test_create_docker_secret_non_existent_namespace(
        self, kube_config, kube_client
    ):
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
    async def test_create_docker_secret_already_exists(self, kube_config, kube_client):
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
    async def test_update_docker_secret_already_exists(self, kube_config, kube_client):
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
    async def test_update_docker_secret_non_existent(self, kube_config, kube_client):
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
        self, kube_config, kube_client
    ):
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

    @pytest.fixture
    async def delete_network_policy_later(self, kube_client):
        names = []

        async def _add_name(name):
            names.append(name)

        yield _add_name

        for name in names:
            try:
                await kube_client.delete_network_policy(name)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_create_default_network_policy(
        self, kube_config, kube_client, delete_network_policy_later
    ):
        name = str(uuid.uuid4())
        await delete_network_policy_later(name)
        payload = await kube_client.create_default_network_policy(
            name, {"testlabel": name}, namespace_name=kube_config.namespace
        )
        assert payload["metadata"]["name"] == name

    @pytest.mark.asyncio
    async def test_create_default_network_policy_twice(
        self, kube_config, kube_client, delete_network_policy_later
    ):
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
    async def test_get_network_policy_not_found(self, kube_config, kube_client):
        name = str(uuid.uuid4())
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.get_network_policy(name)

    @pytest.mark.asyncio
    async def test_delete_network_policy_not_found(self, kube_config, kube_client):
        name = str(uuid.uuid4())
        with pytest.raises(StatusException, match="NotFound"):
            await kube_client.delete_network_policy(name)

    @pytest.mark.asyncio
    async def test_get_pod_events(self, kube_config, kube_client, delete_pod_later):
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
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
    async def test_get_pod_events_empty(self, kube_config, kube_client):
        pod_name = str(uuid.uuid4())
        events = await kube_client.get_pod_events(pod_name, kube_config.namespace)

        assert not events

    @pytest.mark.asyncio
    async def test_get_pod_container_stats(
        self, kube_config, kube_client, delete_pod_later
    ):
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)

        pod_metrics = []
        while True:
            stats = await kube_client.get_pod_container_stats(pod.name, pod.name)
            if stats:
                pod_metrics.append(stats)
            else:
                break
            await asyncio.sleep(1)

        assert pod_metrics
        assert pod_metrics[0] == PodContainerStats(cpu=mock.ANY, memory=mock.ANY)
        assert pod_metrics[0].cpu >= 0.0
        assert pod_metrics[0].memory > 0.0

    @pytest.mark.asyncio
    async def test_get_pod_container_stats_no_pod(self, kube_config, kube_client):
        pod_name = str(uuid.uuid4())
        with pytest.raises(JobNotFoundException):
            await kube_client.get_pod_container_stats(pod_name, pod_name)

    @pytest.mark.asyncio
    async def test_get_pod_container_stats_not_scheduled_yet(
        self, kube_config, kube_client, delete_pod_later
    ):
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=100, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)

        stats = await kube_client.get_pod_container_stats(pod.name, pod.name)
        assert stats is None


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
    async def test_read_instantly_succeeded(
        self, kube_config, kube_client, delete_pod_later
    ):
        container = Container(
            image="ubuntu",
            command="true",
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b""

    @pytest.mark.asyncio
    async def test_read_instantly_failed(
        self, kube_config, kube_client, delete_pod_later
    ):
        command = 'bash -c "echo -n Failure!; false"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        payload = await self._consume_log_reader(log_reader)
        assert payload == b"Failure!"

    @pytest.mark.asyncio
    async def test_read_timed_out(self, kube_config, kube_client, delete_pod_later):
        command = 'bash -c "sleep 5; echo -n Success!"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
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
    async def test_read_succeeded(self, kube_config, kube_client, delete_pod_later):
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        payload = await self._consume_log_reader(log_reader, chunk_size=1)
        expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
        assert payload == expected_payload.encode()

    @pytest.mark.asyncio
    async def test_read_cancelled(self, kube_config, kube_client, delete_pod_later):
        command = 'bash -c "for i in {1..60}; do echo $i; sleep 1; done"'
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
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
    async def test_elasticsearch_log_reader(
        self, kube_config, kube_client, delete_pod_later, es_client
    ):
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        expected_payload = ("\n".join(str(i) for i in range(1, 6)) + "\n").encode()
        container = Container(
            image="ubuntu",
            command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
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
        self, kube_client, namespace_name, pod_name, container_name, expected_payload
    ):
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod_name, container_name=container_name
        )
        payload = await self._consume_log_reader(log_reader, chunk_size=1)
        assert payload == expected_payload, "Pod logs did not match."

    async def _check_es_logs(
        self,
        es_client,
        namespace_name,
        pod_name,
        container_name,
        expected_payload,
        timeout_s=120.0,
        interval_s=1.0,
    ):
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
    async def test_elasticsearch_log_reader_empty(self, es_client):
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
        self, kube_config, kube_orchestrator, kube_client, delete_job_later
    ):
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
        self, kube_config, kube_client, delete_pod_later, resources
    ) -> bytes:
        command = "/bin/df --block-size M --output=avail /dev/shm"
        container = Container(image="ubuntu", command=command, resources=resources)
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)
        log_reader = PodContainerLogReader(
            client=kube_client, pod_name=pod.name, container_name=pod.name
        )
        return await self._consume_log_reader(log_reader, chunk_size=1)

    async def _get_non_pending_status_for_pod(
        self, kube_client, pod_name: str, interval_s: float = 0.5, max_time: float = 180
    ):
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
        self, kube_config, kube_client, delete_pod_later, resources, command
    ):
        container = Container(image="ubuntu", command=command, resources=resources)
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
        )
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
        self, kube_config, kube_client, delete_pod_later
    ):
        resources = ContainerResources(cpu=0.1, memory_mb=128)
        run_output = await self.run_command_get_logs(
            kube_config, kube_client, delete_pod_later, resources
        )
        assert b"64M" in run_output

    @pytest.mark.asyncio
    async def test_shm_extended_request_parameter_not_requested(
        self, kube_config, kube_client, delete_pod_later
    ):
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=False)
        run_output = await self.run_command_get_logs(
            kube_config, kube_client, delete_pod_later, resources
        )
        assert b"64M" in run_output

    @pytest.mark.asyncio
    async def test_shm_extended_request_parameter_requested(
        self, kube_config, kube_client, delete_pod_later
    ):
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=True)
        run_output = await self.run_command_get_logs(
            kube_config, kube_client, delete_pod_later, resources
        )
        assert b"64M" not in run_output

    @pytest.mark.asyncio
    async def test_shm_extended_not_requested_try_create_huge_file(
        self, kube_config, kube_client, delete_pod_later
    ):
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
        self, kube_config, kube_client, delete_pod_later
    ):
        command = "dd if=/dev/zero of=/dev/shm/test bs=256M  count=1"
        resources = ContainerResources(cpu=0.1, memory_mb=1024, shm=True)
        run_output = await self.run_command_get_status(
            kube_config, kube_client, delete_pod_later, resources, command
        )
        assert JobStatusItem.create(status=JobStatus.SUCCEEDED) == run_output

    @pytest.mark.asyncio
    async def test_shm_extended_not_requested_try_create_small_file(
        self, kube_config, kube_client, delete_pod_later
    ):
        command = "dd if=/dev/zero of=/dev/shm/test  bs=32M  count=1"
        resources = ContainerResources(cpu=0.1, memory_mb=128, shm=False)
        run_output = await self.run_command_get_status(
            kube_config, kube_client, delete_pod_later, resources, command
        )
        assert JobStatusItem.create(status=JobStatus.SUCCEEDED) == run_output


class TestNodeSelector:
    @pytest.mark.asyncio
    async def test_pod_node_selector(
        self, kube_config, kube_client, delete_pod_later, delete_node_later
    ):
        node_name = str(uuid.uuid4())
        await delete_node_later(node_name)

        labels = {"gpu": f"{node_name}-gpu"}
        await kube_client.create_node(node_name, labels=labels)

        pod_name = str(uuid.uuid4())
        pod = PodDescriptor(name=pod_name, image="ubuntu:latest", node_selector=labels)

        await delete_pod_later(pod)
        await kube_client.create_pod(pod)

        await kube_client.wait_pod_scheduled(pod_name, node_name)

    @pytest.mark.asyncio
    async def test_gpu(
        self,
        kube_config,
        kube_client,
        delete_job_later,
        kube_orchestrator,
        kube_node_gpu,
    ):
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
        self, kube_config, kube_client, delete_job_later, kube_orchestrator
    ):
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
        kube_config,
        kube_client,
        delete_job_later,
        kube_orchestrator,
        kube_node_preemptible,
    ):
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
        kube_config,
        kube_client,
        delete_job_later,
        kube_orchestrator,
        kube_node_preemptible,
    ):
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
        kube_config,
        kube_client,
        delete_job_later,
        kube_orchestrator,
        kube_node_preemptible,
    ):
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
