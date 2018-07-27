import asyncio
import io
import uuid
from pathlib import PurePath

import aiohttp
import pytest
from async_timeout import timeout

from platform_api.orchestrator import (
    Job, JobError, JobRequest, JobStatus, LogReader, Orchestrator
)
from platform_api.orchestrator.job_request import (
    Container, ContainerResources, ContainerVolume
)
from platform_api.orchestrator.kube_orchestrator import (
    Ingress, IngressRule, KubeClientException, PodDescriptor, Service,
    StatusException
)
from platform_api.orchestrator.logs import PodContainerLogReader


class TestJob(Job):
    def __init__(self, orchestrator: Orchestrator, *args, **kwargs) -> None:
        self._orchestrator = orchestrator
        super().__init__(
            *args, orchestrator_config=orchestrator.config, **kwargs)

    async def start(self) -> JobStatus:
        return await self._orchestrator.start_job(self)

    async def delete(self) -> JobStatus:
        return await self._orchestrator.delete_job(self)

    async def query_status(self) -> JobStatus:
        return await self._orchestrator.status_job(job_id=self.id)


@pytest.fixture
async def job_nginx(kube_orchestrator):
    job_id = str(uuid.uuid4())
    container = Container(
        image='ubuntu', command='sleep 5',
        resources=ContainerResources(cpu=0.1, memory_mb=256))
    job_request = JobRequest(
        job_id=job_id, container=container)
    job = TestJob(orchestrator=kube_orchestrator, job_request=job_request)
    return job


class TestKubeOrchestrator:

    async def wait_for_completion(
            self, job: Job,
            interval_s: int=1, max_attempts: int=30):
        for _ in range(max_attempts):
            status = await job.query_status()
            if status.is_finished:
                return status
            else:
                await asyncio.sleep(interval_s)
        else:
            pytest.fail('too long')

    async def wait_for_failure(self, *args, **kwargs):
        status = await self.wait_for_completion(*args, **kwargs)
        assert status == JobStatus.FAILED

    async def wait_for_success(self, *args, **kwargs):
        status = await self.wait_for_completion(*args, **kwargs)
        assert status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_start_job_happy_path(self, job_nginx):
        status = await job_nginx.start()
        assert status == JobStatus.PENDING

        await self.wait_for_success(job_nginx)

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_start_job_broken_image(self, kube_orchestrator):
        job_id = str(uuid.uuid4())
        container = Container(
            image='notsuchdockerimage',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest(job_id=job_id, container=container)
        job = TestJob(orchestrator=kube_orchestrator, job_request=job_request)
        try:
            status = await job.start()
            assert status == JobStatus.PENDING

            await self.wait_for_failure(job)
        finally:
            status = await job.delete()
            assert status == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_start_job_with_not_unique_id(
            self, kube_orchestrator, job_nginx):
        status = await job_nginx.start()
        assert status == JobStatus.PENDING

        await self.wait_for_success(job_nginx)

        container = Container(
            image='python',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request_second = JobRequest(
            job_id=job_nginx.id, container=container)
        job_second = TestJob(
            orchestrator=kube_orchestrator, job_request=job_request_second)
        with pytest.raises(JobError):
            await job_second.start()

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_status_job_not_exist(self, job_nginx):
        with pytest.raises(JobError):
            await job_nginx.query_status()

    @pytest.mark.asyncio
    async def test_delete_job_not_exist(self, job_nginx):
        with pytest.raises(JobError):
            await job_nginx.delete()

    @pytest.mark.asyncio
    async def test_broken_job_id(self, kube_orchestrator):
        job_id = 'some_BROCKEN_JOB-123@#$%^&*(______------ID'
        container = Container(
            image='python',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest(job_id=job_id, container=container)
        job = TestJob(orchestrator=kube_orchestrator, job_request=job_request)

        with pytest.raises(JobError):
            await job.start()

    @pytest.mark.asyncio
    async def test_job_succeeded(self, kube_orchestrator):
        container = Container(
            image='ubuntu', command='true',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job = TestJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(container))
        try:
            status = await job.start()
            assert status == JobStatus.PENDING

            await self.wait_for_success(job, max_attempts=120)
        finally:
            await job.delete()

    @pytest.mark.asyncio
    async def test_job_failed(self, kube_orchestrator):
        container = Container(
            image='ubuntu', command='false',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job = TestJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(container))
        try:
            status = await job.start()
            assert status == JobStatus.PENDING

            await self.wait_for_failure(job, max_attempts=120)
        finally:
            await job.delete()

    @pytest.mark.asyncio
    async def test_volumes(self, kube_config, kube_orchestrator):
        await self._test_volumes(kube_config, kube_orchestrator)

    @pytest.mark.asyncio
    async def test_volumes_nfs(self, kube_config_nfs, kube_orchestrator_nfs):
        await self._test_volumes(kube_config_nfs, kube_orchestrator_nfs)

    async def _test_volumes(self, kube_config, kube_orchestrator):
        volumes = [ContainerVolume(
            src_path=PurePath(kube_config.storage_mount_path),
            dst_path=PurePath('/storage'))]
        file_path = '/storage/' + str(uuid.uuid4())

        write_container = Container(
            image='ubuntu',
            command=f"""bash -c 'echo "test" > {file_path}'""",
            volumes=volumes,
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        write_job = TestJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(write_container))

        read_container = Container(
            image='ubuntu',
            command=f"""bash -c '[ "$(cat {file_path})" == "test" ]'""",
            volumes=volumes,
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        read_job = TestJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(read_container))

        try:
            status = await write_job.start()
            assert status == JobStatus.PENDING

            await self.wait_for_success(write_job, max_attempts=120)
        finally:
            await write_job.delete()

        try:
            status = await read_job.start()
            assert status == JobStatus.PENDING

            await self.wait_for_success(read_job, max_attempts=120)
        finally:
            await read_job.delete()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'expected_result,expected_status', (
            ('6', JobStatus.SUCCEEDED),
            ('7', JobStatus.FAILED),
        )
    )
    async def test_env(
            self, kube_orchestrator, expected_result, expected_status):
        product = expected_result
        container = Container(
            image='ubuntu',
            env={'A': '2', 'B': '3'},
            command=f"""bash -c '[ "$(expr $A \* $B)" == "{product}" ]'""",
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job = TestJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(container))

        try:
            status = await job.start()
            assert status == JobStatus.PENDING

            status = await self.wait_for_completion(job, max_attempts=120)
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
        await kube_client.add_ingress_rule(
            ingress.name, IngressRule(host='host1'))
        await kube_client.add_ingress_rule(
            ingress.name, IngressRule(host='host2'))
        await kube_client.add_ingress_rule(
            ingress.name, IngressRule(host='host3'))
        result_ingress = await kube_client.get_ingress(ingress.name)
        assert result_ingress == Ingress(name=ingress.name, rules=[
            IngressRule(host=''), IngressRule(host='host1'),
            IngressRule(host='host2'), IngressRule(host='host3'),
        ])

        await kube_client.remove_ingress_rule(ingress.name, 'host2')
        result_ingress = await kube_client.get_ingress(ingress.name)
        assert result_ingress == Ingress(name=ingress.name, rules=[
            IngressRule(host=''), IngressRule(host='host1'),
            IngressRule(host='host3'),
        ])

    @pytest.mark.asyncio
    async def test_remove_ingress_rule(self, kube_client, ingress):
        with pytest.raises(StatusException, match='Not found'):
            await kube_client.remove_ingress_rule(ingress.name, 'unknown')

    @pytest.mark.asyncio
    async def test_delete_ingress_failure(self, kube_client):
        with pytest.raises(StatusException, match='Failure'):
            await kube_client.delete_ingress('unknown')

    @pytest.mark.asyncio
    async def test_service(self, kube_client):
        service_name = f'job-{uuid.uuid4()}'
        service = Service(name=service_name, target_port=8080)
        try:
            result_service = await kube_client.create_service(service)
            assert result_service.name == service_name
            assert result_service.target_port == 8080
            assert result_service.port == 80
        finally:
            await kube_client.delete_service(service_name)

    async def _wait_for_job_service(
            self, kube_ingress_ip: str, jobs_ingress_domain_name: str,
            job_id: str, interval_s: int=1, max_attempts: int=120):
        url = f'http://{kube_ingress_ip}'
        headers = {'Host': f'{job_id}.{jobs_ingress_domain_name}'}
        async with aiohttp.ClientSession() as client:
            for _ in range(max_attempts):
                try:
                    async with client.get(url, headers=headers) as response:
                        if response.status == 200:
                            break
                except (OSError, aiohttp.ClientError):
                    pass
                await asyncio.sleep(interval_s)
            else:
                pytest.fail(f'Failed to connect to job service {job_id}')

    @pytest.mark.asyncio
    async def test_job_with_exposed_port(
            self, kube_config, kube_orchestrator, kube_ingress_ip):
        container = Container(
            image='python', command='python -m http.server 80',
            resources=ContainerResources(cpu=0.1, memory_mb=128),
            port=80)
        job = TestJob(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(container))
        try:
            status = await job.start()
            assert status == JobStatus.PENDING

            await self._wait_for_job_service(
                kube_ingress_ip, kube_config.jobs_ingress_domain_name,
                job.id)
        finally:
            await job.delete()

    @pytest.fixture
    async def delete_job_later(self, kube_orchestrator):
        jobs = []

        async def _add_job(job):
            jobs.append(job)

        yield _add_job

        for job in jobs:
            try:
                await kube_orchestrator.delete_job(job)
            except Exception:
                pass

    async def _assert_no_such_ingress_rule(
            self, kube_client, ingress_name, host,
            timeout_s: int=1, interval_s: int=1):
        try:
            async with timeout(timeout_s):
                while True:
                    ingress = await kube_client.get_ingress(ingress_name)
                    rule_idx = ingress.find_rule_index_by_host(host)
                    if rule_idx == -1:
                        break
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail('Ingress still exists')


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
        with pytest.raises(JobError):
            await kube_client.wait_pod_is_running(pod_name='unknown')

    @pytest.mark.asyncio
    async def test_wait_pod_is_running_timed_out(
            self, kube_config, kube_client, delete_pod_later):
        container = Container(
            image='ubuntu', command='true',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        with pytest.raises(asyncio.TimeoutError):
            await kube_client.wait_pod_is_running(
                pod_name=pod.name, timeout_s=0.1)

    @pytest.mark.asyncio
    async def test_wait_pod_is_running(
            self, kube_config, kube_client, delete_pod_later):
        container = Container(
            image='ubuntu', command='true',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(
            pod_name=pod.name, timeout_s=60.)
        pod_status = await kube_client.get_pod_status(pod.name)
        assert pod_status.status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_create_log_stream_not_found(self, kube_client):
        with pytest.raises(KubeClientException):
            async with kube_client.create_pod_container_logs_stream(
                    pod_name='unknown', container_name='unknown'):
                pass

    @pytest.mark.asyncio
    async def test_create_log_stream_creating(
            self, kube_config, kube_client, delete_pod_later):
        container = Container(
            image='ubuntu', command='true',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        stream_cm = kube_client.create_pod_container_logs_stream(
            pod_name=pod.name, container_name=pod.name)
        with pytest.raises(KubeClientException, match='ContainerCreating'):
            async with stream_cm:
                pass

    @pytest.mark.asyncio
    async def test_create_log_stream(
            self, kube_config, kube_client, delete_pod_later):
        container = Container(
            image='ubuntu', command='true',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(
            pod_name=pod.name, timeout_s=60.)
        stream_cm = kube_client.create_pod_container_logs_stream(
            pod_name=pod.name, container_name=pod.name)
        async with stream_cm as stream:
            payload = await stream.read()
            assert payload == b''


class TestPodContainerLogReader:
    async def _consume_log_reader(
            self, log_reader: LogReader, chunk_size: int=-1) -> bytes:
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
            self, kube_config, kube_client, delete_pod_later):
        container = Container(
            image='ubuntu', command='true',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=pod.name, container_name=pod.name)
        payload = await self._consume_log_reader(log_reader)
        assert payload == b''

    @pytest.mark.asyncio
    async def test_read_instantly_failed(
            self, kube_config, kube_client, delete_pod_later):
        command = 'bash -c "echo -n Failure!; false"'
        container = Container(
            image='ubuntu', command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=pod.name, container_name=pod.name)
        payload = await self._consume_log_reader(log_reader)
        assert payload == b'Failure!'

    @pytest.mark.asyncio
    async def test_read_timed_out(
            self, kube_config, kube_client, delete_pod_later):
        command = 'bash -c "sleep 5; echo -n Success!"'
        container = Container(
            image='ubuntu', command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=pod.name, container_name=pod.name,
            client_read_timeout_s=1)
        with pytest.raises(asyncio.TimeoutError):
            await self._consume_log_reader(log_reader)

    @pytest.mark.asyncio
    async def test_read_succeeded(
            self, kube_config, kube_client, delete_pod_later):
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        container = Container(
            image='ubuntu', command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=pod.name, container_name=pod.name)
        payload = await self._consume_log_reader(log_reader, chunk_size=1)
        expected_payload = '\n'.join(str(i) for i in range(1, 6)) + '\n'
        assert payload == expected_payload.encode()

    @pytest.mark.asyncio
    async def test_read_cancelled(
            self, kube_config, kube_client, delete_pod_later):
        command = 'bash -c "for i in {1..60}; do echo $i; sleep 1; done"'
        container = Container(
            image='ubuntu', command=command,
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(pod)
        await kube_client.wait_pod_is_running(
            pod_name=pod.name, timeout_s=60.)
        log_reader = PodContainerLogReader(
            client=kube_client,
            pod_name=pod.name, container_name=pod.name)
        task = asyncio.ensure_future(
            self._consume_log_reader(log_reader, chunk_size=1))
        await asyncio.sleep(10)
        task.cancel()
        payload = await task
        expected_payload = '\n'.join(str(i) for i in range(1, 6))
        assert payload.startswith(expected_payload.encode())
