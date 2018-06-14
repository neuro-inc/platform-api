import asyncio
from pathlib import PurePath
import uuid

import pytest

from platform_api.orchestrator.job_request import (
    Container, ContainerResources, ContainerVolume,)
from platform_api.orchestrator import (
    KubeOrchestrator, JobRequest, JobStatus, JobError, Job
)
from platform_api.orchestrator.kube_orchestrator import (
    StatusException, Service, Ingress, IngressRule,)


@pytest.fixture
async def kube_orchestrator(kube_config, event_loop):
    orchestrator = KubeOrchestrator(config=kube_config)
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def kube_orchestrator_nfs(kube_config_nfs, event_loop):
    orchestrator = KubeOrchestrator(config=kube_config_nfs)
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def job_nginx(kube_orchestrator):
    job_id = str(uuid.uuid4())
    container = Container(
        image='nginx', resources=ContainerResources(cpu=1, memory_mb=256))
    job_request = JobRequest(
        job_id=job_id, container=container)
    job = Job(orchestrator=kube_orchestrator, job_request=job_request)
    return job


class TestKubeOrchestrator:

    async def wait_for_completion(
            self, job: Job,
            interval_s: int=1, max_attempts: int=30):
        for _ in range(max_attempts):
            status = await job.status()
            if status != JobStatus.PENDING:
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
        job = Job(orchestrator=kube_orchestrator, job_request=job_request)
        try:
            status = await job.start()
            assert status == JobStatus.PENDING

            await self.wait_for_failure(job)
        finally:
            status = await job.delete()
            assert status == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_start_job_with_not_unique_id(self, kube_orchestrator, job_nginx):
        status = await job_nginx.start()
        assert status == JobStatus.PENDING

        await self.wait_for_success(job_nginx)

        container = Container(
            image='python',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request_second = JobRequest(job_id=job_nginx.id, container=container)
        job_second = Job(orchestrator=kube_orchestrator, job_request=job_request_second)
        with pytest.raises(JobError):
            await job_second.start()

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_status_job_not_exist(self, job_nginx):
        with pytest.raises(JobError):
            await job_nginx.status()

    @pytest.mark.asyncio
    async def test_delete_job_not_exist(self, job_nginx):
        with pytest.raises(JobError):
            await job_nginx.delete()

    @pytest.mark.asyncio
    async def test_broken_job_id(self, kube_orchestrator):
        job_id = "some_BROCKEN_JOB-123@#$%^&*(______------ID"
        container = Container(
            image='python',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job_request = JobRequest(job_id=job_id, container=container)
        job = Job(orchestrator=kube_orchestrator, job_request=job_request)

        with pytest.raises(JobError):
            await job.start()

    @pytest.mark.asyncio
    async def test_job_succeeded(self, kube_orchestrator):
        container = Container(
            image='ubuntu', command='true',
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        job = Job(
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
        job = Job(
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
        write_job = Job(
            orchestrator=kube_orchestrator,
            job_request=JobRequest.create(write_container))

        read_container = Container(
            image='ubuntu',
            command=f"""bash -c '[ "$(cat {file_path})" == "test" ]'""",
            volumes=volumes,
            resources=ContainerResources(cpu=0.1, memory_mb=128))
        read_job = Job(
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
        job = Job(
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
            IngressRule(), IngressRule(host='host1'),
            IngressRule(host='host2'), IngressRule(host='host3'),
        ])

        await kube_client.remove_ingress_rule(ingress.name, 'host2')
        result_ingress = await kube_client.get_ingress(ingress.name)
        assert result_ingress == Ingress(name=ingress.name, rules=[
            IngressRule(), IngressRule(host='host1'),
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
