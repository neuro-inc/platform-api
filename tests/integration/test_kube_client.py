import asyncio
import uuid
from typing import AsyncIterator, Awaitable, Callable
from unittest import mock

import pytest
from async_timeout import timeout

from platform_api.orchestrator import (
    Job,
    JobNotFoundException,
    JobRequest,
    KubeConfig,
    KubeOrchestrator,
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
)
from platform_api.orchestrator.kube_client import (
    AlreadyExistsException,
    DockerRegistrySecret,
    Ingress,
    IngressRule,
    KubeClient,
    KubeClientException,
    PodContainerStats,
    PodDescriptor,
    Service,
    StatusException,
)
from tests.integration.test_kube_orchestrator import MyJob


class TestKubeClient(KubeClient):
    @pytest.mark.asyncio
    async def test_wait_pod_is_running_not_found(self, kube_client: KubeClient) -> None:
        with pytest.raises(JobNotFoundException):
            await kube_client.wait_pod_is_running(pod_name="unknown")

    @pytest.mark.asyncio
    async def test_wait_pod_is_running_timed_out(
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
        pod = PodDescriptor.from_job_request(
            kube_config.create_storage_volume(), job_request
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
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
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
    async def test_create_log_stream_not_found(self, kube_client: KubeClient) -> None:
        with pytest.raises(KubeClientException):
            async with kube_client.create_pod_container_logs_stream(
                pod_name="unknown", container_name="unknown"
            ):
                pass

    @pytest.mark.asyncio
    async def test_create_log_stream_creating(
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
    async def test_create_log_stream(
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
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
        wait_pod_is_terminated: Callable[..., Awaitable[None]],
    ) -> None:
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
        await wait_pod_is_terminated(pod.name)

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
    async def test_get_pod_container_stats(
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
    async def test_get_pod_container_stats_no_pod(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        pod_name = str(uuid.uuid4())
        with pytest.raises(JobNotFoundException):
            await kube_client.get_pod_container_stats(pod_name, pod_name)

    @pytest.mark.asyncio
    async def test_get_pod_container_stats_not_scheduled_yet(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
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


class MyKubeClientIngress:
    @pytest.fixture
    async def ingress(self, kube_client: KubeClient) -> AsyncIterator[Ingress]:
        ingress_name = str(uuid.uuid4())
        ingress = await kube_client.create_ingress(ingress_name)
        yield ingress
        await kube_client.delete_ingress(ingress.name)

    @pytest.mark.asyncio
    async def test_ingress(self, kube_client: KubeClient, ingress: Ingress) -> None:
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


class MyKubeClientService:
    @pytest.mark.asyncio
    async def test_service(self, kube_client: KubeClient) -> None:
        service_name = f"job-{uuid.uuid4()}"
        service = Service(name=service_name, target_port=8080)
        try:
            result_service = await kube_client.create_service(service)
            assert result_service.name == service_name
            assert result_service.target_port == 8080
            assert result_service.port == 80
        finally:
            await kube_client.delete_service(service_name)

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

    @pytest.mark.asyncio
    async def test_job_pod_labels_and_network_policy(
        self,
        kube_config: KubeConfig,
        kube_orchestrator: KubeOrchestrator,
        kube_client: KubeClient,
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
