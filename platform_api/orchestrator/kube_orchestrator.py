import asyncio
from asyncio import AbstractEventLoop
from typing import Optional

from kubernetes import client, config

from .base import Orchestrator
from platform_api.job_request import JobRequest, JobStatus, JobError


def _find_pending_pod_status(pod: client.V1Pod) -> JobStatus:

    if pod.status.container_statuses is None:
        return JobStatus.PENDING
    else:
        container_status = pod.status.container_statuses[0]
        if container_status.state.waiting.reason == 'ContainerCreating':
            return JobStatus.PENDING
        else:
            return JobStatus.FAILED


def _find_running_pod_status(pod: client.V1Pod) -> JobStatus:
    container_status = pod.status.container_statuses[0]
    if container_status.ready:
        return JobStatus.SUCCEEDED
    else:
        return JobStatus.FAILED


def _pod_status_to_job_status(pod: client.V1Pod) -> JobStatus:
    if pod.status.phase == 'Pending':
        return _find_pending_pod_status(pod)
    elif pod.status.phase == 'Running':
        return _find_running_pod_status(pod)
    else:
        return JobStatus.FAILED


def _raise_job_exception(exception: client.rest.ApiException, job_id: str):
    if exception.status == 409:
        raise JobError(f"job with {job_id} already exist")
    elif exception.status == 404:
        raise JobError(f"job with {job_id} not exist")
    else:
        raise Exception()


class KubeOrchestrator(Orchestrator):

    @classmethod
    async def from_env(cls, loop: Optional[AbstractEventLoop] = None):
        if loop is None:
            loop = asyncio.get_event_loop()
        config_file = './platform_api_test_app/config'
        config.load_kube_config(config_file=config_file)
        v1 = client.CoreV1Api()
        return cls(v1, loop)

    def __init__(self, v1: client.CoreV1Api, loop: AbstractEventLoop):
        self.v1 = v1
        self.loop = loop

    async def _create_pod(self, job_request: JobRequest) -> client.V1Pod:
        # TODO blocking. make async
        pod = client.V1Pod()
        pod.metadata = client.V1ObjectMeta(name=job_request.job_id)
        container = client.V1Container(name=job_request.container_name)
        container.image = job_request.docker_image
        if job_request.args is not None:
            container.args = job_request.args
        spec = client.V1PodSpec(containers=[container])
        pod.spec = spec
        # TODO handle namespace
        namespace = "default"
        created_pod = self.v1.create_namespaced_pod(namespace=namespace, body=pod)
        return created_pod

    async def job_start(self, job_request: JobRequest) -> JobStatus:
        try:
            # TODO for now out job is k8s pod
            created_pod = await self._create_pod(job_request)
            return _pod_status_to_job_status(created_pod)
        except client.rest.ApiException as ex:
            _raise_job_exception(ex, job_id=job_request.job_id)

    async def job_status(self, job_id: str) -> JobStatus:
        try:
            # TODO blocking. make async
            namespace = "default"
            pod = self.v1.read_namespaced_pod(name=job_id, namespace=namespace)
            return _pod_status_to_job_status(pod)
        except client.rest.ApiException as ex:
            _raise_job_exception(ex, job_id=job_id)

    async def job_delete(self, job_id: str) -> JobStatus:
        try:
            # TODO blocking. make async
            namespace = "default"
            pod_status = self.v1.delete_namespaced_pod(name=job_id, namespace=namespace, body=client.V1DeleteOptions())
            assert pod_status.reason is None
            return JobStatus.DELETED
        except client.rest.ApiException as ex:
            _raise_job_exception(ex, job_id=job_id)
