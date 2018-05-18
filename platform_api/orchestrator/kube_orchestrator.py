import asyncio
from asyncio import AbstractEventLoop
from typing import Optional

import aiohttp
from decouple import config as decouple_config
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
    elif exception.status == 422:
        raise JobError(f"cant create job with id {job_id}")
    else:
        raise Exception()


class KubeOrchestrator(Orchestrator):

    @classmethod
    async def from_env(cls, loop: Optional[AbstractEventLoop] = None) -> 'KubeOrchestrator':
        if loop is None:
            loop = asyncio.get_event_loop()
        config.load_kube_config(config_file=decouple_config('KUBE_CONFIG_FILE'))
        v1 = client.CoreV1Api()
        kube_proxy_url = decouple_config('KUBE_PROXY_URL')
        return cls(v1, kube_proxy_url, loop)

    def __init__(self, v1: client.CoreV1Api, kube_proxy_url: str, loop: AbstractEventLoop):
        self._v1 = v1
        self._loop = loop
        self._kube_proxy_url = kube_proxy_url

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
        created_pod = self._v1.create_namespaced_pod(namespace=namespace, body=pod)
        return created_pod

    async def job_start(self, job_request: JobRequest) -> JobStatus:
        try:
            # TODO for now out job is k8s pod
            created_pod = await self._create_pod(job_request)
            return _pod_status_to_job_status(created_pod)
        except client.rest.ApiException as ex:
            _raise_job_exception(ex, job_id=job_request.job_id)

    async def job_status(self, job_id: str) -> JobStatus:
        # namespaces = "default"
        # url = f"{self._kube_proxy_url}/api/v1/namespaces/{namespaces}/pods/14fb2ac3-e7a3-4c9c-9a11-21aa88aa40ec"
        try:

            # TODO blocking. make async
            namespace = "default"
            pod = self._v1.read_namespaced_pod(name=job_id, namespace=namespace)
            return _pod_status_to_job_status(pod)
        except client.rest.ApiException as ex:
            _raise_job_exception(ex, job_id=job_id)

    async def job_delete(self, job_id: str) -> JobStatus:
        try:
            # TODO blocking. make async
            namespace = "default"
            pod_status = self._v1.delete_namespaced_pod(name=job_id, namespace=namespace, body=client.V1DeleteOptions())
            assert pod_status.reason is None
            return JobStatus.DELETED
        except client.rest.ApiException as ex:
            _raise_job_exception(ex, job_id=job_id)
