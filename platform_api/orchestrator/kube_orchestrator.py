import asyncio
from asyncio import AbstractEventLoop
from typing import Optional
from pprint import pprint


import aiohttp
from decouple import config as decouple_config
from kubernetes import client, config

from .base import Orchestrator
from platform_api.job_request import JobRequest, JobStatus, JobError


def _find_pending_pod_status(status: client.V1PodStatus) -> JobStatus:
    if status.container_statuses is None:
        return JobStatus.PENDING
    else:
        container_status = status.container_statuses[0]
        if container_status.state.waiting.reason == 'ContainerCreating':
            return JobStatus.PENDING
        else:
            return JobStatus.FAILED


def _find_running_pod_status(status: client.V1PodStatus) -> JobStatus:
    container_status = status.container_statuses[0]
    if container_status.ready:
        return JobStatus.SUCCEEDED
    else:
        return JobStatus.FAILED


def _pod_status_to_job_status(status: client.V1PodStatus) -> JobStatus:
    if status.phase == 'Pending':
        return _find_pending_pod_status(status)
    elif status.phase == 'Running':
        return _find_running_pod_status(status)
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

def _raise_status_job_exception(pod: dict, job_id: str):
    if pod['code'] == 409:
        raise JobError(f"job with {job_id} already exist")
    elif pod['code'] == 404:
        raise JobError(f"job with {job_id} not exist")
    elif pod['code'] == 422:
        raise JobError(f"cant create job with id {job_id}")
    else:
        raise Exception()

def _status_for_pending_pod(pod_status: dict) -> JobStatus:
    container_statuses = pod_status.get('containerStatuses')
    if container_statuses is None:
        return JobStatus.PENDING
    else:
        container_status = container_statuses[0]
        if container_status['state']['waiting']['reason'] == 'ContainerCreating':
            return JobStatus.PENDING
        else:
            return JobStatus.FAILED


def _status_for_running_pod(pod_status: dict) -> JobStatus:
    container_statuses = pod_status.get('containerStatuses')
    # TODO for now only one container for one pod
    if container_statuses is not None and container_statuses[0]['ready']:
        return JobStatus.SUCCEEDED
    else:
        return JobStatus.FAILED


def _status_pod_from_dict(pod_status: dict) -> JobStatus:
    phase = pod_status['phase']
    if phase == 'Pending':
        return _status_for_pending_pod(pod_status)
    elif phase == 'Running':
        return _status_for_running_pod(pod_status)
    else:
        return JobStatus.FAILED


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
            return _pod_status_to_job_status(created_pod.status)
        except client.rest.ApiException as ex:
            _raise_job_exception(ex, job_id=job_request.job_id)

    async def _request(self, method: str, url: str) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url) as resp:
                data = await resp.json()
                return data

    async def job_status(self, job_id: str) -> JobStatus:
        namespaces = "default"
        url = f"{self._kube_proxy_url}/api/v1/namespaces/{namespaces}/pods/{job_id}"
        # TODO too nested dict with REST API. Try use async kuber client with classes like client.V1Pod ect.
        pod = await self._request(method="GET", url=url)
        if pod['kind'] == 'Status':
            _raise_status_job_exception(pod, job_id=job_id)
        else:
            status = _status_pod_from_dict(pod['status'])
            return status

    async def job_delete(self, job_id: str) -> JobStatus:
        namespaces = "default"
        url = f"{self._kube_proxy_url}/api/v1/namespaces/{namespaces}/pods/{job_id}"
        pod = await self._request(method="DELETE", url=url)
        if pod['kind'] == 'Status':
            _raise_status_job_exception(pod, job_id=job_id)
        else:
            return JobStatus.DELETED
