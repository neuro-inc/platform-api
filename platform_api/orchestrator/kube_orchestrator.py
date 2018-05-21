import asyncio
from asyncio import AbstractEventLoop
from typing import Optional

import aiohttp
from decouple import config as decouple_config

from .base import Orchestrator
from .job_request import JobRequest, JobStatus, JobError


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
        kube_proxy_url = decouple_config('KUBE_PROXY_URL')
        return cls(kube_proxy_url, loop)

    def __init__(self, kube_proxy_url: str, loop: AbstractEventLoop):
        self._loop = loop
        self._kube_proxy_url = kube_proxy_url

    async def job_start(self, job_request: JobRequest) -> JobStatus:
        namespaces = "default"
        data = self._create_json_pod_request(job_request)
        url = f"{self._kube_proxy_url}/api/v1/namespaces/{namespaces}/pods"
        pod = await self._request(method="POST", url=url, json=data)
        return self._get_status_from_pod(pod, job_id=job_request.job_id)

    async def job_status(self, job_id: str) -> JobStatus:
        namespaces = "default"
        url = f"{self._kube_proxy_url}/api/v1/namespaces/{namespaces}/pods/{job_id}"
        pod = await self._request(method="GET", url=url)
        return self._get_status_from_pod(pod, job_id=job_id)

    async def job_delete(self, job_id: str) -> JobStatus:
        namespaces = "default"
        url = f"{self._kube_proxy_url}/api/v1/namespaces/{namespaces}/pods/{job_id}"
        pod = await self._request(method="DELETE", url=url)
        return self._get_status_from_pod(pod, job_id=job_id)

    async def _request(self, method: str, url: str, **kwargs) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, **kwargs) as resp:
                data = await resp.json()
                return data

    def _get_status_from_pod(self, pod: dict, job_id: str):
        if pod['kind'] == 'Pod':
            return _status_pod_from_dict(pod['status'])
        elif pod['kind'] == 'Status':
            _raise_status_job_exception(pod, job_id=job_id)

    def _create_json_pod_request(self, job_request: JobRequest) -> dict:
        data = {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": f"{job_request.job_id}",
            },
            "spec": {
                "containers": [{"name": f"{job_request.container_name}",
                                "image": f"{job_request.docker_image}"}]
            }
        }
        return data
