from asyncio import AbstractEventLoop
from dataclasses import dataclass
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


@dataclass(frozen=True)
class PodDescriptor:
    name: str
    image: str

    def to_primitive(self):
        return {
            'kind': 'Pod',
            'apiVersion': 'v1',
            'metadata': {
                'name': f'{self.name}',
            },
            'spec': {
                'containers': [{
                    'name': f'{self.name}',
                    'image': f'{self.image}'
                }]
            }
        }


class PodStatus:
    def __init__(self, payload):
        self._payload = payload

    @property
    def status(self) -> JobStatus:
        return _status_pod_from_dict(self._payload)

    @classmethod
    def from_primitive(cls, payload):
        if payload['kind'] == 'Pod':
            return cls(payload['status'])
        elif payload['kind'] == 'Status':
            _raise_status_job_exception(payload, job_id=None)
        else:
            raise ValueError('unknown kind')


class KubeClient:
    def __init__(
            self, *, base_url: str,
            conn_timeout_s: int=300, read_timeout_s: int=300,
            conn_pool_size: int=100) -> None:
        self._base_url = base_url
        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._conn_pool_size = conn_pool_size
        self._client: Optional[aiohttp.ClientSession] = None

    async def init(self) -> None:
        connector = aiohttp.TCPConnector(limit=self._conn_pool_size)
        self._client = aiohttp.ClientSession(
            connector=connector,
            conn_timeout=self._conn_timeout_s,
            read_timeout=self._read_timeout_s
        )

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> 'KubeClient':
        await self.init()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    async def request(self, *args, **kwargs):
        async with self._client.request(*args, **kwargs) as response:
            # TODO (A Danshyn 05/21/18): check status code etc
            return await response.json()


class KubeOrchestrator(Orchestrator):
    def __init__(
            self, *, kube_proxy_url: str, namespace: str='default',
            loop: Optional[AbstractEventLoop]=None) -> None:
        self._loop = loop
        self._kube_proxy_url = kube_proxy_url

        self._client = KubeClient(base_url=kube_proxy_url)

        # TODO (A Danshyn 05/21/18): think of the namespace life-time;
        # should we ensure it does exist before continuing
        self._namespace = namespace or 'default'

    async def __aenter__(self) -> 'KubeOrchestrator':
        await self._client.init()
        return self

    async def __aexit__(self, *args) -> None:
        if self._client:
            await self._client.close()

    @property
    def _namespace_url(self) -> str:
        return f'{self._kube_proxy_url}/api/v1/namespaces/{self._namespace}'

    @property
    def _pods_url(self) -> str:
        return f'{self._namespace_url}/pods'

    def _generate_pod_url(self, pod_id: str) -> str:
        return f'{self._pods_url}/{pod_id}'

    async def start_job(self, job_request: JobRequest) -> JobStatus:
        data = self._create_json_pod_request(job_request)
        pod = await self._request(method='POST', url=self._pods_url, json=data)
        return self._get_status_from_pod(pod, job_id=job_request.job_id)

    async def status_job(self, job_id: str) -> JobStatus:
        url = self._generate_pod_url(job_id)
        pod = await self._request(method='GET', url=url)
        return self._get_status_from_pod(pod, job_id=job_id)

    async def delete_job(self, job_id: str) -> JobStatus:
        url = self._generate_pod_url(job_id)
        pod = await self._request(method='DELETE', url=url)
        return self._get_status_from_pod(pod, job_id=job_id)

    async def _request(self, method: str, url: str, **kwargs) -> dict:
        return await self._client.request(method, url, **kwargs)

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
