import asyncio
from asyncio import AbstractEventLoop
from typing import Optional

from kubernetes import client, config

from .base import Orchestrator
from platform_api.job_request import JobRequest, JobStatus


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

    def _pod_status_to_job_status(self, pod: client.V1Pod) -> JobStatus:
        if pod.status.phase == 'Pending':
            return JobStatus.PENDING
        elif pod.status.phase == 'Running':
            # TODO for now only one container per pod
            container_statuse = pod.status.container_statuses[0]
            if container_statuse.ready:
                return JobStatus.SUCCEEDED
            else:
                return JobStatus.FAILED
        else:
            # TODO replace custom exception
            raise ValueError('JobStatus')

    async def job_start(self, job_request: JobRequest) -> JobStatus:
        # TODO blocking. make async
        # TODO for now out job is kuber pod
        pod = client.V1Pod()
        pod.metadata = client.V1ObjectMeta(name=job_request.job_id)
        container = client.V1Container(name=job_request.container_name)
        container.image = job_request.docker_image
        container.args = job_request.args
        spec = client.V1PodSpec(containers=[container])
        pod.spec = spec
        # TODO handle namespace
        namespace = "default"
        create_pod = self.v1.create_namespaced_pod(namespace=namespace, body=pod)
        return self._pod_status_to_job_status(create_pod)

    async def job_status(self, job_id: str) -> JobStatus:
        # TODO blocking. make async
        namespace = "default"
        pod = self.v1.read_namespaced_pod(name=job_id, namespace=namespace)
        return self._pod_status_to_job_status(pod)

    async def job_delete(self, job_id: str) -> JobStatus:
        # TODO blocking. make async
        namespace = "default"
        pod_status = self.v1.delete_namespaced_pod(name=job_id, namespace=namespace, body=client.V1DeleteOptions())
        assert pod_status.reason is None
        return JobStatus.DELETED
