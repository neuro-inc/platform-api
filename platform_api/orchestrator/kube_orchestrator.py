import asyncio
from asyncio import AbstractEventLoop
from typing import Optional
from kubernetes import client, config


from .base import Orchestrator
from platform_api.job import JobRequest


class KubeOrchestrator(Orchestrator):

    @classmethod
    async def from_env(cls, loop: Optional[AbstractEventLoop]=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        config_file = './platform_api_test_app/config'
        config.load_kube_config(config_file=config_file)
        v1 = client.CoreV1Api()
        return cls(v1, loop)

    def __init__(self, v1: client.CoreV1Api, loop: AbstractEventLoop):
        self.v1 = v1
        self.loop = loop

    async def start_job(self, job_request: JobRequest):
        # TODO blocking. make async
        pod = client.V1Pod()
        pod.metadata = client.V1ObjectMeta(name=job_request.job_id)
        container = client.V1Container(name=job_request.container_name)
        container.image = job_request.docker_image
        container.args = job_request.args
        spec = client.V1PodSpec(containers=[container])
        pod.spec = spec
        # TODO handle namespace
        namespace = "default"
        kuber_response = self.v1.create_namespaced_pod(namespace=namespace, body=pod)
        status = kuber_response['status']['phase']
        return status

    async def status_job(self, job_id: str):
        # TODO blocking. make async
        namespace = "default"
        v1_pod = self.v1.read_namespaced_pod(name=job_id, namespace=namespace)
        return v1_pod.status.phase

    async def delete_job(self, job_id: str):
        # TODO blocking. make async
        namespace = "default"
        res = self.v1.delete_namespaced_pod(name=job_id, namespace=namespace, body=client.V1DeleteOptions())
        return res.status

