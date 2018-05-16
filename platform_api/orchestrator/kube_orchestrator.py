import asyncio
from asyncio import AbstractEventLoop
from typing import Optional
from kubernetes import client, config


from .orchestrator import Orchestrator
from platform_api.job import JobRequest, Job


class KubeOrchestrator(Orchestrator):

    @classmethod
    async def from_env(cls, loop: Optional[AbstractEventLoop]=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        config_file = './platform_api_test_app/config'
        config.load_kube_config(config_file=config_file)
        v1 = client.CoreV1Api()
        return cls(v1, loop)

    def __init__(self, v1, loop: AbstractEventLoop):
        self.v1 = v1
        self.loop = loop

    def _new_job(self, job_request: JobRequest) -> Job:
        pod = client.V1Pod()
        pod.metadata = client.V1ObjectMeta(name=job_request.job_name)

        container = client.V1Container(name=job_request.job_name)
        container.image = job_request.docker_image
        container.args = job_request.args

        spec = client.V1PodSpec(containers=[container])
        pod.spec = spec

        # TODO handle namespace
        namespace = "default"
        self.v1.create_namespaced_pod(namespace=namespace, body=pod)
        print("done")

    async def new_job(self, job_request: JobRequest) -> Job:
        self.loop.run_in_executor(None, self._new_job, job_request)
        pass

    async def get_job(self):
        pass
