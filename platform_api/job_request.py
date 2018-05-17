from dataclasses import dataclass


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    container_name: str
    docker_image: str
    args: [str]


def main():
    import asyncio
    from platform_api.orchestrator.kube_orchestrator import KubeOrchestrator
    from platform_api.job import JobRequest, Job


    async def _main():
        kube_orchestrator = await KubeOrchestrator.from_env()
        print(kube_orchestrator)
        job_request = JobRequest(job_id='busybox', docker_image='truskovskyi/test', args=['python'], container_name='containername')
        job = Job(orchestrator=kube_orchestrator, job_request=job_request)


        # status = await job.start()
        # print(f"job.start() status = {status}")

        # status = await job.status()
        # print(f"job.status() status = {status}")
        # print(status)

        status = await job.delete()
        print(f"job.delete() status = {status}")
        print(status)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(_main())
