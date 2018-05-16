from dataclasses import dataclass


@dataclass(frozen=True)
class JobRequest:
    job_name: str
    docker_image: str
    args: [str]


class Job:
    async def start(self):
        pass

    async def stop(self):
        pass

    async def delete(self):
        pass

    async def status(self):
        pass

    async def get_id(self):
        pass
