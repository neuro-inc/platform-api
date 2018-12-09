from typing import Dict, Any
from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class JobTop:
    cpu: float
    mem: float

    def to_primitive(self) -> Dict[str, Any]:
        return asdict(self)


class DataDogClient:
    def get_job_top(self, job_id: str) -> JobTop:
        # TODO (truskovskiyk fetch from data dog)
        return JobTop(cpu=1, mem=16)


class JobsTelemetry:
    def __init__(self, data_dog_client: DataDogClient) -> None:
        self._data_dog_client = data_dog_client

    async def get_job_top(self, job_id: str) -> JobTop:
        job_top = self._data_dog_client.get_job_top(job_id)
        return job_top

    @classmethod
    def create(cls):
        return JobsTelemetry(DataDogClient())

