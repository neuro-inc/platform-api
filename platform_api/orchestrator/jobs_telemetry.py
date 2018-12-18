from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
import time
from typing import Any, Dict


@dataclass(frozen=True)
class JobTop:
    cpu: float
    mem: float
    timestamp: float = field(default_factory=time.time)

    def to_primitive(self) -> Dict[str, Any]:
        return asdict(self)


class TelemetryClient(ABC):
    @abstractmethod
    async def get_job_top(self, job_id: str) -> JobTop:
        pass


class DataDogClient(TelemetryClient):
    # TODO (truskovskiyk 17/12/18) implement it
    # https://github.com/neuromation/platform-api/issues/377
    async def get_job_top(self, job_id: str) -> JobTop:
        # TODO (truskovskiyk fetch from data dog)
        return JobTop(cpu=1, mem=16)


class JobsTelemetry:
    def __init__(self, data_dog_client: DataDogClient) -> None:
        self._data_dog_client = data_dog_client

    async def get_job_top(self, job_id: str) -> JobTop:
        job_top = await self._data_dog_client.get_job_top(job_id)
        return job_top

    @classmethod
    def create(cls):
        return JobsTelemetry(DataDogClient())
