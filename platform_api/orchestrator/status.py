import uuid
from dataclasses import dataclass

from .job_request import JobStatus


@dataclass
class Status:
    status_id: str
    _value: JobStatus

    @property
    def value(self) -> JobStatus:
        return self._value

    def set(self, value: JobStatus) -> None:
        self._value = value

    @classmethod
    def create(cls, value: JobStatus) -> "Status":
        status_id = str(uuid.uuid4())
        return cls(status_id, value)

    @property
    def id(self) -> str:
        return self.status_id
