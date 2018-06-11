from dataclasses import dataclass
import uuid

from .job_request import JobStatus


@dataclass
class Status:
    status_id: str
    _value: JobStatus

    @property
    def value(self) -> JobStatus:
        return self._value

    def set(self, value: JobStatus):
        self._value = value

    @classmethod
    def create(cls, value: JobStatus) -> 'Status':
        status_id = str(uuid.uuid4())
        return cls(status_id, value)  # type: ignore

    @property
    def id(self):
        return self.status_id
