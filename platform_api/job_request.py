import enum
from dataclasses import dataclass


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    container_name: str
    docker_image: str
    args: [str]


class JobStatus(str, enum.Enum):
    PENDING = 'pending'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    DELETED = 'deleted'
