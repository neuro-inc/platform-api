import enum
from dataclasses import dataclass
from typing import Optional, List


class JobError(Exception):
    pass


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    container_name: str
    docker_image: str
    args: Optional[List[str]] = None


class JobStatus(str, enum.Enum):
    PENDING = 'pending'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    DELETED = 'deleted'
