import enum
from dataclasses import dataclass, field
import shlex
from typing import Dict, Optional, List
import uuid


class JobError(Exception):
    pass


@dataclass(frozen=True)
class ContainerVolume:
    src_path: str
    dst_path: str
    read_only: bool = False


@dataclass(frozen=True)
class Container:
    image: str
    # TODO (A Danshyn 05/23/18): env is not integrated yet
    command: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)
    volumes: List[ContainerVolume] = field(default_factory=list)

    @property
    def command_list(self) -> List[str]:
        if self.command:
            return shlex.split(self.command)
        return []


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    container: Container

    @classmethod
    def create(cls, container) -> 'JobRequest':
        job_id = str(uuid.uuid4())
        return cls(job_id, container)  # type: ignore


class JobStatus(str, enum.Enum):
    PENDING = 'pending'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
