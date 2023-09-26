import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional


@dataclass(frozen=True)
class GPUModel:
    """Represent a single GPU model, e.g. NVIDIA Tesla V100.

    :param str id:
        The unique identifier of GPU models defined by an infrastructure
        provider.
    """

    # TODO (A Danshyn 10/23/18): add name, memory etc
    id: str


class GKEGPUModels(Enum):
    K80 = GPUModel(id="nvidia-tesla-k80")
    P4 = GPUModel(id="nvidia-tesla-p4")
    P100 = GPUModel(id="nvidia-tesla-p100")
    V100 = GPUModel(id="nvidia-tesla-v100")


@dataclass(frozen=True)
class TPUPreset:
    type: str
    software_version: str


@dataclass(frozen=True)
class Preset:
    name: str
    credits_per_hour: Decimal
    cpu: float
    memory: int
    scheduler_enabled: bool = False
    preemptible_node: bool = False
    gpu: Optional[int] = None
    gpu_model: Optional[str] = None
    tpu: Optional[TPUPreset] = None
    is_external_job: bool = False


@dataclass(frozen=True)
class TPUResource:
    ipv4_cidr_block: str = ""
    types: Sequence[str] = ()
    software_versions: Sequence[str] = ()


@dataclass(frozen=True)
class ResourcePoolType:
    """Represents an infrastructure instance/node template."""

    # default_factory is used only in tests
    name: str = field(default_factory=lambda: str(uuid.uuid4()))
    is_preemptible: Optional[bool] = False
    cpu: Optional[float] = None
    available_cpu: Optional[float] = None
    memory: Optional[int] = None
    available_memory: Optional[int] = None
    gpu: Optional[int] = None
    gpu_model: Optional[str] = None
    disk_gb: Optional[int] = None
    min_size: Optional[int] = None
    max_size: Optional[int] = None
    tpu: Optional[TPUResource] = None

    def __post_init__(self) -> None:
        if self.gpu and not self.gpu_model:
            raise ValueError("GPU model unspecified")
