import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum


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
    nvidia_gpu: int | None = None
    amd_gpu: int | None = None
    intel_gpu: int | None = None
    nvidia_gpu_model: str | None = None
    amd_gpu_model: str | None = None
    intel_gpu_model: str | None = None
    gpu_model: str | None = None  # TODO: deprecated
    tpu: TPUPreset | None = None
    is_external_job: bool = False
    resource_pool_names: Sequence[str] = ()
    available_resource_pool_names: Sequence[str] = ()


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
    is_preemptible: bool | None = False
    cpu: float | None = None
    memory: int | None = None
    nvidia_gpu: int | None = None
    amd_gpu: int | None = None
    intel_gpu: int | None = None
    nvidia_gpu_model: str | None = None
    amd_gpu_model: str | None = None
    intel_gpu_model: str | None = None
    disk_size: int | None = None
    min_size: int | None = None
    max_size: int | None = None
    idle_size: int | None = None
    tpu: TPUResource | None = None
    cpu_min_watts: float | None = None
    cpu_max_watts: float | None = None

    @property
    def has_gpu(self) -> bool:
        return bool(self.nvidia_gpu or self.amd_gpu or self.intel_gpu)
