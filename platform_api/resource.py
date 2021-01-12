import dataclasses
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Sequence


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
    cpu: float
    memory_mb: int
    scheduler_enabled: bool = False
    preemptible_node: bool = False
    gpu: Optional[int] = None
    gpu_model: Optional[str] = None
    tpu: Optional[TPUPreset] = None


@dataclass(frozen=True)
class TPUResource:
    ipv4_cidr_block: str = ""
    types: Sequence[str] = ()
    software_versions: Sequence[str] = ()

    def to_primitive(self) -> Mapping[str, Any]:
        return {
            "ipv4_cidr_block": self.ipv4_cidr_block,
            "types": list(self.types),
            "software_versions": list(self.software_versions),
        }

    @classmethod
    def from_primitive(cls, data: Mapping[str, Any]) -> "TPUResource":
        return cls(**data)


@dataclass(frozen=True)
class ResourcePoolType:
    """Represents an infrastructure instance/node template."""

    # default_factory is used only in tests
    name: str = field(default_factory=lambda: str(uuid.uuid4()))
    is_preemptible: Optional[bool] = False
    cpu: Optional[float] = None
    available_cpu: Optional[float] = None
    memory_mb: Optional[int] = None
    available_memory_mb: Optional[int] = None
    gpu: Optional[int] = None
    gpu_model: Optional[str] = None
    disk_gb: Optional[int] = None
    min_size: Optional[int] = None
    max_size: Optional[int] = None
    tpu: Optional[TPUResource] = None

    def __post_init__(self) -> None:
        if self.gpu and not self.gpu_model:
            raise ValueError("GPU model unspecified")

    def to_primitive(self) -> Dict[str, Any]:
        data = dataclasses.asdict(self)
        if self.tpu:
            data["tpu"] = self.tpu.to_primitive()
        return data

    @classmethod
    def from_primitive(cls, data: Dict[str, Any]) -> "ResourcePoolType":
        if data.get("tpu") is not None:
            data["tpu"] = TPUResource.from_primitive(data["tpu"])
        return cls(**data)


DEFAULT_PRESETS = (
    Preset(
        name="gpu-small",
        cpu=7,
        memory_mb=30 * 1024,
        gpu=1,
        gpu_model=next(iter(GKEGPUModels)).value.id,
    ),
    Preset(
        name="gpu-large",
        cpu=7,
        memory_mb=60 * 1024,
        gpu=1,
        gpu_model=next(reversed(GKEGPUModels)).value.id,
    ),
    Preset(name="cpu-small", cpu=2, memory_mb=2 * 1024),
    Preset(name="cpu-large", cpu=3, memory_mb=14 * 1024),
)
