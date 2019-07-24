from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


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

    @classmethod
    def find_model_by_id(cls, id_: str) -> Optional["GPUModel"]:
        for model in cls:
            if model.value.id == id_:
                return model.value
        return None


@dataclass(frozen=True)
class Preset:
    name: str
    cpu: float
    memory_mb: int
    gpu: Optional[int] = None
    gpu_model: Optional[GPUModel] = None


@dataclass(frozen=True)
class ResourcePoolType:
    """Represents an infrastructure instance/node template."""

    is_preemptible: Optional[bool] = False
    presets: Optional[List[Preset]] = None
    cpu: Optional[float] = None
    memory_mb: Optional[int] = None
    gpu: Optional[int] = None
    gpu_model: Optional[GPUModel] = None
    disk_gb: Optional[int] = None
    min_size: Optional[int] = None
    max_size: Optional[int] = None

    def __post_init__(self) -> None:
        if self.gpu and not self.gpu_model:
            raise ValueError("GPU model unspecified")


DEFAULT_PRESETS = [
    Preset(
        name="gpu-small",
        cpu=7,
        memory_mb=30 * 1024,
        gpu=1,
        gpu_model=next(iter(GKEGPUModels)).value,
    ),
    Preset(
        name="gpu-large",
        cpu=7,
        memory_mb=60 * 1024,
        gpu=1,
        gpu_model=next(reversed(GKEGPUModels)).value,
    ),
    Preset(name="cpu-small", cpu=2, memory_mb=2 * 1024),
    Preset(name="cpu-large", cpu=3, memory_mb=14 * 1024),
]
