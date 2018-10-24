from dataclasses import dataclass
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

    @classmethod
    def find_model_by_id(cls, id_: str) -> Optional["GPUModel"]:
        for model in cls:
            if model.value.id == id_:
                return model.value
        return None


@dataclass(frozen=True)
class ResourcePoolType:
    """Represents an infrastructure instance/node template."""

    # TODO (A Danshyn 10/23/18): add cpu, memory, local drives etc
    gpu: Optional[int] = None
    gpu_model: Optional[GPUModel] = None

    def __post_init__(self) -> None:
        if self.gpu and not self.gpu_model:
            raise ValueError("GPU model unspecified")
