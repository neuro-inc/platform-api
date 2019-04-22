from typing import Any

import pytest

from platform_api.orchestrator.job_request import ContainerResources
from platform_api.resource import GPUModel, ResourcePoolType


class TestResourcePoolType:
    def test_no_gpu_model(self) -> None:
        with pytest.raises(ValueError, match="GPU model unspecified"):
            ResourcePoolType(gpu=1)


class TestContainerResourcesFit:
    @pytest.mark.parametrize(
        "pool_type",
        (
            ResourcePoolType(),
            ResourcePoolType(gpu=1, gpu_model=GPUModel(id="gpumodel")),
        ),
    )
    def test_container_requires_no_gpu(self, pool_type: Any) -> None:
        resources = ContainerResources(cpu=1, memory_mb=32)
        assert resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_any_gpu_no_gpu_in_pool_type(self) -> None:
        pool_type = ResourcePoolType()
        resources = ContainerResources(cpu=1, memory_mb=32, gpu=1)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_any_gpu(self) -> None:
        pool_type = ResourcePoolType(gpu=1, gpu_model=GPUModel(id="gpumodel"))
        resources = ContainerResources(cpu=1, memory_mb=32, gpu=1)
        assert resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_too_many_gpu(self) -> None:
        pool_type = ResourcePoolType(gpu=1, gpu_model=GPUModel(id="gpumodel"))
        resources = ContainerResources(cpu=1, memory_mb=32, gpu=2)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_unknown_gpu(self) -> None:
        pool_type = ResourcePoolType(gpu=1, gpu_model=GPUModel(id="gpumodel"))
        resources = ContainerResources(
            cpu=1, memory_mb=32, gpu=1, gpu_model_id="unknown"
        )
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_specific_gpu(self) -> None:
        pool_type = ResourcePoolType(gpu=1, gpu_model=GPUModel(id="gpumodel"))
        resources = ContainerResources(
            cpu=1, memory_mb=32, gpu=1, gpu_model_id="gpumodel"
        )
        assert resources.check_fit_into_pool_type(pool_type)
