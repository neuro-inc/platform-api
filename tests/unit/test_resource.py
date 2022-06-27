import pytest

from platform_api.orchestrator.job_request import (
    ContainerResources,
    ContainerTPUResource,
)
from platform_api.resource import ResourcePoolType, TPUResource


class TestResourcePoolType:
    def test_no_gpu_model(self) -> None:
        with pytest.raises(ValueError, match="GPU model unspecified"):
            ResourcePoolType(gpu=1)


class TestContainerResourcesFit:
    @pytest.mark.parametrize(
        "pool_type",
        (
            ResourcePoolType(available_cpu=1.0, available_memory=32),
            ResourcePoolType(
                available_cpu=1.0, available_memory=32, gpu=1, gpu_model="gpumodel"
            ),
        ),
    )
    def test_container_requires_no_gpu(self, pool_type: ResourcePoolType) -> None:
        resources = ContainerResources(cpu=1, memory=32)
        assert resources.check_fit_into_pool_type(pool_type)

    def test_container_too_much_cpu(self) -> None:
        pool_type = ResourcePoolType(available_cpu=1.0, available_memory=32)
        resources = ContainerResources(cpu=1.1, memory=32)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_no_cpu_in_pool_type(self) -> None:
        pool_type = ResourcePoolType(available_memory=32)
        resources = ContainerResources(cpu=1, memory=32)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_too_much_memory(self) -> None:
        pool_type = ResourcePoolType(available_cpu=1.0, available_memory=32)
        resources = ContainerResources(cpu=1, memory=33)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_no_memory_in_pool_type(self) -> None:
        pool_type = ResourcePoolType(available_cpu=1)
        resources = ContainerResources(cpu=1, memory=32)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_any_gpu_no_gpu_in_pool_type(self) -> None:
        pool_type = ResourcePoolType(available_cpu=1.0, available_memory=32)
        resources = ContainerResources(cpu=1, memory=32, gpu=1)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_any_gpu(self) -> None:
        pool_type = ResourcePoolType(
            available_cpu=1.0, available_memory=32, gpu=1, gpu_model="gpumodel"
        )
        resources = ContainerResources(cpu=1, memory=32, gpu=1)
        assert resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_too_many_gpu(self) -> None:
        pool_type = ResourcePoolType(
            available_cpu=1.0,
            available_memory=32 * 10**6,
            gpu=1,
            gpu_model="gpumodel",
        )
        resources = ContainerResources(cpu=1, memory=32 * 10**6, gpu=2)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_unknown_gpu(self) -> None:
        pool_type = ResourcePoolType(
            available_cpu=1.0,
            available_memory=32 * 10**6,
            gpu=1,
            gpu_model="gpumodel",
        )
        resources = ContainerResources(
            cpu=1, memory=32 * 10**6, gpu=1, gpu_model_id="unknown"
        )
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_specific_gpu(self) -> None:
        pool_type = ResourcePoolType(
            available_cpu=1.0,
            available_memory=32 * 10**6,
            gpu=1,
            gpu_model="gpumodel",
        )
        resources = ContainerResources(
            cpu=1, memory=32 * 10**6, gpu=1, gpu_model_id="gpumodel"
        )
        assert resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_tpu(self) -> None:
        pool_type = ResourcePoolType(
            available_cpu=1.0,
            available_memory=32 * 10**6,
            tpu=TPUResource(types=("v2-8",), software_versions=("1.14",)),
        )
        resources = ContainerResources(
            cpu=1,
            memory=32 * 10**6,
            tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
        )
        assert resources.check_fit_into_pool_type(pool_type)

    @pytest.mark.parametrize(
        "container_tpu_resource",
        (
            ContainerTPUResource(type="unknown", software_version="1.14"),
            ContainerTPUResource(type="v2-8", software_version="unknown"),
        ),
    )
    def test_container_requires_tpu_unknown(
        self, container_tpu_resource: ContainerTPUResource
    ) -> None:
        pool_type = ResourcePoolType(
            available_cpu=1.0,
            available_memory=32 * 10**6,
            tpu=TPUResource(types=("v2-8",), software_versions=("1.14",)),
        )
        resources = ContainerResources(
            cpu=1, memory=32 * 10**6, tpu=container_tpu_resource
        )
        assert not resources.check_fit_into_pool_type(pool_type)
