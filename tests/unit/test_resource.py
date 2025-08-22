import pytest
from neuro_config_client import AMDGPU, NvidiaGPU, ResourcePoolType, TPUResource

from platform_api.orchestrator.job_request import (
    ContainerResources,
    ContainerTPUResource,
)


class TestContainerResourcesFit:
    @pytest.mark.parametrize(
        "pool_type",
        (
            ResourcePoolType(name="pool", cpu=1.0, memory=32),
            ResourcePoolType(
                name="pool",
                cpu=1.0,
                memory=32,
                nvidia_gpu=NvidiaGPU(count=1, model="nvidia-gpu"),
            ),
            ResourcePoolType(
                name="pool",
                cpu=1.0,
                memory=32,
                amd_gpu=AMDGPU(count=1, model="amd-gpu"),
            ),
        ),
    )
    def test_container_requires_no_gpu(self, pool_type: ResourcePoolType) -> None:
        resources = ContainerResources(cpu=1, memory=32)
        assert resources.check_fit_into_pool_type(pool_type)

    def test_container_too_much_cpu(self) -> None:
        pool_type = ResourcePoolType(name="pool", cpu=1.0, memory=32)
        resources = ContainerResources(cpu=1.1, memory=32)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_no_cpu_in_pool_type(self) -> None:
        pool_type = ResourcePoolType(name="pool", memory=32)
        resources = ContainerResources(cpu=1, memory=32)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_too_much_memory(self) -> None:
        pool_type = ResourcePoolType(name="pool", cpu=1.0, memory=32)
        resources = ContainerResources(cpu=1, memory=33)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_no_memory_in_pool_type(self) -> None:
        pool_type = ResourcePoolType(name="pool", cpu=1)
        resources = ContainerResources(cpu=1, memory=32)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_any_nvidia_gpu_no_nvidia_gpu_in_pool_type(self) -> None:
        pool_type = ResourcePoolType(name="pool", cpu=1.0, memory=32)
        resources = ContainerResources(cpu=1, memory=32, nvidia_gpu=1)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_too_many_nvidia_gpu(self) -> None:
        pool_type = ResourcePoolType(
            name="pool",
            cpu=1.0,
            memory=32 * 10**6,
            nvidia_gpu=NvidiaGPU(count=1, model="nvidia-gpu"),
        )
        resources = ContainerResources(cpu=1, memory=32 * 10**6, nvidia_gpu=2)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_any_amd_gpu_no_amd_gpu_in_pool_type(self) -> None:
        pool_type = ResourcePoolType(name="pool", cpu=1.0, memory=32)
        resources = ContainerResources(cpu=1, memory=32, nvidia_gpu=1)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_too_many_amd_gpu(self) -> None:
        pool_type = ResourcePoolType(
            name="pool",
            cpu=1.0,
            memory=32 * 10**6,
            amd_gpu=AMDGPU(count=1, model="amd-gpu"),
        )
        resources = ContainerResources(cpu=1, memory=32 * 10**6, amd_gpu=2)
        assert not resources.check_fit_into_pool_type(pool_type)

    def test_container_requires_tpu(self) -> None:
        pool_type = ResourcePoolType(
            name="pool",
            cpu=1.0,
            memory=32 * 10**6,
            tpu=TPUResource(
                ipv4_cidr_block="1.2.3.4/24",
                types=("v2-8",),
                software_versions=("1.14",),
            ),
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
            name="pool",
            cpu=1.0,
            memory=32 * 10**6,
            tpu=TPUResource(
                ipv4_cidr_block="1.2.3.4/24",
                types=("v2-8",),
                software_versions=("1.14",),
            ),
        )
        resources = ContainerResources(
            cpu=1, memory=32 * 10**6, tpu=container_tpu_resource
        )
        assert not resources.check_fit_into_pool_type(pool_type)
