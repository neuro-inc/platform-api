from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import PurePath
from typing import Any
from unittest import mock

import pytest
from multidict import MultiDict
from neuro_auth_client import Permission
from neuro_auth_client.client import ClientAccessSubTreeView, ClientSubTreeViewRoot
from neuro_config_client import (
    AMDGPUPreset,
    IntelGPUPreset,
    NvidiaGPUPreset,
    NvidiaMIGPreset,
    ResourcePreset,
    TPUPreset,
    TPUResource,
)
from trafaret import DataError
from yarl import URL

from platform_api.handlers.jobs_handler import (
    BulkJobFilter,
    BulkJobFilterBuilder,
    JobFilterException,
    JobFilterFactory,
    convert_job_container_to_json,
    convert_job_to_job_response,
    create_job_cluster_org_name_validator,
    create_job_preset_validator,
    create_job_request_validator,
    infer_permissions_from_container,
)
from platform_api.handlers.validators import (
    JOB_NAME_MAX_LENGTH,
    USER_NAME_MAX_LENGTH,
    create_container_request_validator,
    create_container_response_validator,
    create_job_tag_validator,
)
from platform_api.orchestrator.job import (
    Job,
    JobRecord,
    JobRestartPolicy,
    JobStatusHistory,
    JobStatusItem,
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerNvidiaMIGResource,
    ContainerResources,
    ContainerTPUResource,
    ContainerVolume,
    DiskContainerVolume,
    JobRequest,
    JobStatus,
    Secret,
    SecretContainerVolume,
)
from platform_api.orchestrator.jobs_poller import job_response_to_job_record
from platform_api.orchestrator.jobs_storage import JobFilter

from .conftest import MockOrchestrator


class TestContainerRequestValidator:
    @pytest.fixture
    def payload(self) -> dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory": 16 * 2**20},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_zero_gpu(self) -> dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "nvidia_gpu": 0, "amd_gpu": 0},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_negative_gpu(self) -> dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "nvidia_gpu": -1, "amd_gpu": -1},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_one_gpu(self) -> dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "nvidia_gpu": 1, "amd_gpu": 1},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_too_many_gpu(self) -> dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "nvidia_gpu": 130,
                "amd_gpu": 130,
                "intel_gpu": 130,
            },
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_dev_shm(self) -> dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    def test_allowed_volumes(self, payload: dict[str, Any]) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload)
        assert result["volumes"][0]["read_only"]
        assert "shm" not in result["resources"]

    def test_allowed_volumes_with_shm(
        self, payload_with_dev_shm: dict[str, Any]
    ) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload_with_dev_shm)
        assert result["volumes"][0]["read_only"]
        assert result["resources"]["shm"]

    def test_disallowed_volumes(self, payload: dict[str, Any]) -> None:
        validator = create_container_request_validator(cluster_name="test-cluster")
        with pytest.raises(ValueError, match="volumes is not allowed key"):
            validator.check(payload)

    def test_with_zero_gpu(self, payload_with_zero_gpu: dict[str, Any]) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload_with_zero_gpu)
        assert result["resources"]["nvidia_gpu"] == 0
        assert result["resources"]["amd_gpu"] == 0

    def test_with_one_gpu(self, payload_with_one_gpu: dict[str, Any]) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload_with_one_gpu)
        assert result["resources"]["nvidia_gpu"]
        assert result["resources"]["nvidia_gpu"] == 1
        assert result["resources"]["amd_gpu"]
        assert result["resources"]["amd_gpu"] == 1

    def test_with_negative_gpu(self, payload_with_negative_gpu: dict[str, Any]) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        with pytest.raises(ValueError, match="nvidia_gpu.*amd_gpu"):
            validator.check(payload_with_negative_gpu)

    def test_gpu_model(self) -> None:
        cluster = "test-cluster"
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "nvidia_gpu_model": "nvidia-gpu",
                "amd_gpu_model": "amd-gpu",
                "intel_gpu_model": "intel-gpu",
            },
        }
        validator = create_container_request_validator(cluster_name=cluster)
        result = validator.check(payload)
        assert result["resources"]["nvidia_gpu_model"] == "nvidia-gpu"
        assert result["resources"]["amd_gpu_model"] == "amd-gpu"
        assert result["resources"]["intel_gpu_model"] == "intel-gpu"

    def test_gpu_tpu_conflict(self) -> None:
        cluster = "test-cluster"
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "nvidia_gpu": 1,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
        }
        validator = create_container_request_validator(cluster_name=cluster)
        with pytest.raises(ValueError, match="tpu is not allowed key"):
            validator.check(payload)

    @pytest.mark.parametrize(
        "allowed_tpu_resources",
        (
            [],
            [
                TPUResource(
                    ipv4_cidr_block="1.2.3.4/24",
                    types=["v2-8"],
                    software_versions=["1.14"],
                )
            ],
        ),
    )
    def test_tpu_unavailable(
        self, allowed_tpu_resources: Sequence[TPUResource]
    ) -> None:
        cluster = "test-cluster"
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "tpu": {"type": "unknown", "software_version": "unknown"},
            },
        }
        validator = create_container_request_validator(
            allowed_tpu_resources=allowed_tpu_resources, cluster_name=cluster
        )
        with pytest.raises(ValueError):
            validator.check(payload)

    def test_tpu(self) -> None:
        cluster = "test-cluster"
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
        }
        validator = create_container_request_validator(
            allowed_tpu_resources=[
                TPUResource(
                    ipv4_cidr_block="1.2.3.4/24",
                    types=["v2-8"],
                    software_versions=["1.14"],
                )
            ],
            cluster_name=cluster,
        )
        result = validator.check(payload)
        assert result["resources"]["tpu"] == {
            "type": "v2-8",
            "software_version": "1.14",
        }

    def test_with_entrypoint_and_cmd(self, payload: dict[str, Any]) -> None:
        payload["entrypoint"] = "/script.sh"
        payload["command"] = "arg1 arg2 arg3"
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload)
        assert result["entrypoint"] == "/script.sh"
        assert result["command"] == "arg1 arg2 arg3"

    def test_invalid_entrypoint(self, payload: dict[str, Any]) -> None:
        payload["entrypoint"] = '"'
        validator = create_container_request_validator(cluster_name="test-cluster")
        with pytest.raises(DataError, match="invalid command format"):
            validator.check(payload)

    def test_invalid_command(self, payload: dict[str, Any]) -> None:
        payload["command"] = '"'
        validator = create_container_request_validator(cluster_name="test-cluster")
        with pytest.raises(DataError, match="invalid command format"):
            validator.check(payload)


class TestContainerResponseValidator:
    def test_gpu(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "nvidia_gpu": 1,
                "nvidia_gpu_model": "nvidia-gpu",
                "nvidia_migs": {"1g.5gb": {"count": 1, "model": "nvidia-mig"}},
                "amd_gpu": 2,
                "amd_gpu_model": "amd-gpu",
                "intel_gpu": 3,
                "intel_gpu_model": "intel-gpu",
            },
        }
        validator = create_container_response_validator()
        result = validator.check(payload)
        assert result["resources"]["nvidia_gpu"] == 1
        assert result["resources"]["nvidia_gpu_model"] == "nvidia-gpu"
        assert result["resources"]["nvidia_migs"] == {
            "1g.5gb": {"count": 1, "model": "nvidia-mig"}
        }
        assert result["resources"]["amd_gpu"] == 2
        assert result["resources"]["amd_gpu_model"] == "amd-gpu"
        assert result["resources"]["intel_gpu"] == 3
        assert result["resources"]["intel_gpu_model"] == "intel-gpu"

    def test_tpu(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
        }
        validator = create_container_response_validator()
        result = validator.check(payload)
        assert result["resources"]["tpu"] == {
            "type": "v2-8",
            "software_version": "1.14",
        }

    def test_invalid_command(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "command": '"',
            "tty": False,
        }
        validator = create_container_response_validator()
        result = validator.check(payload)
        assert result["command"] == '"'


class TestJobClusterNameValidator:
    def test_without_cluster_name(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
        }
        validator = create_job_cluster_org_name_validator(
            default_cluster_name="default",
            default_org_name="default-org",
            default_project_name="default-project",
        )
        payload = validator.check(request)
        assert payload["cluster_name"] == "default"
        assert payload["org_name"] == "default-org"
        assert payload["project_name"] == "default-project"

    def test_without_org_name(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
        }
        validator = create_job_cluster_org_name_validator(
            default_cluster_name="default",
            default_org_name="some-org",
            default_project_name="default-project",
        )
        payload = validator.check(request)
        assert payload["cluster_name"] == "default"
        assert payload["org_name"] == "some-org"
        assert payload["project_name"] == "default-project"

    def test_with_cluster_name(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "cluster_name": "testcluster",
            "container": container,
        }
        validator = create_job_cluster_org_name_validator(
            default_cluster_name="default",
            default_org_name="default-org",
            default_project_name="default-project",
        )
        payload = validator.check(request)
        assert payload["cluster_name"] == "testcluster"
        assert payload["org_name"] == "default-org"

    def test_with_org_name(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "cluster_name": "testcluster",
            "org_name": "testorg",
            "container": container,
        }
        validator = create_job_cluster_org_name_validator(
            default_cluster_name="default",
            default_org_name="test-org",
            default_project_name="default-project",
        )
        payload = validator.check(request)
        assert payload["cluster_name"] == "testcluster"
        assert payload["org_name"] == "testorg"

    def test_invalid_payload_type(self) -> None:
        validator = create_job_cluster_org_name_validator(
            default_cluster_name="default",
            default_org_name="test-org",
            default_project_name="default-project",
        )
        with pytest.raises(DataError):
            validator.check([])

    def test_invalid_cluster_name_type(self) -> None:
        request = {
            "cluster_name": 123,
        }
        validator = create_job_cluster_org_name_validator(
            default_cluster_name="default",
            default_org_name="test-org",
            default_project_name="default-project",
        )
        with pytest.raises(DataError, match="value is not a string"):
            validator.check(request)

    def test_invalid_org_name_type(self) -> None:
        request = {
            "org_name": 123,
        }
        validator = create_job_cluster_org_name_validator(
            default_cluster_name="default",
            default_org_name="test-org",
            default_project_name="default-project",
        )
        with pytest.raises(DataError, match="value is not a string"):
            validator.check(request)

    def test_with_project_name(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "cluster_name": "testcluster",
            "org_name": "testorg",
            "project_name": "testproject",
            "container": container,
        }
        validator = create_job_cluster_org_name_validator(
            default_cluster_name="default",
            default_org_name="test-org",
            default_project_name="default-project",
        )
        payload = validator.check(request)
        assert payload["cluster_name"] == "testcluster"
        assert payload["org_name"] == "testorg"
        assert payload["project_name"] == "testproject"

    def test_invalid_project_name_type(self) -> None:
        request = {
            "cluster_name": "testcluster",
            "org_name": "testorg",
            "project_name": 123,
        }
        validator = create_job_cluster_org_name_validator(
            default_cluster_name="default",
            default_org_name="test-org",
            default_project_name="default-project",
        )
        with pytest.raises(DataError, match="value is not a string"):
            validator.check(request)


class TestJobPresetValidator:
    def test_validator(self) -> None:
        request = {"preset_name": "preset", "container": {}}
        validator = create_job_preset_validator(
            [
                ResourcePreset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory=100 * 10**6,
                )
            ]
        )
        payload = validator.check(request)

        assert payload == {
            "preset_name": "preset",
            "container": {
                "resources": {"cpu": 0.1, "memory": 100 * 10**6, "shm": False}
            },
            "scheduler_enabled": False,
            "preemptible_node": False,
        }

    def test_validator_no_presets(self) -> None:
        request = {"preset_name": "preset", "container": {}}
        validator = create_job_preset_validator([])

        with pytest.raises(
            DataError, match="At least one preset is required to run a job"
        ):
            validator.check(request)

    def test_validator_default_preset(self) -> None:
        request: dict[str, Any] = {"container": {}}
        validator = create_job_preset_validator(
            [
                ResourcePreset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory=100 * 10**6,
                )
            ]
        )
        payload = validator.check(request)

        assert payload == {
            "preset_name": "preset",
            "container": {
                "resources": {"cpu": 0.1, "memory": 100 * 10**6, "shm": False}
            },
            "scheduler_enabled": False,
            "preemptible_node": False,
        }

    def test_flat_structure_validator(self) -> None:
        request = {"preset_name": "preset"}
        validator = create_job_preset_validator(
            [
                ResourcePreset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory=100 * 10**6,
                )
            ]
        )
        payload = validator.check(request)

        assert payload == {
            "preset_name": "preset",
            "resources": {"cpu": 0.1, "memory": 100 * 10**6, "shm": False},
            "scheduler_enabled": False,
            "preemptible_node": False,
        }

    def test_validator_unknown_preset_name(self) -> None:
        request = {"preset_name": "unknown", "container": {}}
        validator = create_job_preset_validator(
            [
                ResourcePreset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory=100 * 10**6,
                )
            ]
        )
        with pytest.raises(DataError, match="Preset was not found"):
            validator.check(request)

    def test_validator_preset_name_and_resources(self) -> None:
        request = {
            "preset_name": "preset",
            "container": {"resources": {"cpu": 1.0, "memory": 1024 * 10**6}},
        }
        validator = create_job_preset_validator(
            [
                ResourcePreset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory=100 * 10**6,
                )
            ]
        )
        with pytest.raises(
            DataError, match="Both preset and resources are not allowed"
        ):
            validator.check(request)

    def test_validator_flat_structure_preset_name_and_resources(self) -> None:
        request = {
            "preset_name": "preset",
            "resources": {"cpu": 1.0, "memory": 1024 * 10**6},
        }
        validator = create_job_preset_validator(
            [
                ResourcePreset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory=100 * 10**6,
                )
            ]
        )
        with pytest.raises(
            DataError, match="Both preset and resources are not allowed"
        ):
            validator.check(request)

    def test_validator_with_shm_gpu_tpu_preemptible(self) -> None:
        request = {
            "preset_name": "preset",
            "container": {"resources": {"shm": True}},
        }
        validator = create_job_preset_validator(
            [
                ResourcePreset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory=100 * 10**6,
                    nvidia_gpu=NvidiaGPUPreset(count=1, model="nvidia-gpu"),
                    nvidia_migs=[
                        NvidiaMIGPreset(
                            profile_name="1g.5gb", count=1, model="nvidia-mig"
                        )
                    ],
                    amd_gpu=AMDGPUPreset(count=2, model="amd-gpu"),
                    intel_gpu=IntelGPUPreset(count=3, model="intel-gpu"),
                    tpu=TPUPreset(type="v2-8", software_version="1.14"),
                    scheduler_enabled=True,
                    preemptible_node=True,
                )
            ]
        )
        payload = validator.check(request)

        assert payload == {
            "preset_name": "preset",
            "container": {
                "resources": {
                    "cpu": 0.1,
                    "memory": 100 * 10**6,
                    "shm": True,
                    "nvidia_gpu": 1,
                    "nvidia_gpu_model": "nvidia-gpu",
                    "nvidia_migs": {"1g.5gb": {"count": 1, "model": "nvidia-mig"}},
                    "amd_gpu": 2,
                    "amd_gpu_model": "amd-gpu",
                    "intel_gpu": 3,
                    "intel_gpu_model": "intel-gpu",
                    "tpu": {
                        "type": "v2-8",
                        "software_version": "1.14",
                    },
                }
            },
            "scheduler_enabled": True,
            "preemptible_node": True,
        }


class TestJobRequestValidator:
    def test_validator(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="testcluster",
            org_name="test-org",
        )
        payload = validator.check(request)
        assert payload["cluster_name"] == "testcluster"
        assert payload["restart_policy"] == JobRestartPolicy.NEVER

    def test_schedule_timeout_disallowed_for_scheduled_job(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
            "schedule_timeout": 90,
            "scheduler_enabled": True,
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="testcluster",
            org_name="test-org",
        )
        with pytest.raises(
            DataError, match="schedule_timeout is not allowed for scheduled jobs"
        ):
            validator.check(request)

    def test_flat_payload_validator(self) -> None:
        request = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        validator = create_job_request_validator(
            allow_flat_structure=True,
            allowed_tpu_resources=(),
            cluster_name="testcluster",
            org_name="test-org",
        )
        payload = validator.check(request)
        assert payload["cluster_name"] == "testcluster"
        assert payload["restart_policy"] == JobRestartPolicy.NEVER

    def test_validator_explicit_cluster_name(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
            "cluster_name": "testcluster",
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="testcluster",
            org_name="test-org",
        )
        payload = validator.check(request)
        assert payload["cluster_name"] == "testcluster"

    def test_validator_invalid_cluster_name(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
            "cluster_name": "testcluster",
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="another",
            org_name="test-org",
        )
        with pytest.raises(DataError, match="value is not exactly 'another'"):
            validator.check(request)

    @pytest.mark.parametrize("limit_minutes", [1, 10])
    def test_with_max_run_time_minutes_valid(self, limit_minutes: int) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
            "max_run_time_minutes": limit_minutes,
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="test-cluster",
            org_name="test-org",
        )
        validator.check(request)

    @pytest.mark.parametrize("limit_minutes", [0, -1])
    def test_with_max_run_time_minutes_invalid(self, limit_minutes: int) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
            "max_run_time_minutes": limit_minutes,
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="test-cluster",
            org_name="test-org",
        )
        with pytest.raises(DataError, match="value is less than"):
            validator.check(request)

    @pytest.mark.parametrize(
        "tag",
        [
            "a",
            "a" * 256,
            "foo123",
            "foo:bar123",
            "foo:bar-baz123",
            "pre/foo:bar-baz123",
            "pre.org/foo:bar-baz123",
        ],
    )
    def test_job_tags_validator_valid(self, tag: str) -> None:
        validator = create_job_tag_validator()
        assert validator.check(tag) == tag

    @pytest.mark.parametrize(
        "tag",
        [
            "",
            "a" * 257,  # Too long
            "with space",
            "with\nnewline",
            "with\ttab",
        ],
    )
    def test_job_tags_validator_invalid(self, tag: str) -> None:
        validator = create_job_tag_validator()
        with pytest.raises(DataError):
            validator.check(tag)

    def test_restart_policy(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
            "cluster_name": "testcluster",
            "restart_policy": "unknown",
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="testcluster",
            org_name="test-org",
        )
        with pytest.raises(DataError, match="restart_policy.+any variant"):
            validator.check(request)

    def test_job_with_secret_env(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "secret_env": {
                "ENV_SECRET1": "secret://clustername/username/key1",
                "ENV_SECRET2": "secret://clustername/username/key2",
            },
        }
        request = {
            "container": container,
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="clustername",
            org_name="test-org",
        )
        validator.check(request)

    def test_job_with_secret_volumes(self) -> None:
        container = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "secret_volumes": [
                {
                    "src_secret_uri": "secret://clustername/username/key1",
                    "dst_path": "/container/path1",
                },
                {
                    "src_secret_uri": "secret://clustername/username/key2",
                    "dst_path": "/container/path2",
                },
            ],
        }
        request = {"container": container}
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="clustername",
            org_name="test-org",
        )
        validator.check(request)

    def test_energy_schedule_name(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
            "scheduler_enabled": True,
            "energy_schedule_name": "default",
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="testcluster",
            org_name="test-org",
            allowed_energy_schedule_names=["default"],
        )
        validator.check(request)

    def test_energy_schedule_name__unknown(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
            "scheduler_enabled": True,
            "energy_schedule_name": "default",
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="testcluster",
            org_name="test-org",
            allowed_energy_schedule_names=[],
        )
        with pytest.raises(DataError, match="value doesn't match any"):
            validator.check(request)

    def test_energy_schedule_name_allowed_only_when_scheduler_enabled(self) -> None:
        container = {
            "image": "testimage",
            "command": "arg1 arg2 arg3",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
        }
        request = {
            "container": container,
            "energy_schedule_name": "default",
        }
        validator = create_job_request_validator(
            allowed_tpu_resources=(),
            cluster_name="testcluster",
            org_name="test-org",
            allowed_energy_schedule_names=["default"],
        )
        with pytest.raises(DataError, match="energy_schedule_name.+scheduler.+enabled"):
            validator.check(request)


class TestJobContainerToJson:
    def test_minimal(self) -> None:
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory=16 * 10**6)
        )
        assert convert_job_container_to_json(container) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory": 16000000, "memory_mb": 15},
            "volumes": [],
        }

    def test_tpu_resource(self) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(
                cpu=0.1,
                memory=16 * 10**6,
                tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
            ),
        )
        assert convert_job_container_to_json(container) == {
            "env": {},
            "image": "image",
            "resources": {
                "cpu": 0.1,
                "memory": 16 * 10**6,
                "memory_mb": 15,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
            "volumes": [],
        }

    def test_gpu_and_shm_resources(self) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(
                cpu=0.1,
                memory=16 * 10**6,
                nvidia_gpu=1,
                nvidia_gpu_model="nvidia-gpu",
                nvidia_migs={
                    "1g.5gb": ContainerNvidiaMIGResource(count=1, model="nvidia-mig")
                },
                amd_gpu=2,
                amd_gpu_model="amd-gpu",
                intel_gpu=3,
                intel_gpu_model="intel-gpu",
                shm=True,
            ),
        )
        assert convert_job_container_to_json(container) == {
            "env": {},
            "image": "image",
            "resources": {
                "cpu": 0.1,
                "memory": 16 * 10**6,
                "memory_mb": 15,
                "nvidia_gpu": 1,
                "nvidia_gpu_model": "nvidia-gpu",
                "nvidia_migs": {"1g.5gb": {"count": 1, "model": "nvidia-mig"}},
                "amd_gpu": 2,
                "amd_gpu_model": "amd-gpu",
                "intel_gpu": 3,
                "intel_gpu_model": "intel-gpu",
                "shm": True,
            },
            "volumes": [],
        }

    def test_with_working_dir(self) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory=16 * 10**6),
            working_dir="/working/dir",
        )
        assert convert_job_container_to_json(container) == {
            "env": {},
            "image": "image",
            "resources": {
                "cpu": 0.1,
                "memory": 16 * 10**6,
                "memory_mb": 15,
            },
            "volumes": [],
            "working_dir": "/working/dir",
        }


class TestJobFilterFactory:
    def test_create_from_query(self) -> None:
        factory = JobFilterFactory().create_from_query

        query: Any = MultiDict()
        assert factory(query) == JobFilter()

        query = MultiDict([("name", "test-job")])
        assert factory(query) == JobFilter(name="test-job")

        query = MultiDict([("name", "test-job"), ("name", "other-job")])
        assert factory(query) == JobFilter(name="test-job")

        query = MultiDict([("owner", "alice"), ("owner", "bob/test/foo")])
        assert factory(query) == JobFilter(owners={"bob/test/foo", "alice"})

        query = MultiDict([("base_owner", "alice"), ("base_owner", "bob")])
        assert factory(query) == JobFilter(base_owners={"bob", "alice"})

        query = MultiDict([("name", "test-job"), ("owner", "alice"), ("owner", "bob")])
        assert factory(query) == JobFilter(owners={"bob", "alice"}, name="test-job")

        query = MultiDict([("materialized", "True")])
        assert factory(query) == JobFilter(materialized=True)

        query = MultiDict([("materialized", "False")])
        assert factory(query) == JobFilter(materialized=False)

        query = MultiDict([("being_dropped", "True")])
        assert factory(query) == JobFilter(being_dropped=True)

        query = MultiDict([("being_dropped", "False")])
        assert factory(query) == JobFilter(being_dropped=False)

        query = MultiDict([("logs_removed", "True")])
        assert factory(query) == JobFilter(logs_removed=True)

        query = MultiDict([("logs_removed", "False")])
        assert factory(query) == JobFilter(logs_removed=False)

        query = MultiDict([("org_name", "org1"), ("org_name", "org2")])
        assert factory(query) == JobFilter(orgs={"org1", "org2"})

    def test_create_from_query_with_status(self) -> None:
        factory = JobFilterFactory().create_from_query

        query: Any = MultiDict(
            [
                ("name", "test-job"),
                ("status", "running"),
                ("status", "pending"),
                ("status", "failed"),
                ("status", "succeeded"),
            ]
        )
        assert factory(query) == JobFilter(
            statuses={
                JobStatus.FAILED,
                JobStatus.PENDING,
                JobStatus.SUCCEEDED,
                JobStatus.RUNNING,
            },
            name="test-job",
        )

        query = MultiDict(
            [
                ("name", "test-job"),
                ("owner", "alice"),
                ("owner", "bob"),
                ("status", "failed"),
                ("status", "succeeded"),
            ]
        )
        assert factory(query) == JobFilter(
            statuses={JobStatus.FAILED, JobStatus.SUCCEEDED},
            owners={"bob", "alice"},
            name="test-job",
        )

    def test_create_from_query_by_hostname(self) -> None:
        factory = JobFilterFactory().create_from_query

        query: Any = MultiDict([("hostname", "test-job--0123abcdef.example.org")])
        assert factory(query) == JobFilter(
            name="test-job", org_project_hash="0123abcdef"
        )

        query = MultiDict([("hostname", "test-job-id.example.org")])
        assert factory(query) == JobFilter(ids={"test-job-id"})

        query = MultiDict(
            [
                ("hostname", "test-job--0123abcdef.example.org"),
                ("hostname", "test-job-id.example.org"),
            ]
        )
        assert factory(query) == JobFilter(
            name="test-job", org_project_hash="0123abcdef"
        )

        query = MultiDict(
            [
                ("hostname", "test-job-id.example.org"),
                ("hostname", "test-job--0123abcdef.example.org"),
            ]
        )
        assert factory(query) == JobFilter(name=None, ids={"test-job-id"})

    def test_create_from_query_by_hostname_with_status(self) -> None:
        factory = JobFilterFactory().create_from_query

        query: Any = MultiDict(
            [
                ("hostname", "test-job--0123abcdef.example.org"),
                ("status", "failed"),
                ("status", "succeeded"),
            ]
        )
        assert factory(query) == JobFilter(
            statuses={JobStatus.FAILED, JobStatus.SUCCEEDED},
            name="test-job",
            org_project_hash="0123abcdef",
        )

        query = MultiDict(
            [
                ("hostname", "test-job-id.example.org"),
                ("status", "failed"),
                ("status", "succeeded"),
            ]
        )
        assert factory(query) == JobFilter(
            statuses={JobStatus.FAILED, JobStatus.SUCCEEDED}, ids={"test-job-id"}
        )

    @pytest.mark.parametrize(
        "query",
        [
            [("name", "jt")],
            [("name", "test_job")],
            [("name", "test.job")],
            [("name", "0testjob")],
            [("owner", "jd")],
            [("owner", "john.doe")],
            [("owner", "john_doe")],
            [("owner", "john--doe")],
            [("status", "unknown")],
            [("hostname", "testjob--.example.org")],
            [("hostname", "--johndoe.example.org")],
            [("hostname", "testjob--john_doe.example.org")],
            [("hostname", "test_job--johndoe.example.org")],
            [("hostname", "xn--johndoe.example.org")],
            [("hostname", "testjob--johndoe.example.org"), ("name", "testjob")],
            [("hostname", "testjob--johndoe.example.org"), ("project_name", "johndoe")],
            [("hostname", "TESTJOB--johndoe.example.org")],
            [("hostname", "testjob--JOHNDOE.example.org")],
            [("org_name", "invalid_org")],
        ],
    )
    def test_create_from_query_fail(self, query: Any) -> None:
        factory = JobFilterFactory().create_from_query
        with pytest.raises((ValueError, DataError)):
            factory(MultiDict(query))  # type: ignore


def make_access_tree(perm_dict: dict[str, str]) -> ClientSubTreeViewRoot:
    tree = ClientSubTreeViewRoot(
        scheme="job",
        path="/",
        sub_tree=ClientAccessSubTreeView(
            action="list" if perm_dict else "deny", children={}
        ),
    )
    for path, action in perm_dict.items():
        node = tree.sub_tree
        if path:
            for name in path.split("/"):
                if name not in node.children:
                    node.children[name] = ClientAccessSubTreeView(
                        action="list", children={}
                    )
                node = node.children[name]
        node.action = action
    return tree


class TestBulkJobFilterBuilder:
    def test_no_access(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree({})
        with pytest.raises(JobFilterException, match="no jobs"):
            BulkJobFilterBuilder(query_filter, tree).build()

    def test_no_access_with_owners_and_orgs(self) -> None:
        query_filter = JobFilter(owners={"someuser"}, orgs={"someuser"})
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/job-test-2": "deny",
                "test-cluster/someuser": "deny",
            }
        )
        with pytest.raises(JobFilterException, match="no jobs"):
            BulkJobFilterBuilder(query_filter, tree).build()

    def test_no_access_with_clusters(self) -> None:
        query_filter = JobFilter(clusters={"somecluster": {}})
        tree = make_access_tree({"test-cluster/testuser": "read"})
        with pytest.raises(JobFilterException, match="no jobs"):
            BulkJobFilterBuilder(query_filter, tree).build()

    def test_full_access_no_owners(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree({"": "manage"})
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(), shared_ids=set(), shared_ids_filter=None
        )

    def test_full_access_with_owners(self) -> None:
        query_filter = JobFilter(owners={"testuser", "someuser"})
        tree = make_access_tree({"": "manage"})
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(owners={"testuser", "someuser"}),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_full_access_with_clusters(self) -> None:
        query_filter = JobFilter(clusters={"test-cluster": {}, "somecluster": {}})
        tree = make_access_tree({"": "manage"})
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(clusters={"test-cluster": {}, "somecluster": {}}),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_cluster_access_no_owners(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree({"test-cluster": "read", "anothercluster": "read"})
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(clusters={"test-cluster": {}, "anothercluster": {}}),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_cluster_access_with_owners(self) -> None:
        query_filter = JobFilter(owners={"testuser", "someuser"})
        tree = make_access_tree({"test-cluster": "read", "anothercluster": "read"})
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}, "anothercluster": {}},
                owners={"testuser", "someuser"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_cluster_access_with_clusters(self) -> None:
        query_filter = JobFilter(clusters={"test-cluster": {}, "somecluster": {}})
        tree = make_access_tree({"test-cluster": "read", "anothercluster": "read"})
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(clusters={"test-cluster": {}}),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_same_project(self) -> None:
        query_filter = JobFilter(orgs={"test-org"})
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "anothercluster/test-org/testuser": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}, "anothercluster": {}},
                projects={"testuser"},
                orgs={"test-org"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_same_cluster(self) -> None:
        query_filter = JobFilter(orgs={"test-org"})
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}},
                projects={"testuser", "anotheruser"},
                orgs={"test-org"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_different_projects_and_clusters(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree(
            {"test-cluster/testuser": "read", "anothercluster/anotheruser": "read"}
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={
                    "test-cluster": {"testuser": {}},
                    "anothercluster": {"anotheruser": {}},
                },
                orgs={"testuser", "anotheruser"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_mixed_projects_and_clusters(self) -> None:
        query_filter = JobFilter(orgs={"test-org"})
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser": "read",
                "anothercluster/test-org/testuser": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={
                    "test-cluster": {},
                    "anothercluster": {"test-org": {"testuser": set()}},
                },
                projects={"testuser", "anotheruser"},
                orgs={"test-org"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_mixed_projects_and_clusters2(self) -> None:
        query_filter = JobFilter(orgs={"test-org"})
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser": "read",
                "anothercluster/test-org/testuser": "read",
                "anothercluster/test-org/thirduser": "read",
                "thirdcluster/test-org/testuser": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={
                    "test-cluster": {
                        "test-org": {"testuser": set(), "anotheruser": set()}
                    },
                    "anothercluster": {
                        "test-org": {"testuser": set(), "thirduser": set()}
                    },
                    "thirdcluster": {"test-org": {"testuser": set()}},
                },
                projects={"testuser", "anotheruser", "thirduser"},
                orgs={"test-org"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_mixed_jobs_and_clusters(self) -> None:
        query_filter = JobFilter(orgs={"test-org"})
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser/foo": "read",
                "test-cluster/test-org/anotheruser/foo": "read",
                "anothercluster": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={
                    "test-cluster": {
                        "test-org": {
                            "testuser": {"foo"},
                            "anotheruser": {"foo"},
                        },
                    },
                    "anothercluster": {},
                },
                orgs={"test-org"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_cluster_project_access(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser": "read",
                "anothercluster": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={
                    "test-cluster": {
                        "testuser": {},
                        "anotheruser": {},
                    },
                    "anothercluster": {},
                },
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_access_no_projects(self) -> None:
        query_filter = JobFilter(orgs={"test-org"})
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser/job-test-1": "read",
                "test-cluster/test-org/anotheruser/job-test-2": "deny",
                "test-cluster/test-org/someuser": "deny",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}},
                projects={"testuser"},
                orgs={"test-org"},
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(orgs={"test-org"}),
        )

    def test_mixed_access_projects_shared_all(self) -> None:
        query_filter = JobFilter(projects={"testuser"}, orgs={"test-org"})
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser/job-test-1": "read",
                "test-cluster/test-org/anotheruser/job-test-2": "deny",
                "test-cluster/test-org/someuser": "deny",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}}, projects={"testuser"}, orgs={"test-org"}
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_access_shared_ids_only(self) -> None:
        query_filter = JobFilter(projects={"anotheruser"}, orgs={"test-org"})
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser/job-test-1": "read",
                "test-cluster/test-org/anotheruser/job-test-2": "deny",
                "test-cluster/test-org/someuser": "deny",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=None,
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(projects={"anotheruser"}, orgs={"test-org"}),
        )

    def test_mixed_access_projects_shared_all_and_specific(self) -> None:
        query_filter = JobFilter(
            projects={"testuser", "anotheruser"},
            statuses={JobStatus.PENDING},
            name="testname",
            orgs={"test-org"},
        )
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser/job-test-1": "read",
                "test-cluster/test-org/anotheruser/job-test-2": "deny",
                "test-cluster/test-org/someuser": "deny",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}},
                projects={"testuser"},
                statuses={JobStatus.PENDING},
                name="testname",
                orgs={"test-org"},
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(
                projects={"testuser", "anotheruser"},
                statuses={JobStatus.PENDING},
                name="testname",
                orgs={"test-org"},
            ),
        )

    def test_shared_by_name(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/testname": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={
                    "test-cluster": {
                        "testuser": {},
                        "anotheruser": {"job-test-1": set(), "testname": set()},
                    }
                },
                orgs={"testuser", "anotheruser"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_shared_by_name_with_name(self) -> None:
        query_filter = JobFilter(name="testname", orgs={"test-org"})
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser/job-test-1": "read",
                "test-cluster/test-org/anotheruser/testname": "read",
                "test-cluster/test-org/anotheruser/testname2": "read",
                "test-cluster/test-org/thirduser/testname": "read",
                "test-cluster/test-org/forthduser/testname2": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}},
                projects={
                    "testuser",
                    "anotheruser",
                    "thirduser",
                },
                name="testname",
                orgs={"test-org"},
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(name="testname", orgs={"test-org"}),
        )

    def test_shared_by_name_with_projects(self) -> None:
        query_filter = JobFilter(
            projects={"anotheruser", "someuser"}, orgs={"test-org"}
        )
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser/job-test-1": "read",
                "test-cluster/test-org/anotheruser/testname": "read",
                "test-cluster/test-org/thirduser/testname": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {"test-org": {"anotheruser": {"testname"}}}},
                projects={"anotheruser"},
                orgs={"test-org"},
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(
                projects={"anotheruser", "someuser"}, orgs={"test-org"}
            ),
        )

    def test_shared_by_name_with_project_and_name(self) -> None:
        query_filter = JobFilter(projects={"anotheruser"}, name="testname")
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/testname": "read",
                "test-cluster/anotheruser/testname2": "read",
                "test-cluster/thirduser/testname": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}},
                orgs={"testuser"},
                projects={"anotheruser"},
                name="testname",
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_shared_by_name_with_project_and_name_2(self) -> None:
        query_filter = JobFilter(
            projects={"anotheruser"}, name="testname", orgs={"test-org"}
        )
        tree = make_access_tree(
            {
                "test-cluster/test-org/testuser": "read",
                "test-cluster/test-org/anotheruser/job-test-1": "read",
                "test-cluster/test-org/anotheruser/testname2": "read",
                "test-cluster/test-org/thirduser/testname": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=None,
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(
                projects={"anotheruser"}, name="testname", orgs={"test-org"}
            ),
        )


class TestInferPermissionsFromContainer:
    def test_no_volumes(self) -> None:
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory=16 * 10**6)
        )
        permissions = infer_permissions_from_container(
            container,
            "example.com",
            "test-cluster",
            "test-org",
            project_name="testproject",
        )
        assert permissions == [
            Permission(uri="job://test-cluster/test-org/testproject", action="write")
        ]

    def test_volumes(self) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory=16 * 10**6),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://test-cluster/testuser/dataset"),
                    dst_path=PurePath("/var/storage/testuser/dataset"),
                    read_only=True,
                ),
                ContainerVolume(
                    uri=URL("storage://test-cluster/testuser/result"),
                    dst_path=PurePath("/var/storage/testuser/result"),
                ),
            ],
        )
        permissions = infer_permissions_from_container(
            container,
            "http://example.com",
            "test-cluster",
            "test-org",
            project_name="testproject",
        )
        assert permissions == [
            Permission(uri="job://test-cluster/test-org/testproject", action="write"),
            Permission(uri="storage://test-cluster/testuser/dataset", action="read"),
            Permission(uri="storage://test-cluster/testuser/result", action="write"),
        ]

    def test_image(self) -> None:
        container = Container(
            image="example.com/testuser/image",
            resources=ContainerResources(cpu=0.1, memory=16 * 10**6),
        )
        permissions = infer_permissions_from_container(
            container,
            "example.com",
            "test-cluster",
            "test-org",
            project_name="testproject",
        )
        assert permissions == [
            Permission(uri="job://test-cluster/test-org/testproject", action="write"),
            Permission(
                uri="image://test-cluster/test-org/testuser/image", action="read"
            ),
        ]


async def test_parse_response(mock_orchestrator: MockOrchestrator) -> None:
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(
                        cpu=1,
                        memory=128 * 10**6,
                        nvidia_gpu=1,
                        nvidia_gpu_model="nvidia-gpu",
                        nvidia_migs={
                            "1g.5gb": ContainerNvidiaMIGResource(
                                count=1, model="nvidia-mig"
                            )
                        },
                        amd_gpu=2,
                        amd_gpu_model="amd-gpu",
                        intel_gpu=3,
                        intel_gpu_model="intel-gpu",
                        shm=True,
                        tpu=ContainerTPUResource(type="type", software_version="1.0"),
                    ),
                    volumes=[
                        ContainerVolume(
                            uri=URL("storage://test-cluster/testuser/dataset"),
                            dst_path=PurePath("/var/storage/testuser/dataset"),
                            read_only=True,
                        ),
                        ContainerVolume(
                            uri=URL("storage://test-cluster/testuser/result"),
                            dst_path=PurePath("/var/storage/testuser/result"),
                        ),
                    ],
                    secret_env={
                        "test": Secret.create(
                            "secrete://test-cluster/test-user/test-secret"
                        )
                    },
                    disk_volumes=[
                        DiskContainerVolume.create(
                            "disk://test-cluster/test-user/test-disk",
                            dst_path=PurePath("/container"),
                            read_only=False,
                        )
                    ],
                    secret_volumes=[
                        SecretContainerVolume.create(
                            "secret://test-cluster/test-user/test-secret",
                            dst_path=PurePath("/container"),
                        )
                    ],
                    http_server=ContainerHTTPServer(
                        port=8080,
                    ),
                ),
                description="test test description",
            ),
            cluster_name="test-cluster",
            org_name="test-org",
            name="test-job-name",
            scheduler_enabled=True,
            energy_schedule_name="green",
            project_name="test-project",
        ),
    )

    response = convert_job_to_job_response(job)
    assert job_response_to_job_record(response).to_primitive() == job.to_primitive()


async def test_job_to_job_response(mock_orchestrator: MockOrchestrator) -> None:
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory=128 * 10**6),
                ),
                description="test test description",
            ),
            cluster_name="test-cluster",
            org_name="test-tenant-id",
            name="test-job-name",
            project_name="test-project",
            preset_name="cpu-small",
        ),
    )
    response = convert_job_to_job_response(job)
    assert response == {
        "id": job.id,
        "owner": "compute",
        "cluster_name": "test-cluster",
        "org_name": "test-tenant-id",
        "project_name": "test-project",
        "org_project_hash": "61c2aef2a5",
        "namespace": "platform--test-tenant-i--test-project--d0d03a56494108f3b9fb51c1",
        "status": "pending",
        "statuses": [
            {
                "description": None,
                "reason": None,
                "status": "pending",
                "transition_time": mock.ANY,
            }
        ],
        "history": {
            "status": "pending",
            "reason": None,
            "description": None,
            "created_at": mock.ANY,
            "run_time_seconds": 0,
            "restarts": 0,
        },
        "container": {
            "image": "testimage",
            "env": {},
            "volumes": [],
            "resources": {"cpu": 1, "memory": 128000000, "memory_mb": 122},
        },
        "name": "test-job-name",
        "description": "test test description",
        "scheduler_enabled": False,
        "preemptible_node": False,
        "is_preemptible": False,
        "is_preemptible_node_required": False,
        "materialized": False,
        "pass_config": False,
        "uri": f"job://test-cluster/test-tenant-id/test-project/{job.id}",
        "restart_policy": "never",
        "privileged": False,
        "being_dropped": False,
        "logs_removed": False,
        "total_price_credits": "0",
        "price_credits_per_hour": "10",
        "priority": "normal",
        "internal_hostname": mock.ANY,
        "internal_hostname_named": mock.ANY,
        "preset_name": "cpu-small",
    }


async def test_job_to_job_response_nonzero_runtime(
    mock_orchestrator: MockOrchestrator,
) -> None:
    def _mocked_datetime_factory() -> datetime:
        return datetime(year=2019, month=1, day=1)

    time_now = _mocked_datetime_factory()
    started_ago_delta = timedelta(minutes=10)  # job started 10 min ago: pending
    pending_delta = timedelta(
        minutes=5, seconds=30
    )  # after 5 min: running (still running)
    pending_at = time_now - started_ago_delta
    running_at = pending_at + pending_delta
    items = [
        JobStatusItem.create(JobStatus.PENDING, transition_time=pending_at),
        JobStatusItem.create(JobStatus.RUNNING, transition_time=running_at),
    ]
    status_history = JobStatusHistory(items)

    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory=128 * 10**6),
                ),
                description="test test description",
            ),
            status_history=status_history,
            cluster_name="test-cluster",
            org_name="test-org",
            project_name="test-project",
            name="test-job-name",
        ),
        current_datetime_factory=_mocked_datetime_factory,
    )
    response = convert_job_to_job_response(job)
    run_time = response["history"]["run_time_seconds"]
    assert run_time == (time_now - running_at).total_seconds()


async def test_job_to_job_response_with_job_name_and_http_exposed(
    mock_orchestrator: MockOrchestrator,
) -> None:
    owner_name = "a" * USER_NAME_MAX_LENGTH
    job_name = "b" * JOB_NAME_MAX_LENGTH
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory=128 * 10**6),
                    http_server=ContainerHTTPServer(port=80),
                )
            ),
            cluster_name="test-cluster",
            org_name="test-org",
            project_name="test-project",
            owner=owner_name,
            name=job_name,
            preset_name="cpu-small",
        ),
    )
    response = convert_job_to_job_response(job)
    assert response == {
        "id": job.id,
        "owner": owner_name,
        "cluster_name": "test-cluster",
        "project_name": "test-project",
        "org_name": "test-org",
        "org_project_hash": "48e35fc28a",
        "namespace": "platform--test-org--test-project--a5e4118c50842e7acbd85e28",
        "name": job_name,
        "http_url": f"https://{job.id}.jobs",
        "http_url_named": job.http_url_named,
        "status": "pending",
        "statuses": [
            {
                "description": None,
                "reason": None,
                "status": "pending",
                "transition_time": mock.ANY,
            }
        ],
        "history": {
            "status": "pending",
            "reason": None,
            "description": None,
            "created_at": mock.ANY,
            "run_time_seconds": 0,
            "restarts": 0,
        },
        "container": {
            "image": "testimage",
            "env": {},
            "volumes": [],
            "resources": {"cpu": 1, "memory": 128000000, "memory_mb": 122},
            "http": {"port": 80, "health_check_path": "/", "requires_auth": False},
        },
        "scheduler_enabled": False,
        "preemptible_node": False,
        "is_preemptible": False,
        "is_preemptible_node_required": False,
        "materialized": False,
        "pass_config": False,
        "uri": f"job://test-cluster/test-org/test-project/{job.id}",
        "restart_policy": "never",
        "privileged": False,
        "being_dropped": False,
        "logs_removed": False,
        "price_credits_per_hour": "10",
        "total_price_credits": "0",
        "priority": "normal",
        "internal_hostname": mock.ANY,
        "internal_hostname_named": mock.ANY,
        "preset_name": "cpu-small",
    }


async def test_job_to_job_response_with_job_name_and_http_exposed_too_long_name(
    mock_orchestrator: MockOrchestrator,
) -> None:
    owner_name = "a" * USER_NAME_MAX_LENGTH
    job_name = "b" * (JOB_NAME_MAX_LENGTH + 1)
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory=128 * 10**6),
                    http_server=ContainerHTTPServer(port=80),
                )
            ),
            cluster_name="test-cluster",
            org_name="test-org",
            project_name="test-project",
            owner=owner_name,
            name=job_name,
            preset_name="cpu-small",
        ),
    )
    response = convert_job_to_job_response(job)
    assert response == {
        "id": job.id,
        "owner": owner_name,
        "cluster_name": "test-cluster",
        "org_name": "test-org",
        "project_name": "test-project",
        "org_project_hash": "48e35fc28a",
        "namespace": "platform--test-org--test-project--a5e4118c50842e7acbd85e28",
        "name": job_name,
        "http_url": f"https://{job.id}.jobs",
        "http_url_named": job.http_url_named,
        # NOTE: field `http_url_named` is cut off when it is invalid
        "status": "pending",
        "statuses": [
            {
                "description": None,
                "reason": None,
                "status": "pending",
                "transition_time": mock.ANY,
            }
        ],
        "history": {
            "status": "pending",
            "reason": None,
            "description": None,
            "created_at": mock.ANY,
            "run_time_seconds": 0,
            "restarts": 0,
        },
        "container": {
            "image": "testimage",
            "env": {},
            "volumes": [],
            "resources": {"cpu": 1, "memory": 128000000, "memory_mb": 122},
            "http": {"port": 80, "health_check_path": "/", "requires_auth": False},
        },
        "scheduler_enabled": False,
        "preemptible_node": False,
        "is_preemptible": False,
        "is_preemptible_node_required": False,
        "materialized": False,
        "pass_config": False,
        "uri": f"job://test-cluster/test-org/test-project/{job.id}",
        "restart_policy": "never",
        "privileged": False,
        "being_dropped": False,
        "logs_removed": False,
        "price_credits_per_hour": "10",
        "total_price_credits": "0",
        "priority": "normal",
        "internal_hostname": mock.ANY,
        "internal_hostname_named": mock.ANY,
        "preset_name": "cpu-small",
    }


async def test_job_to_job_response_assert_non_empty_cluster_name(
    mock_orchestrator: MockOrchestrator,
) -> None:
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory=128 * 10**6),
                )
            ),
            cluster_name="",
            org_name="test-org",
            project_name="test-project",
        ),
    )
    with pytest.raises(AssertionError):
        convert_job_to_job_response(job)


async def test_job_to_job_response_with_preset_name(
    mock_orchestrator: MockOrchestrator,
) -> None:
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory=128 * 10**6),
                )
            ),
            cluster_name="test-cluster",
            org_name="test-org",
            project_name="test-project",
            preset_name="cpu-small",
        ),
    )
    payload = convert_job_to_job_response(job)

    assert payload["preset_name"] == "cpu-small"


async def test_job_to_job_response__energy_schedule_name(
    mock_orchestrator: MockOrchestrator,
) -> None:
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory=128 * 10**6),
                )
            ),
            cluster_name="test-cluster",
            org_name="test-org",
            project_name="test-project",
            preset_name="cpu-small",
            scheduler_enabled=True,
            energy_schedule_name="green",
        ),
    )
    payload = convert_job_to_job_response(job)

    assert payload["energy_schedule_name"] == "green"
    assert payload["energy_schedule_name"] == "green"
