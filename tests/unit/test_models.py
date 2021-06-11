from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import PurePath
from typing import Any, Dict, Sequence
from unittest import mock

import pytest
from multidict import MultiDict
from neuro_auth_client import Permission, User as AuthUser
from neuro_auth_client.client import ClientAccessSubTreeView, ClientSubTreeViewRoot
from trafaret import DataError
from yarl import URL

from platform_api.handlers.jobs_handler import (
    BulkJobFilter,
    BulkJobFilterBuilder,
    JobFilterException,
    JobFilterFactory,
    convert_job_container_to_json,
    convert_job_to_job_response,
    create_job_cluster_name_validator,
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
from platform_api.resource import Preset, TPUPreset, TPUResource

from .conftest import MockOrchestrator


class TestContainerRequestValidator:
    @pytest.fixture
    def payload(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_zero_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 0},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_negative_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": -1},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_one_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 1},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_too_many_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 130},
            "volumes": [
                {
                    "src_storage_uri": "storage://test-cluster/",
                    "dst_path": "/var/storage",
                }
            ],
        }

    @pytest.fixture
    def payload_with_dev_shm(self) -> Dict[str, Any]:
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

    def test_allowed_volumes(self, payload: Dict[str, Any]) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload)
        assert result["volumes"][0]["read_only"]
        assert "shm" not in result["resources"]

    def test_allowed_volumes_with_shm(
        self, payload_with_dev_shm: Dict[str, Any]
    ) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload_with_dev_shm)
        assert result["volumes"][0]["read_only"]
        assert result["resources"]["shm"]

    def test_disallowed_volumes(self, payload: Dict[str, Any]) -> None:
        validator = create_container_request_validator(cluster_name="test-cluster")
        with pytest.raises(ValueError, match="volumes is not allowed key"):
            validator.check(payload)

    def test_with_zero_gpu(self, payload_with_zero_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload_with_zero_gpu)
        assert result["resources"]["gpu"] == 0

    def test_with_one_gpu(self, payload_with_one_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload_with_one_gpu)
        assert result["resources"]["gpu"]
        assert result["resources"]["gpu"] == 1

    def test_with_too_many_gpu(self, payload_with_too_many_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        with pytest.raises(ValueError, match="gpu"):
            validator.check(payload_with_too_many_gpu)

    def test_with_negative_gpu(self, payload_with_negative_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        with pytest.raises(ValueError, match="gpu"):
            validator.check(payload_with_negative_gpu)

    def test_gpu_model_but_no_gpu(self) -> None:
        cluster = "test-cluster"
        payload = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu_model": "unknown"},
        }
        validator = create_container_request_validator(cluster_name=cluster)
        with pytest.raises(ValueError, match="gpu_model is not allowed key"):
            validator.check(payload)

    def test_gpu_model_unknown(self) -> None:
        cluster = "test-cluster"
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "gpu": 1,
                "gpu_model": "unknown",
            },
        }
        validator = create_container_request_validator(cluster_name=cluster)
        with pytest.raises(ValueError, match="value doesn't match any variant"):
            validator.check(payload)

    def test_gpu_model(self) -> None:
        cluster = "test-cluster"
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "gpu": 1,
                "gpu_model": "unknown",
            },
        }
        validator = create_container_request_validator(
            allowed_gpu_models=["unknown"], cluster_name=cluster
        )
        result = validator.check(payload)
        assert result["resources"]["gpu"] == 1
        assert result["resources"]["gpu_model"] == "unknown"

    def test_gpu_tpu_conflict(self) -> None:
        cluster = "test-cluster"
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "gpu": 1,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
        }
        validator = create_container_request_validator(cluster_name=cluster)
        with pytest.raises(ValueError, match="tpu is not allowed key"):
            validator.check(payload)

    @pytest.mark.parametrize(
        "allowed_tpu_resources",
        ([], [TPUResource(types=["v2-8"], software_versions=["1.14"])]),
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
                TPUResource(types=["v2-8"], software_versions=["1.14"])
            ],
            cluster_name=cluster,
        )
        result = validator.check(payload)
        assert result["resources"]["tpu"] == {
            "type": "v2-8",
            "software_version": "1.14",
        }

    def test_with_entrypoint_and_cmd(self, payload: Dict[str, Any]) -> None:
        payload["entrypoint"] = "/script.sh"
        payload["command"] = "arg1 arg2 arg3"
        validator = create_container_request_validator(
            allow_volumes=True, cluster_name="test-cluster"
        )
        result = validator.check(payload)
        assert result["entrypoint"] == "/script.sh"
        assert result["command"] == "arg1 arg2 arg3"

    def test_invalid_entrypoint(self, payload: Dict[str, Any]) -> None:
        payload["entrypoint"] = '"'
        validator = create_container_request_validator(cluster_name="test-cluster")
        with pytest.raises(DataError, match="invalid command format"):
            validator.check(payload)

    def test_invalid_command(self, payload: Dict[str, Any]) -> None:
        payload["command"] = '"'
        validator = create_container_request_validator(cluster_name="test-cluster")
        with pytest.raises(DataError, match="invalid command format"):
            validator.check(payload)


class TestContainerResponseValidator:
    def test_gpu_model(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "gpu": 1,
                "gpu_model": "unknown",
            },
        }
        validator = create_container_response_validator()
        result = validator.check(payload)
        assert result["resources"]["gpu"] == 1
        assert result["resources"]["gpu_model"] == "unknown"

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
        validator = create_job_cluster_name_validator("default")
        payload = validator.check(request)
        assert payload["cluster_name"] == "default"

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
        validator = create_job_cluster_name_validator("default")
        payload = validator.check(request)
        assert payload["cluster_name"] == "testcluster"

    def test_invalid_payload_type(self) -> None:
        validator = create_job_cluster_name_validator("default")
        with pytest.raises(DataError):
            validator.check([])

    def test_invalid_cluster_name_type(self) -> None:
        request = {
            "cluster_name": 123,
        }
        validator = create_job_cluster_name_validator("default")
        with pytest.raises(DataError, match="value is not a string"):
            validator.check(request)


class TestJobPresetValidator:
    def test_validator(self) -> None:
        request = {"preset_name": "preset", "container": {}}
        validator = create_job_preset_validator(
            [
                Preset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory_mb=100,
                )
            ]
        )
        payload = validator.check(request)

        assert payload == {
            "preset_name": "preset",
            "container": {"resources": {"cpu": 0.1, "memory_mb": 100, "shm": False}},
            "scheduler_enabled": False,
            "preemptible_node": False,
        }

    def test_validator_default_preset(self) -> None:
        request: Dict[str, Any] = {"container": {}}
        validator = create_job_preset_validator(
            [
                Preset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory_mb=100,
                )
            ]
        )
        payload = validator.check(request)

        assert payload == {
            "preset_name": "preset",
            "container": {"resources": {"cpu": 0.1, "memory_mb": 100, "shm": False}},
            "scheduler_enabled": False,
            "preemptible_node": False,
        }

    def test_flat_structure_validator(self) -> None:
        request = {"preset_name": "preset"}
        validator = create_job_preset_validator(
            [
                Preset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory_mb=100,
                )
            ]
        )
        payload = validator.check(request)

        assert payload == {
            "preset_name": "preset",
            "resources": {"cpu": 0.1, "memory_mb": 100, "shm": False},
            "scheduler_enabled": False,
            "preemptible_node": False,
        }

    def test_validator_unknown_preset_name(self) -> None:
        request = {"preset_name": "unknown", "container": {}}
        validator = create_job_preset_validator(
            [
                Preset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory_mb=100,
                )
            ]
        )
        with pytest.raises(DataError, match="value doesn't match any variant"):
            validator.check(request)

    def test_validator_preset_name_and_resources(self) -> None:
        request = {
            "preset_name": "preset",
            "container": {"resources": {"cpu": 1.0, "memory_mb": 1024}},
        }
        validator = create_job_preset_validator(
            [
                Preset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory_mb=100,
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
            "resources": {"cpu": 1.0, "memory_mb": 1024},
        }
        validator = create_job_preset_validator(
            [
                Preset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory_mb=100,
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
                Preset(
                    name="preset",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory_mb=100,
                    gpu=1,
                    gpu_model="nvidia-tesla-k80",
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
                    "memory_mb": 100,
                    "shm": True,
                    "gpu": 1,
                    "gpu_model": "nvidia-tesla-k80",
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
            allowed_gpu_models=(), allowed_tpu_resources=(), cluster_name="testcluster"
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
            allowed_gpu_models=(), allowed_tpu_resources=(), cluster_name="testcluster"
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
            allowed_gpu_models=(),
            allowed_tpu_resources=(),
            cluster_name="testcluster",
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
            allowed_gpu_models=(), allowed_tpu_resources=(), cluster_name="testcluster"
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
            allowed_gpu_models=(), allowed_tpu_resources=(), cluster_name="another"
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
            allowed_gpu_models=(), allowed_tpu_resources=(), cluster_name="test-cluster"
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
            allowed_gpu_models=(), allowed_tpu_resources=(), cluster_name="test-cluster"
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
            allowed_gpu_models=(), allowed_tpu_resources=(), cluster_name="testcluster"
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
            allowed_gpu_models=(), allowed_tpu_resources=(), cluster_name="clustername"
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
            allowed_gpu_models=(), allowed_tpu_resources=(), cluster_name="clustername"
        )
        validator.check(request)


class TestJobContainerToJson:
    def test_minimal(self) -> None:
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory_mb=16)
        )
        assert convert_job_container_to_json(container) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "volumes": [],
        }

    def test_tpu_resource(self) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(
                cpu=0.1,
                memory_mb=16,
                tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
            ),
        )
        assert convert_job_container_to_json(container) == {
            "env": {},
            "image": "image",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
            "volumes": [],
        }

    def test_gpu_and_shm_resources(self) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory_mb=16, gpu=1, shm=True),
        )
        assert convert_job_container_to_json(container) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 1, "shm": True},
            "volumes": [],
        }

    def test_with_working_dir(self) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
            working_dir="/working/dir",
        )
        assert convert_job_container_to_json(container) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16},
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

        query: Any = MultiDict([("hostname", "test-job--john-doe.example.org")])
        assert factory(query) == JobFilter(name="test-job", base_owners={"john-doe"})

        query = MultiDict([("hostname", "test-job-id.example.org")])
        assert factory(query) == JobFilter(ids={"test-job-id"})

        query = MultiDict(
            [
                ("hostname", "test-job--john-doe.example.org"),
                ("hostname", "test-job-id.example.org"),
            ]
        )
        assert factory(query) == JobFilter(name="test-job", base_owners={"john-doe"})

        query = MultiDict(
            [
                ("hostname", "test-job-id.example.org"),
                ("hostname", "test-job--john-doe.example.org"),
            ]
        )
        assert factory(query) == JobFilter(name=None, ids={"test-job-id"})

    def test_create_from_query_by_hostname_with_status(self) -> None:
        factory = JobFilterFactory().create_from_query

        query: Any = MultiDict(
            [
                ("hostname", "test-job--john-doe.example.org"),
                ("status", "failed"),
                ("status", "succeeded"),
            ]
        )
        assert factory(query) == JobFilter(
            statuses={JobStatus.FAILED, JobStatus.SUCCEEDED},
            name="test-job",
            base_owners={"john-doe"},
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
            [("hostname", "testjob--johndoe.example.org"), ("owner", "johndoe")],
            [("hostname", "TESTJOB--johndoe.example.org")],
            [("hostname", "testjob--JOHNDOE.example.org")],
        ],
    )
    def test_create_from_query_fail(self, query: Any) -> None:
        factory = JobFilterFactory().create_from_query
        with pytest.raises((ValueError, DataError)):
            factory(MultiDict(query))  # type: ignore


def make_access_tree(perm_dict: Dict[str, str]) -> ClientSubTreeViewRoot:
    tree = ClientSubTreeViewRoot(
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

    def test_no_access_with_owners(self) -> None:
        query_filter = JobFilter(owners={"someuser"})
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

    def test_user_access_same_user(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree(
            {"test-cluster/testuser": "read", "anothercluster/testuser": "read"}
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}, "anothercluster": {}},
                owners={"testuser"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_same_cluster(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree(
            {"test-cluster/testuser": "read", "test-cluster/anotheruser": "read"}
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}}, owners={"testuser", "anotheruser"}
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_different_users_and_clusters(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree(
            {"test-cluster/testuser": "read", "anothercluster/anotheruser": "read"}
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={
                    "test-cluster": {"testuser": set()},
                    "anothercluster": {"anotheruser": set()},
                },
                owners={"testuser", "anotheruser"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_mixed_users_and_clusters(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser": "read",
                "anothercluster/testuser": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}, "anothercluster": {"testuser": set()}},
                owners={"testuser", "anotheruser"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_user_access_mixed_users_and_clusters2(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser": "read",
                "anothercluster/testuser": "read",
                "anothercluster/thirduser": "read",
                "thirdcluster/testuser": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={
                    "test-cluster": {"testuser": set(), "anotheruser": set()},
                    "anothercluster": {"testuser": set(), "thirduser": set()},
                    "thirdcluster": {"testuser": set()},
                },
                owners={"testuser", "anotheruser", "thirduser"},
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_cluster_user_access(self) -> None:
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
                    "test-cluster": {"testuser": set(), "anotheruser": set()},
                    "anothercluster": {},
                },
            ),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_access_no_owners(self) -> None:
        query_filter = JobFilter()
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/job-test-2": "deny",
                "test-cluster/someuser": "deny",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(clusters={"test-cluster": {}}, owners={"testuser"}),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(),
        )

    def test_mixed_access_owners_shared_all(self) -> None:
        query_filter = JobFilter(owners={"testuser"})
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/job-test-2": "deny",
                "test-cluster/someuser": "deny",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(clusters={"test-cluster": {}}, owners={"testuser"}),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_access_shared_ids_only(self) -> None:
        query_filter = JobFilter(owners={"anotheruser"})
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/job-test-2": "deny",
                "test-cluster/someuser": "deny",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=None,
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(owners={"anotheruser"}),
        )

    def test_mixed_access_owners_shared_all_and_specific(self) -> None:
        query_filter = JobFilter(
            owners={"testuser", "anotheruser"},
            statuses={JobStatus.PENDING},
            name="testname",
        )
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/job-test-2": "deny",
                "test-cluster/someuser": "deny",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}},
                owners={"testuser"},
                statuses={JobStatus.PENDING},
                name="testname",
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(
                owners={"testuser", "anotheruser"},
                statuses={JobStatus.PENDING},
                name="testname",
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
                    "test-cluster": {"testuser": set(), "anotheruser": {"testname"}}
                },
                owners={"testuser", "anotheruser"},
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(),
        )

    def test_shared_by_name_with_name(self) -> None:
        query_filter = JobFilter(name="testname")
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/testname": "read",
                "test-cluster/anotheruser/testname2": "read",
                "test-cluster/thirduser/testname": "read",
                "test-cluster/forthduser/testname2": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {}},
                owners={"testuser", "anotheruser", "thirduser"},
                name="testname",
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(name="testname"),
        )

    def test_shared_by_name_with_owners(self) -> None:
        query_filter = JobFilter(owners={"anotheruser", "someuser"})
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/testname": "read",
                "test-cluster/thirduser/testname": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                clusters={"test-cluster": {"anotheruser": {"testname"}}},
                owners={"anotheruser"},
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(owners={"anotheruser", "someuser"}),
        )

    def test_shared_by_name_with_owner_and_name(self) -> None:
        query_filter = JobFilter(owners={"anotheruser"}, name="testname")
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
                clusters={"test-cluster": {}}, owners={"anotheruser"}, name="testname"
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(owners={"anotheruser"}, name="testname"),
        )

    def test_shared_by_name_with_owner_and_name_2(self) -> None:
        query_filter = JobFilter(owners={"anotheruser"}, name="testname")
        tree = make_access_tree(
            {
                "test-cluster/testuser": "read",
                "test-cluster/anotheruser/job-test-1": "read",
                "test-cluster/anotheruser/testname2": "read",
                "test-cluster/thirduser/testname": "read",
            }
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=None,
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(owners={"anotheruser"}, name="testname"),
        )


class TestInferPermissionsFromContainer:
    def test_no_volumes(self) -> None:
        user = AuthUser(name="testuser")
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory_mb=16)
        )
        permissions = infer_permissions_from_container(
            user, container, "example.com", "test-cluster"
        )
        assert permissions == [
            Permission(uri="job://test-cluster/testuser", action="write")
        ]

    def test_volumes(self) -> None:
        user = AuthUser(name="testuser")
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
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
            user, container, "http://example.com", "test-cluster"
        )
        assert permissions == [
            Permission(uri="job://test-cluster/testuser", action="write"),
            Permission(uri="storage://test-cluster/testuser/dataset", action="read"),
            Permission(uri="storage://test-cluster/testuser/result", action="write"),
        ]

    def test_image(self) -> None:
        user = AuthUser(name="testuser")
        container = Container(
            image="example.com/testuser/image",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
        )
        permissions = infer_permissions_from_container(
            user, container, "example.com", "test-cluster"
        )
        assert permissions == [
            Permission(uri="job://test-cluster/testuser", action="write"),
            Permission(uri="image://test-cluster/testuser/image", action="read"),
        ]


@pytest.mark.asyncio
async def test_parse_response(mock_orchestrator: MockOrchestrator) -> None:
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(
                        cpu=1,
                        memory_mb=128,
                        gpu=1,
                        gpu_model_id="nvidia-tesla-k80",
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
            name="test-job-name",
        ),
    )

    response = convert_job_to_job_response(job)
    assert job_response_to_job_record(response).to_primitive() == job.to_primitive()


@pytest.mark.asyncio
async def test_job_to_job_response(mock_orchestrator: MockOrchestrator) -> None:
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory_mb=128),
                ),
                description="test test description",
            ),
            cluster_name="test-cluster",
            name="test-job-name",
        ),
    )
    response = convert_job_to_job_response(job)
    assert response == {
        "id": job.id,
        "owner": "compute",
        "cluster_name": "test-cluster",
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
            "resources": {"cpu": 1, "memory_mb": 128},
        },
        "name": "test-job-name",
        "description": "test test description",
        "scheduler_enabled": False,
        "preemptible_node": False,
        "is_preemptible": False,
        "is_preemptible_node_required": False,
        "materialized": False,
        "pass_config": False,
        "uri": f"job://test-cluster/compute/{job.id}",
        "restart_policy": "never",
        "privileged": False,
    }


@pytest.mark.asyncio
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
                    resources=ContainerResources(cpu=1, memory_mb=128),
                ),
                description="test test description",
            ),
            status_history=status_history,
            cluster_name="test-cluster",
            name="test-job-name",
        ),
        current_datetime_factory=_mocked_datetime_factory,
    )
    response = convert_job_to_job_response(job)
    run_time = response["history"]["run_time_seconds"]
    assert run_time == (time_now - running_at).total_seconds()


@pytest.mark.asyncio
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
                    resources=ContainerResources(cpu=1, memory_mb=128),
                    http_server=ContainerHTTPServer(port=80),
                )
            ),
            cluster_name="test-cluster",
            owner=owner_name,
            name=job_name,
        ),
    )
    response = convert_job_to_job_response(job)
    assert response == {
        "id": job.id,
        "owner": owner_name,
        "cluster_name": "test-cluster",
        "name": job_name,
        "http_url": f"http://{job.id}.jobs",
        "http_url_named": f"http://{job_name}--{owner_name}.jobs",
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
            "resources": {"cpu": 1, "memory_mb": 128},
            "http": {"port": 80, "health_check_path": "/", "requires_auth": False},
        },
        "scheduler_enabled": False,
        "preemptible_node": False,
        "is_preemptible": False,
        "is_preemptible_node_required": False,
        "materialized": False,
        "pass_config": False,
        "uri": f"job://test-cluster/{owner_name}/{job.id}",
        "restart_policy": "never",
        "privileged": False,
    }


@pytest.mark.asyncio
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
                    resources=ContainerResources(cpu=1, memory_mb=128),
                    http_server=ContainerHTTPServer(port=80),
                )
            ),
            cluster_name="test-cluster",
            owner=owner_name,
            name=job_name,
        ),
    )
    response = convert_job_to_job_response(job)
    assert response == {
        "id": job.id,
        "owner": owner_name,
        "cluster_name": "test-cluster",
        "name": job_name,
        "http_url": f"http://{job.id}.jobs",
        "http_url_named": f"http://{job_name}--{owner_name}.jobs",
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
            "resources": {"cpu": 1, "memory_mb": 128},
            "http": {"port": 80, "health_check_path": "/", "requires_auth": False},
        },
        "scheduler_enabled": False,
        "preemptible_node": False,
        "is_preemptible": False,
        "is_preemptible_node_required": False,
        "materialized": False,
        "pass_config": False,
        "uri": f"job://test-cluster/{owner_name}/{job.id}",
        "restart_policy": "never",
        "privileged": False,
    }


@pytest.mark.asyncio
async def test_job_to_job_response_assert_non_empty_cluster_name(
    mock_orchestrator: MockOrchestrator,
) -> None:
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory_mb=128),
                )
            ),
            cluster_name="",
        ),
    )
    with pytest.raises(AssertionError, match="must be already replaced"):
        convert_job_to_job_response(job)


@pytest.mark.asyncio
async def test_job_to_job_response_with_preset_name(
    mock_orchestrator: MockOrchestrator,
) -> None:
    job = Job(
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory_mb=128),
                )
            ),
            cluster_name="test-cluster",
            preset_name="cpu-small",
        ),
    )
    payload = convert_job_to_job_response(job)

    assert payload["preset_name"] == "cpu-small"
