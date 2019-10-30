from pathlib import PurePath
from typing import Any, Dict, Sequence
from unittest import mock

import pytest
from multidict import MultiDict
from neuro_auth_client import Permission
from neuro_auth_client.client import ClientAccessSubTreeView, ClientSubTreeViewRoot
from trafaret import DataError
from yarl import URL

from platform_api.config import RegistryConfig, StorageConfig
from platform_api.handlers.jobs_handler import (
    BulkJobFilter,
    BulkJobFilterBuilder,
    JobFilterException,
    JobFilterFactory,
    convert_container_volume_to_json,
    convert_job_container_to_json,
    convert_job_to_job_response,
    infer_permissions_from_container,
)
from platform_api.handlers.validators import (
    JOB_NAME_MAX_LENGTH,
    USER_NAME_MAX_LENGTH,
    create_container_request_validator,
    create_container_response_validator,
)
from platform_api.orchestrator.job import Job, JobRecord
from platform_api.orchestrator.job_request import (
    Container,
    ContainerHTTPServer,
    ContainerResources,
    ContainerSSHServer,
    ContainerTPUResource,
    ContainerVolume,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.resource import TPUResource
from platform_api.user import User

from .conftest import MockOrchestrator


class TestContainerRequestValidator:
    @pytest.fixture
    def payload(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_zero_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 0},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_negative_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": -1},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_one_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 1},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_too_many_gpu(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 130},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_dev_shm(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
        }

    @pytest.fixture
    def payload_with_ssh(self) -> Dict[str, Any]:
        return {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "shm": True},
            "volumes": [{"src_storage_uri": "storage:///", "dst_path": "/var/storage"}],
            "ssh": {"port": 666},
        }

    def test_allowed_volumes(self, payload: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload)
        assert result["volumes"][0]["read_only"]
        assert "shm" not in result["resources"]

    def test_allowed_volumes_with_shm(
        self, payload_with_dev_shm: Dict[str, Any]
    ) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_dev_shm)
        assert result["volumes"][0]["read_only"]
        assert result["resources"]["shm"]

    def test_disallowed_volumes(self, payload: Dict[str, Any]) -> None:
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match="volumes is not allowed key"):
            validator.check(payload)

    def test_with_zero_gpu(self, payload_with_zero_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_zero_gpu)
        assert result["resources"]["gpu"] == 0

    def test_with_ssh(self, payload_with_ssh: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_ssh)
        assert result["ssh"]
        assert result["ssh"]["port"]
        assert result["ssh"]["port"] == 666

    def test_with_one_gpu(self, payload_with_one_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload_with_one_gpu)
        assert result["resources"]["gpu"]
        assert result["resources"]["gpu"] == 1

    def test_with_too_many_gpu(self, payload_with_too_many_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        with pytest.raises(ValueError, match="gpu"):
            validator.check(payload_with_too_many_gpu)

    def test_with_negative_gpu(self, payload_with_negative_gpu: Dict[str, Any]) -> None:
        validator = create_container_request_validator(allow_volumes=True)
        with pytest.raises(ValueError, match="gpu"):
            validator.check(payload_with_negative_gpu)

    def test_gpu_model_but_no_gpu(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu_model": "unknown"},
        }
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match="gpu_model is not allowed key"):
            validator.check(payload)

    def test_gpu_model_unknown(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "gpu": 1,
                "gpu_model": "unknown",
            },
        }
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match="value doesn't match any variant"):
            validator.check(payload)

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
        validator = create_container_request_validator(allowed_gpu_models=["unknown"])
        result = validator.check(payload)
        assert result["resources"]["gpu"] == 1
        assert result["resources"]["gpu_model"] == "unknown"

    def test_gpu_tpu_conflict(self) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "gpu": 1,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
        }
        validator = create_container_request_validator()
        with pytest.raises(ValueError, match="tpu is not allowed key"):
            validator.check(payload)

    @pytest.mark.parametrize(
        "allowed_tpu_resources",
        ([], [TPUResource(types=["v2-8"], software_versions=["1.14"])]),
    )
    def test_tpu_unavailable(
        self, allowed_tpu_resources: Sequence[TPUResource]
    ) -> None:
        payload = {
            "image": "testimage",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "tpu": {"type": "unknown", "software_version": "unknown"},
            },
        }
        validator = create_container_request_validator(
            allowed_tpu_resources=allowed_tpu_resources
        )
        with pytest.raises(ValueError):
            validator.check(payload)

    def test_tpu(self) -> None:
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
            ]
        )
        result = validator.check(payload)
        assert result["resources"]["tpu"] == {
            "type": "v2-8",
            "software_version": "1.14",
        }

    def test_with_entrypoint_and_cmd(self, payload: Dict[str, Any]) -> None:
        payload["entrypoint"] = "/script.sh"
        payload["command"] = "arg1 arg2 arg3"
        validator = create_container_request_validator(allow_volumes=True)
        result = validator.check(payload)
        assert result["entrypoint"] == "/script.sh"
        assert result["command"] == "arg1 arg2 arg3"


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


class TestJobContainerToJson:
    @pytest.fixture
    def storage_config(self) -> StorageConfig:
        return StorageConfig(host_mount_path=PurePath("/whatever"))

    def test_minimal(self, storage_config: StorageConfig) -> None:
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory_mb=16)
        )
        assert convert_job_container_to_json(container, storage_config) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "volumes": [],
        }

    def test_tpu_resource(self, storage_config: StorageConfig) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(
                cpu=0.1,
                memory_mb=16,
                tpu=ContainerTPUResource(type="v2-8", software_version="1.14"),
            ),
        )
        assert convert_job_container_to_json(container, storage_config) == {
            "env": {},
            "image": "image",
            "resources": {
                "cpu": 0.1,
                "memory_mb": 16,
                "tpu": {"type": "v2-8", "software_version": "1.14"},
            },
            "volumes": [],
        }

    def test_gpu_and_shm_resources(self, storage_config: StorageConfig) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory_mb=16, gpu=1, shm=True),
        )
        assert convert_job_container_to_json(container, storage_config) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 1, "shm": True},
            "volumes": [],
        }

    def test_with_ssh(self, storage_config: StorageConfig) -> None:
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory_mb=16, gpu=1, shm=True),
            ssh_server=ContainerSSHServer(port=777),
        )
        assert convert_job_container_to_json(container, storage_config) == {
            "env": {},
            "image": "image",
            "resources": {"cpu": 0.1, "memory_mb": 16, "gpu": 1, "shm": True},
            "volumes": [],
            "ssh": {"port": 777},
        }

    def test_src_storage_uri_fallback_default(
        self, storage_config: StorageConfig
    ) -> None:
        volume = ContainerVolume(
            uri=URL(""),
            src_path=PurePath("/"),
            dst_path=PurePath("/var/storage/username/dataset"),
        )
        payload = convert_container_volume_to_json(volume, storage_config)
        assert payload == {
            "src_storage_uri": "storage://username/dataset",
            "dst_path": "/var/storage/username/dataset",
            "read_only": False,
        }

    def test_src_storage_uri_fallback_root(self, storage_config: StorageConfig) -> None:
        volume = ContainerVolume(
            uri=URL(""), src_path=PurePath("/"), dst_path=PurePath("/var/storage")
        )
        payload = convert_container_volume_to_json(volume, storage_config)
        assert payload == {
            "src_storage_uri": "storage:",
            "dst_path": "/var/storage",
            "read_only": False,
        }

    def test_src_storage_uri_fallback_custom(
        self, storage_config: StorageConfig
    ) -> None:
        volume = ContainerVolume(
            uri=URL(""),
            src_path=PurePath("/"),
            dst_path=PurePath("/var/custom/username/dataset"),
        )
        payload = convert_container_volume_to_json(volume, storage_config)
        assert payload == {
            "src_storage_uri": "storage:",
            "dst_path": "/var/custom/username/dataset",
            "read_only": False,
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

        query = MultiDict([("owner", "alice"), ("owner", "bob")])
        assert factory(query) == JobFilter(owners={"bob", "alice"})

        query = MultiDict([("name", "test-job"), ("owner", "alice"), ("owner", "bob")])
        assert factory(query) == JobFilter(owners={"bob", "alice"}, name="test-job")

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
        assert factory(query) == JobFilter(name="test-job", owners={"john-doe"})

        query = MultiDict([("hostname", "test-job-id.example.org")])
        assert factory(query) == JobFilter(ids={"test-job-id"})

        query = MultiDict(
            [
                ("hostname", "test-job--john-doe.example.org"),
                ("hostname", "test-job-id.example.org"),
            ]
        )
        assert factory(query) == JobFilter(name="test-job", owners={"john-doe"})

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
            owners={"john-doe"},
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
            factory(MultiDict(query))  # type: ignore # noqa


class TestBulkJobFilterBuilder:
    def test_no_access(self) -> None:
        query_filter = JobFilter()
        tree = ClientSubTreeViewRoot(
            path="/", sub_tree=ClientAccessSubTreeView(action="deny", children={})
        )
        with pytest.raises(JobFilterException, match="no jobs"):
            BulkJobFilterBuilder(query_filter, tree).build()

    def test_no_access_with_owners(self) -> None:
        query_filter = JobFilter(owners={"someuser"})
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        with pytest.raises(JobFilterException, match="no jobs"):
            BulkJobFilterBuilder(query_filter, tree).build()

    def test_full_access_no_owners(self) -> None:
        query_filter = JobFilter()
        tree = ClientSubTreeViewRoot(
            path="/", sub_tree=ClientAccessSubTreeView(action="manage", children={})
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(), shared_ids=set(), shared_ids_filter=None
        )

    def test_full_access_with_owners(self) -> None:
        query_filter = JobFilter(owners={"testuser"})
        tree = ClientSubTreeViewRoot(
            path="/", sub_tree=ClientAccessSubTreeView(action="manage", children={})
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(owners={"testuser"}),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_access_no_owners(self) -> None:
        query_filter = JobFilter()
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(owners={"testuser"}),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(),
        )

    def test_mixed_access_owners_shared_all(self) -> None:
        query_filter = JobFilter(owners={"testuser"})
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(owners={"testuser"}),
            shared_ids=set(),
            shared_ids_filter=None,
        )

    def test_mixed_access_shared_ids_only(self) -> None:
        query_filter = JobFilter(owners={"anotheruser"})
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
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
        tree = ClientSubTreeViewRoot(
            path="/",
            sub_tree=ClientAccessSubTreeView(
                action="list",
                children={
                    "testuser": ClientAccessSubTreeView(action="read", children={}),
                    "anotheruser": ClientAccessSubTreeView(
                        action="list",
                        children={
                            "job-test-1": ClientAccessSubTreeView("read", children={}),
                            "job-test-2": ClientAccessSubTreeView("deny", children={}),
                        },
                    ),
                    "someuser": ClientAccessSubTreeView(action="deny", children={}),
                },
            ),
        )
        bulk_filter = BulkJobFilterBuilder(query_filter, tree).build()
        assert bulk_filter == BulkJobFilter(
            bulk_filter=JobFilter(
                owners={"testuser"}, statuses={JobStatus.PENDING}, name="testname"
            ),
            shared_ids={"job-test-1"},
            shared_ids_filter=JobFilter(
                owners={"testuser", "anotheruser"},
                statuses={JobStatus.PENDING},
                name="testname",
            ),
        )


class TestInferPermissionsFromContainer:
    def test_no_volumes(self) -> None:
        user = User(name="testuser", token="")
        container = Container(
            image="image", resources=ContainerResources(cpu=0.1, memory_mb=16)
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        permissions = infer_permissions_from_container(user, container, registry_config)
        assert permissions == [Permission(uri="job://testuser", action="write")]

    def test_volumes(self) -> None:
        user = User(name="testuser", token="")
        container = Container(
            image="image",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
            volumes=[
                ContainerVolume(
                    uri=URL("storage://testuser/dataset"),
                    src_path=PurePath("/"),
                    dst_path=PurePath("/var/storage/testuser/dataset"),
                    read_only=True,
                ),
                ContainerVolume(
                    uri=URL("storage://testuser/result"),
                    src_path=PurePath("/"),
                    dst_path=PurePath("/var/storage/testuser/result"),
                ),
            ],
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        permissions = infer_permissions_from_container(user, container, registry_config)
        assert permissions == [
            Permission(uri="job://testuser", action="write"),
            Permission(uri="storage://testuser/dataset", action="read"),
            Permission(uri="storage://testuser/result", action="write"),
        ]

    def test_image(self) -> None:
        user = User(name="testuser", token="")
        container = Container(
            image="example.com/testuser/image",
            resources=ContainerResources(cpu=0.1, memory_mb=16),
        )
        registry_config = RegistryConfig(url=URL("http://example.com"))
        permissions = infer_permissions_from_container(user, container, registry_config)
        assert permissions == [
            Permission(uri="job://testuser", action="write"),
            Permission(uri="image://testuser/image", action="read"),
        ]


@pytest.mark.asyncio
async def test_job_to_job_response(mock_orchestrator: MockOrchestrator) -> None:
    job = Job(
        storage_config=mock_orchestrator.storage_config,
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
    response = convert_job_to_job_response(job, cluster_name="my-cluster")
    assert response == {
        "id": job.id,
        "owner": "compute",
        "cluster_name": "my-cluster",
        "status": "pending",
        "history": {
            "status": "pending",
            "reason": None,
            "description": None,
            "created_at": mock.ANY,
        },
        "container": {
            "image": "testimage",
            "env": {},
            "volumes": [],
            "resources": {"cpu": 1, "memory_mb": 128},
        },
        "name": "test-job-name",
        "description": "test test description",
        "ssh_server": "ssh://nobody@ssh-auth:22",
        "ssh_auth_server": "ssh://nobody@ssh-auth:22",
        "is_preemptible": False,
    }


@pytest.mark.asyncio
async def test_job_to_job_response_with_job_name_and_http_exposed(
    mock_orchestrator: MockOrchestrator,
) -> None:
    owner_name = "a" * USER_NAME_MAX_LENGTH
    job_name = "b" * JOB_NAME_MAX_LENGTH
    job = Job(
        storage_config=mock_orchestrator.storage_config,
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
    response = convert_job_to_job_response(job, cluster_name="my-cluster")
    assert response == {
        "id": job.id,
        "owner": owner_name,
        "cluster_name": "my-cluster",
        "name": job_name,
        "http_url": f"http://{job.id}.jobs",
        "http_url_named": f"http://{job_name}--{owner_name}.jobs",
        "status": "pending",
        "history": {
            "status": "pending",
            "reason": None,
            "description": None,
            "created_at": mock.ANY,
        },
        "container": {
            "image": "testimage",
            "env": {},
            "volumes": [],
            "resources": {"cpu": 1, "memory_mb": 128},
            "http": {"port": 80, "health_check_path": "/", "requires_auth": False},
        },
        "ssh_server": "ssh://nobody@ssh-auth:22",
        "ssh_auth_server": "ssh://nobody@ssh-auth:22",
        "is_preemptible": False,
    }


@pytest.mark.asyncio
async def test_job_to_job_response_with_job_name_and_http_exposed_too_long_name(
    mock_orchestrator: MockOrchestrator,
) -> None:
    owner_name = "a" * USER_NAME_MAX_LENGTH
    job_name = "b" * (JOB_NAME_MAX_LENGTH + 1)
    job = Job(
        storage_config=mock_orchestrator.storage_config,
        orchestrator_config=mock_orchestrator.config,
        record=JobRecord.create(
            request=JobRequest.create(
                Container(
                    image="testimage",
                    resources=ContainerResources(cpu=1, memory_mb=128),
                    http_server=ContainerHTTPServer(port=80),
                )
            ),
            cluster_name="",
            owner=owner_name,
            name=job_name,
        ),
    )
    response = convert_job_to_job_response(job, cluster_name="my-cluster")
    assert response == {
        "id": job.id,
        "owner": owner_name,
        "cluster_name": "my-cluster",
        "name": job_name,
        "http_url": f"http://{job.id}.jobs",
        # NOTE: field `http_url_named` is cut off when it is invalid
        "status": "pending",
        "history": {
            "status": "pending",
            "reason": None,
            "description": None,
            "created_at": mock.ANY,
        },
        "container": {
            "image": "testimage",
            "env": {},
            "volumes": [],
            "resources": {"cpu": 1, "memory_mb": 128},
            "http": {"port": 80, "health_check_path": "/", "requires_auth": False},
        },
        "ssh_server": "ssh://nobody@ssh-auth:22",
        "ssh_auth_server": "ssh://nobody@ssh-auth:22",
        "is_preemptible": False,
    }


@pytest.mark.asyncio
async def test_job_to_job_response_assert_non_empty_cluster_name(
    mock_orchestrator: MockOrchestrator,
) -> None:
    job = Job(
        storage_config=mock_orchestrator.storage_config,
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
        convert_job_to_job_response(job, cluster_name="")
