import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

import pytest
from aiohttp import ClientResponseError
from neuro_admin_client import (
    AdminClient,
    Balance,
    ClusterUser,
    ClusterUserRoleType,
    ClusterUserWithInfo,
    Quota,
    User,
    UserInfo,
)
from neuro_auth_client import AuthClient, Permission, User as AuthUser
from neuro_notifications_client import Client as NotificationsClient
from neuro_notifications_client.notifications import Notification
from yarl import URL

from platform_api.cluster import (
    Cluster,
    ClusterConfig,
    ClusterConfigRegistry,
    ClusterHolder,
)
from platform_api.cluster_config import (
    IngressConfig,
    OrchestratorConfig,
    RegistryConfig,
)
from platform_api.config import JobsConfig, JobsSchedulerConfig
from platform_api.orchestrator.base import Orchestrator
from platform_api.orchestrator.job import Job, JobRecord, JobStatusItem, JobStatusReason
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    Disk,
    JobError,
    JobNotFoundException,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import (
    InMemoryJobsStorage,
    JobsStorage,
    JobStorageTransactionError,
)
from platform_api.orchestrator.poller_service import (
    JobsPollerApi,
    JobsPollerService,
    JobsScheduler,
)
from platform_api.resource import ResourcePoolType


CA_DATA_PEM = "this-is-certificate-authority-public-key"


class MockOrchestrator(Orchestrator):
    def __init__(self, config: ClusterConfig) -> None:
        self._config = config
        self._mock_status_to_return = JobStatus.PENDING
        self._mock_reason_to_return: Optional[str] = JobStatusReason.CONTAINER_CREATING
        self._mock_exit_code_to_return: Optional[int] = None
        self._mock_statuses: Dict[str, JobStatus] = {}
        self._mock_reasons: Dict[str, Optional[str]] = {}
        self._mock_exit_codes: Dict[str, Optional[int]] = {}
        self.raise_on_get_job_status = False
        self.raise_on_start_job_status = False
        self.get_job_status_exc_factory = self._create_get_job_status_exc
        self.raise_on_delete = False
        self.delete_job_exc_factory = self._create_delete_job_exc
        self.current_datetime_factory: Callable[[], datetime] = partial(
            datetime.now, timezone.utc
        )
        self._successfully_deleted_jobs: List[Job] = []

    @property
    def config(self) -> OrchestratorConfig:
        return self._config.orchestrator

    def _get_status(self, job_id: str) -> JobStatus:
        return self._mock_statuses.get(job_id, self._mock_status_to_return)

    def _get_exit_code(self, job_id: str) -> Optional[int]:
        return self._mock_exit_codes.get(job_id, self._mock_exit_code_to_return)

    def _get_reason(self, job_id: str) -> Optional[str]:
        return self._mock_reasons.get(job_id, self._mock_reason_to_return)

    async def start_job(self, job: Job) -> JobStatus:
        if self.raise_on_start_job_status:
            raise self.get_job_status_exc_factory(job)
        job.status_history.current = JobStatusItem.create(
            self._get_status(job.id),
            reason=self._get_reason(job.id),
            exit_code=self._get_exit_code(job.id),
            current_datetime_factory=self.current_datetime_factory,
        )
        return job.status

    def _create_get_job_status_exc(self, job: Job) -> Exception:
        return JobNotFoundException(f"job {job.id} was not found")

    async def get_job_status(self, job: Job) -> JobStatusItem:
        if self.raise_on_get_job_status:
            raise self.get_job_status_exc_factory(job)
        return JobStatusItem.create(
            self._get_status(job.id),
            reason=self._get_reason(job.id),
            exit_code=self._get_exit_code(job.id),
            current_datetime_factory=self.current_datetime_factory,
        )

    def _create_delete_job_exc(self, job: Job) -> Exception:
        return JobError()

    async def delete_job(self, job: Job) -> JobStatus:
        if self.raise_on_delete:
            raise self.delete_job_exc_factory(job)
        self._successfully_deleted_jobs.append(job)
        return JobStatus.SUCCEEDED

    def update_status_to_return(self, new_status: JobStatus) -> None:
        self._mock_status_to_return = new_status

    def update_reason_to_return(self, new_reason: Optional[str]) -> None:
        self._mock_reason_to_return = new_reason

    def update_exit_code_to_return(self, new_exit_code: Optional[int]) -> None:
        self._mock_exit_code_to_return = new_exit_code

    def update_status_to_return_single(
        self, job_id: str, new_status: JobStatus
    ) -> None:
        self._mock_statuses[job_id] = new_status

    def update_reason_to_return_single(
        self, job_id: str, new_reason: Optional[str]
    ) -> None:
        self._mock_reasons[job_id] = new_reason

    def update_exit_code_to_return_single(
        self, job_id: str, new_exit_code: Optional[int]
    ) -> None:
        self._mock_exit_codes[job_id] = new_exit_code

    def get_successfully_deleted_jobs(self) -> List[Job]:
        return self._successfully_deleted_jobs

    async def get_missing_secrets(
        self, user_name: str, secret_names: List[str]
    ) -> List[str]:
        pass

    async def get_missing_disks(self, disks: List[Disk]) -> List[Disk]:
        pass


class MockJobsStorage(InMemoryJobsStorage):
    def __init__(self) -> None:
        super().__init__()
        self.fail_set_job_transaction = False

    async def set_job(self, job: JobRecord) -> None:
        if self.fail_set_job_transaction:
            raise JobStorageTransactionError("transaction failed")
        await super().set_job(job)


class MockNotificationsClient(NotificationsClient):
    def __init__(self) -> None:
        self._sent_notifications: List[Notification] = []
        pass

    async def notify(self, notification: Notification) -> None:
        self._sent_notifications.append(notification)

    async def init(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @property
    def sent_notifications(self) -> List[Notification]:
        return self._sent_notifications


class MockAuthClient(AuthClient):
    def __init__(self) -> None:
        self.user_to_return = AuthUser(
            name="testuser",
        )
        self._grants: List[Tuple[str, Sequence[Permission]]] = []
        self._revokes: List[Tuple[str, Sequence[str]]] = []

    async def get_user(self, name: str, token: Optional[str] = None) -> AuthUser:
        return self.user_to_return

    @property
    def grants(self) -> List[Tuple[str, Sequence[Permission]]]:
        return self._grants

    @property
    def revokes(self) -> List[Tuple[str, Sequence[str]]]:
        return self._revokes

    async def grant_user_permissions(
        self, name: str, permissions: Sequence[Permission], token: Optional[str] = None
    ) -> None:
        self._grants.append((name, permissions))

    async def revoke_user_permissions(
        self, name: str, resources_uris: Sequence[str], token: Optional[str] = None
    ) -> None:
        self._revokes.append((name, resources_uris))

    async def get_user_token(
        self,
        name: str,
        new_token_uri: Optional[str] = None,
        token: Optional[str] = None,
    ) -> str:
        return f"token-{name}"


class MockAdminClient(AdminClient):
    def __init__(self) -> None:
        self.users: Dict[str, User] = {}
        self.cluster_users: Dict[str, List[ClusterUser]] = defaultdict(list)
        self.spending_log: List[Tuple[str, str, Decimal, str]] = []
        self.debts_log: List[Tuple[str, str, Decimal, str]] = []
        self.raise_404: bool = False

    async def get_user_with_clusters(self, name: str) -> Tuple[User, List[ClusterUser]]:
        if name not in self.users:
            raise ClientResponseError(None, (), status=404)  # type: ignore
        return self.users[name], self.cluster_users[name]

    async def charge_user(
        self,
        cluster_name: str,
        username: str,
        spending: Decimal,
        idempotency_key: str,
    ) -> None:
        if self.raise_404:
            raise ClientResponseError(None, (), status=404)  # type: ignore
        self.spending_log.append((cluster_name, username, spending, idempotency_key))

    async def add_debt(
        self,
        cluster_name: str,
        username: str,
        credits: Decimal,
        idempotency_key: str,
    ) -> None:
        self.debts_log.append((cluster_name, username, credits, idempotency_key))

    @overload
    async def get_cluster_user(
        self, cluster_name: str, user_name: str, with_user_info: Literal[True]
    ) -> ClusterUserWithInfo:
        ...

    @overload
    async def get_cluster_user(
        self, cluster_name: str, user_name: str, with_user_info: Literal[False] = ...
    ) -> ClusterUser:
        ...

    async def get_cluster_user(
        self, cluster_name: str, user_name: str, with_user_info: bool = False
    ) -> Union[ClusterUser, ClusterUserWithInfo]:
        for cluster_user in self.cluster_users.get(user_name, []):
            if cluster_user.cluster_name == cluster_name:
                if with_user_info:
                    return cluster_user.add_info(
                        UserInfo(
                            email=self.users[user_name].email,
                            first_name=self.users[user_name].first_name,
                            last_name=self.users[user_name].last_name,
                            created_at=self.users[user_name].created_at,
                        )
                    )
                return cluster_user
        raise ClientResponseError(None, (), status=404)  # type: ignore


class MockJobsPollerApi(JobsPollerApi):
    def __init__(self, jobs_service: JobsService, jobs_storage: JobsStorage):
        self._jobs_service = jobs_service
        self._jobs_storage = jobs_storage

    async def get_unfinished_jobs(self) -> List[JobRecord]:
        return await self._jobs_storage.get_unfinished_jobs()

    async def get_jobs_for_deletion(self, *, delay: timedelta) -> List[JobRecord]:
        return await self._jobs_storage.get_jobs_for_deletion(delay=delay)

    async def push_status(self, job_id: str, status: JobStatusItem) -> None:
        await self._jobs_service.set_job_status(job_id, status)

    async def set_materialized(self, job_id: str, materialized: bool) -> None:
        async with self._jobs_storage.try_update_job(job_id=job_id) as record:
            record.materialized = materialized


@pytest.fixture
def job_request_factory() -> Callable[[], JobRequest]:
    def factory(with_gpu: bool = False) -> JobRequest:
        cont_kwargs: Dict[str, Any] = {"cpu": 1, "memory_mb": 128}
        if with_gpu:
            cont_kwargs["gpu"] = 1
            cont_kwargs["gpu_model_id"] = "nvidia-tesla-k80"

        return JobRequest.create(
            Container(image="testimage", resources=ContainerResources(**cont_kwargs))
        )

    return factory


@pytest.fixture()
def cert_authority_path(tmp_path: Path) -> str:
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(CA_DATA_PEM)
    return str(ca_path)


@pytest.fixture
def mock_job_request(job_request_factory: Callable[[], JobRequest]) -> JobRequest:
    return job_request_factory()


class MockCluster(Cluster):
    def __init__(self, config: ClusterConfig, orchestrator: Orchestrator) -> None:
        self._config = config
        self._orchestrator = orchestrator

    async def init(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @property
    def config(self) -> ClusterConfig:
        return self._config

    @property
    def orchestrator(self) -> Orchestrator:
        return self._orchestrator


@pytest.fixture
def registry_config() -> RegistryConfig:
    return RegistryConfig(username="compute", password="compute_token")


@pytest.fixture
def cluster_config() -> ClusterConfig:
    orchestrator_config = OrchestratorConfig(
        jobs_domain_name_template="{job_id}.jobs",
        jobs_internal_domain_name_template="{job_id}.default",
        resource_pool_types=[ResourcePoolType()],
    )
    return ClusterConfig(
        name="test-cluster",
        orchestrator=orchestrator_config,
        ingress=IngressConfig(
            registry_url=URL(),
            storage_url=URL(),
            blob_storage_url=URL(),
            monitoring_url=URL(),
            secrets_url=URL(),
            metrics_url=URL(),
            disks_url=URL(),
            buckets_url=URL(),
        ),
    )


@pytest.fixture
def mock_orchestrator(cluster_config: ClusterConfig) -> MockOrchestrator:
    return MockOrchestrator(config=cluster_config)


@pytest.fixture
async def cluster_holder(
    cluster_config: ClusterConfig, mock_orchestrator: MockOrchestrator
) -> AsyncIterator[ClusterHolder]:
    def _cluster_factory(config: ClusterConfig) -> Cluster:
        return MockCluster(config, mock_orchestrator)

    async with ClusterHolder(factory=_cluster_factory) as holder:
        await holder.update(cluster_config)
        yield holder


@pytest.fixture
async def cluster_config_registry(
    cluster_config: ClusterConfig,
) -> ClusterConfigRegistry:
    registry = ClusterConfigRegistry()
    await registry.replace(cluster_config)
    return registry


@pytest.fixture
def mock_api_base() -> URL:
    return URL("https://testing.neu.ro/api/v1")


@pytest.fixture
def mock_jobs_storage() -> MockJobsStorage:
    return MockJobsStorage()


@pytest.fixture
def mock_notifications_client() -> NotificationsClient:
    return MockNotificationsClient()


@pytest.fixture
def mock_auth_client() -> AuthClient:
    return MockAuthClient()


@pytest.fixture
def mock_admin_client() -> MockAdminClient:
    return MockAdminClient()


@pytest.fixture
def jobs_config() -> JobsConfig:
    return JobsConfig(orphaned_job_owner="compute")


@pytest.fixture
def scheduler_config() -> JobsSchedulerConfig:
    return JobsSchedulerConfig()


@pytest.fixture
def jobs_service(
    cluster_config_registry: ClusterConfigRegistry,
    mock_jobs_storage: MockJobsStorage,
    jobs_config: JobsConfig,
    mock_notifications_client: NotificationsClient,
    scheduler_config: JobsSchedulerConfig,
    mock_auth_client: AuthClient,
    mock_admin_client: AdminClient,
    mock_api_base: URL,
) -> JobsService:
    return JobsService(
        cluster_config_registry=cluster_config_registry,
        jobs_storage=mock_jobs_storage,
        jobs_config=jobs_config,
        notifications_client=mock_notifications_client,
        auth_client=mock_auth_client,
        admin_client=mock_admin_client,
        api_base_url=mock_api_base,
    )


@pytest.fixture
def mock_poller_api(
    jobs_service: JobsService,
    mock_jobs_storage: MockJobsStorage,
) -> MockJobsPollerApi:
    return MockJobsPollerApi(jobs_service, mock_jobs_storage)


@pytest.fixture
def jobs_poller_service(
    cluster_holder: ClusterHolder,
    jobs_config: JobsConfig,
    scheduler_config: JobsSchedulerConfig,
    mock_auth_client: AuthClient,
    mock_admin_client: AdminClient,
    mock_poller_api: MockJobsPollerApi,
) -> JobsPollerService:
    return JobsPollerService(
        cluster_holder=cluster_holder,
        jobs_config=jobs_config,
        scheduler=JobsScheduler(scheduler_config, mock_admin_client),
        auth_client=mock_auth_client,
        api=mock_poller_api,
    )


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


UserFactory = Callable[[str, List[Tuple[str, Balance, Quota]]], Awaitable[AuthUser]]


@pytest.fixture
def user_factory(
    mock_admin_client: MockAdminClient,
) -> Callable[[str, List[Tuple[str, Balance, Quota]]], Awaitable[AuthUser]]:
    async def _factory(
        name: str, clusters: List[Tuple[str, Balance, Quota]]
    ) -> AuthUser:
        mock_admin_client.users[name] = User(name=name, email=f"{name}@domain.com")
        for cluster, balance, quota in clusters:
            mock_admin_client.cluster_users[name].append(
                ClusterUser(
                    user_name=name,
                    cluster_name=cluster,
                    role=ClusterUserRoleType.USER,
                    balance=balance,
                    quota=quota,
                    org_name=None,
                )
            )
        return AuthUser(name=name)

    return _factory


@pytest.fixture
def test_cluster(user_factory: UserFactory) -> str:
    return "test-cluster"


@pytest.fixture
async def test_user(user_factory: UserFactory, test_cluster: str) -> AuthUser:
    return await user_factory("test_user", [(test_cluster, Balance(), Quota())])
