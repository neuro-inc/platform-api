from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime, time, timedelta, timezone
from decimal import Decimal
from functools import partial
from pathlib import Path
from types import TracebackType
from typing import Any

import neuro_config_client
import pytest
from aiohttp import ClientResponseError
from neuro_admin_client import (
    AdminClient,
    AdminClientDummy,
    AuthClient as AdminAuthClient,
    Balance,
    ClusterUser,
    ClusterUserRoleType,
    ClusterUserWithInfo,
    Org,
    OrgCluster,
    OrgUser,
    OrgUserRoleType,
    OrgUserWithInfo,
    Quota,
    User,
    UserInfo,
)
from neuro_auth_client import AuthClient, Permission, User as AuthUser
from neuro_config_client import (
    ACMEEnvironment,
    AppsConfig,
    BucketsConfig,
    DisksConfig,
    DNSConfig,
    EnergyConfig,
    EnergySchedule,
    EnergySchedulePeriod,
    IngressConfig,
    MetricsConfig,
    MonitoringConfig,
    OrchestratorConfig,
    ResourcePoolType,
    ResourcePreset,
    SecretsConfig,
    StorageConfig,
)
from neuro_notifications_client import Client as NotificationsClient
from neuro_notifications_client.notifications import Notification
from yarl import URL

from platform_api.cluster import (
    Cluster,
    ClusterConfigRegistry,
    ClusterHolder,
)
from platform_api.config import JobsConfig, JobsSchedulerConfig, RegistryConfig
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

CA_DATA_PEM = "this-is-certificate-authority-public-key"


class MockOrchestrator(Orchestrator):
    def __init__(self, config: neuro_config_client.Cluster) -> None:
        self._config = config
        self._mock_status_to_return = JobStatus.PENDING
        self._mock_reason_to_return: str | None = JobStatusReason.CONTAINER_CREATING
        self._mock_exit_code_to_return: int | None = None
        self._mock_statuses: dict[str, JobStatus] = {}
        self._mock_reasons: dict[str, str | None] = {}
        self._mock_exit_codes: dict[str, int | None] = {}
        self.raise_on_get_job_status = False
        self.raise_on_start_job_status = False
        self.raise_on_preempt_jobs = False
        self.get_job_status_exc_factory = self._create_get_job_status_exc
        self.raise_on_delete = False
        self.delete_job_exc_factory = self._create_delete_job_exc
        self.current_datetime_factory: Callable[[], datetime] = partial(
            datetime.now,
            timezone.utc,  # noqa: UP017
        )
        self._deleted_job_ids: list[str] = []
        self._preemptible_job_ids: list[str] = []
        self._scheduled_job_ids: list[str] = []
        self._schedulable_job_ids: list[str] = []

    @property
    def config(self) -> OrchestratorConfig:
        return self._config.orchestrator

    async def __aenter__(self) -> None:
        pass

    async def __aexit__(
        self,
        exc_typ: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    def _get_status(self, job_id: str) -> JobStatus:
        return self._mock_statuses.get(job_id, self._mock_status_to_return)

    def _get_exit_code(self, job_id: str) -> int | None:
        return self._mock_exit_codes.get(job_id, self._mock_exit_code_to_return)

    def _get_reason(self, job_id: str) -> str | None:
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

    def get_deleted_job_ids(self) -> list[str]:
        return self._deleted_job_ids

    async def delete_job(self, job: Job) -> JobStatus:
        if self.raise_on_delete:
            raise self.delete_job_exc_factory(job)
        self._deleted_job_ids.append(job.id)
        return JobStatus.SUCCEEDED

    def update_status_to_return(self, new_status: JobStatus) -> None:
        self._mock_status_to_return = new_status

    def update_reason_to_return(self, new_reason: str | None) -> None:
        self._mock_reason_to_return = new_reason

    def update_exit_code_to_return(self, new_exit_code: int | None) -> None:
        self._mock_exit_code_to_return = new_exit_code

    def update_status_to_return_single(
        self, job_id: str, new_status: JobStatus
    ) -> None:
        self._mock_statuses[job_id] = new_status

    def update_reason_to_return_single(
        self, job_id: str, new_reason: str | None
    ) -> None:
        self._mock_reasons[job_id] = new_reason

    def update_exit_code_to_return_single(
        self, job_id: str, new_exit_code: int | None
    ) -> None:
        self._mock_exit_codes[job_id] = new_exit_code

    async def get_missing_secrets(
        self, namespace: str, user_name: str, secret_names: list[str]
    ) -> list[str]:
        return []

    async def get_missing_disks(
        self, namespace: str, org_name: str, project_name: str, disks: list[Disk]
    ) -> list[Disk]:
        return []

    def update_preemptible_jobs(self, *jobs: Job | list[Job]) -> None:
        self._preemptible_job_ids = []
        for job in jobs:
            if isinstance(job, Job):
                self._preemptible_job_ids.append(job.id)
            else:
                self._preemptible_job_ids.extend([job.id for job in job])

    async def preempt_jobs(
        self, jobs_to_schedule: list[Job], preemptible_jobs: list[Job]
    ) -> list[Job]:
        if self.raise_on_preempt_jobs:
            raise JobError("Failed to suspend jobs")
        return [job for job in preemptible_jobs if job.id in self._preemptible_job_ids]

    def update_scheduled_jobs(self, *jobs: Job | list[Job]) -> None:
        self._scheduled_job_ids = []
        for job in jobs:
            if isinstance(job, Job):
                self._scheduled_job_ids.append(job.id)
            else:
                self._scheduled_job_ids.extend([job.id for job in job])

    async def get_scheduled_jobs(self, jobs: list[Job]) -> list[Job]:
        return [job for job in jobs if job.id in self._scheduled_job_ids]

    def update_schedulable_jobs(self, *jobs: Job | list[Job]) -> None:
        self._schedulable_job_ids = []
        for job in jobs:
            if isinstance(job, Job):
                self._schedulable_job_ids.append(job.id)
            else:
                self._schedulable_job_ids.extend([job.id for job in job])

    async def get_schedulable_jobs(self, jobs: list[Job]) -> list[Job]:
        return [job for job in jobs if job.id in self._schedulable_job_ids]


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
        self._sent_notifications: list[Notification] = []

    async def notify(self, notification: Notification) -> None:
        self._sent_notifications.append(notification)

    async def init(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @property
    def sent_notifications(self) -> list[Notification]:
        return self._sent_notifications


class MockAuthClient(AuthClient):
    def __init__(self) -> None:
        self.user_to_return = AuthUser(
            name="testuser",
        )
        self._grants: list[tuple[str, Sequence[Permission]]] = []
        self._revokes: list[tuple[str, Sequence[str]]] = []

    async def get_user(self, name: str, token: str | None = None) -> AuthUser:
        return self.user_to_return

    @property
    def grants(self) -> list[tuple[str, Sequence[Permission]]]:
        return self._grants

    @property
    def revokes(self) -> list[tuple[str, Sequence[str]]]:
        return self._revokes

    async def grant_user_permissions(
        self, name: str, permissions: Sequence[Permission], token: str | None = None
    ) -> None:
        self._grants.append((name, permissions))

    async def revoke_user_permissions(
        self, name: str, resources_uris: Sequence[str], token: str | None = None
    ) -> None:
        self._revokes.append((name, resources_uris))

    async def get_user_token(
        self,
        name: str,
        new_token_uri: str | None = None,
        token: str | None = None,
    ) -> str:
        return f"token-{name}"


class MockAdminAuthClient(AdminAuthClient):
    def __init__(self) -> None:
        pass

    async def get_user_token(
        self,
        name: str,
        new_token_uri: str | None = None,
        job_id: str | None = None,
        token: str | None = None,
    ) -> str:
        return f"token-{name}"


class MockAdminClient(AdminClientDummy):
    def __init__(self) -> None:
        self.users: dict[str, User] = {}
        self.orgs: dict[str, list[Org]] = defaultdict(list)
        self.cluster_users: dict[str, list[ClusterUser]] = defaultdict(list)
        self.org_users: dict[str, list[OrgUser]] = defaultdict(list)
        self.org_clusters: dict[str, list[OrgCluster]] = defaultdict(list)
        self.spending_log: list[tuple[str, str | None, str, Decimal, str | None]] = []
        self.debts_log: list[tuple[str, Decimal, str, str | None, str | None]] = []
        self.raise_404: bool = False

    async def get_user_with_clusters(self, name: str) -> tuple[User, list[ClusterUser]]:
        if name not in self.users:
            raise ClientResponseError(None, (), status=404)  # type: ignore
        return self.users[name], self.cluster_users[name]

    async def charge_cluster_user(
        self,
        cluster_name: str,
        user_name: str,
        amount: Decimal,
        *,
        with_user_info: bool = False,
        idempotency_key: str | None = None,
        org_name: str | None = None,
    ) -> ClusterUser | ClusterUserWithInfo:
        if self.raise_404:
            raise ClientResponseError(None, (), status=404)  # type: ignore
        self.spending_log.append(
            (cluster_name, org_name, user_name, amount, idempotency_key)
        )
        return await self.get_cluster_user(
            cluster_name, user_name, org_name=org_name, with_user_info=True
        )

    async def add_debt(
        self,
        cluster_name: str,
        credits: Decimal,
        idempotency_key: str,
        org_name: str | None = None,
        username: str | None = None,
    ) -> None:
        self.debts_log.append(
            (cluster_name, credits, idempotency_key, org_name, username)
        )

    async def get_cluster_user(  # type: ignore
        self,
        cluster_name: str,
        user_name: str,
        with_user_info: bool = False,
        org_name: str | None = None,
    ) -> ClusterUser | ClusterUserWithInfo:
        for cluster_user in self.cluster_users.get(user_name, []):
            if (
                cluster_user.cluster_name == cluster_name
                and cluster_user.org_name == org_name
            ):
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

    async def get_org_cluster(
        self,
        cluster_name: str,
        org_name: str,
    ) -> OrgCluster:
        for org_cluster in self.org_clusters.get(org_name, []):
            if org_cluster.cluster_name == cluster_name:
                return org_cluster
        raise ClientResponseError(None, (), status=404)  # type: ignore

    async def get_org_user(  # type: ignore
        self, org_name: str, user_name: str, with_user_info: bool = False
    ) -> OrgUser | OrgUserWithInfo:
        for org_user in self.org_users.get(user_name, []):
            if org_user.org_name == org_name:
                if with_user_info:
                    return org_user.add_info(
                        UserInfo(
                            email=self.users[user_name].email,
                            first_name=self.users[user_name].first_name,
                            last_name=self.users[user_name].last_name,
                            created_at=self.users[user_name].created_at,
                        )
                    )
                return org_user
        raise ClientResponseError(None, (), status=404)  # type: ignore

    async def get_org(self, name: str) -> Org:
        for org in self.orgs.get(name, []):
            if org.name == name:
                return org
        raise ClientResponseError(None, (), status=404)  # type: ignore


class MockJobsPollerApi(JobsPollerApi):
    def __init__(self, jobs_service: JobsService, jobs_storage: JobsStorage):
        self._jobs_service = jobs_service
        self._jobs_storage = jobs_storage

    async def get_unfinished_jobs(self) -> list[JobRecord]:
        return await self._jobs_storage.get_unfinished_jobs()

    async def get_jobs_for_deletion(self, *, delay: timedelta) -> list[JobRecord]:
        return await self._jobs_storage.get_jobs_for_deletion(delay=delay)

    async def push_status(self, job_id: str, status: JobStatusItem) -> None:
        await self._jobs_service.set_job_status(job_id, status)

    async def set_materialized(self, job_id: str, materialized: bool) -> None:
        async with self._jobs_storage.try_update_job(job_id=job_id) as record:
            record.materialized = materialized


@pytest.fixture
def job_request_factory() -> Callable[[], JobRequest]:
    def factory(
        cpu: float = 1, memory: int = 128 * 10**6, with_gpu: bool = False
    ) -> JobRequest:
        cont_kwargs: dict[str, Any] = {"cpu": cpu, "memory": memory}
        if with_gpu:
            cont_kwargs["nvidia_gpu"] = 1

        return JobRequest.create(
            Container(image="testimage", resources=ContainerResources(**cont_kwargs))
        )

    return factory


@pytest.fixture
def cert_authority_path(tmp_path: Path) -> str:
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(CA_DATA_PEM)
    return str(ca_path)


@pytest.fixture
def mock_job_request(job_request_factory: Callable[[], JobRequest]) -> JobRequest:
    return job_request_factory()


class MockCluster(Cluster):
    def __init__(
        self, config: neuro_config_client.Cluster, orchestrator: Orchestrator
    ) -> None:
        self._config = config
        self._orchestrator = orchestrator

    async def init(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @property
    def config(self) -> neuro_config_client.Cluster:
        return self._config

    @property
    def orchestrator(self) -> Orchestrator:
        return self._orchestrator


@pytest.fixture
def registry_config() -> RegistryConfig:
    return RegistryConfig(username="compute", password="compute_token")


@pytest.fixture
def cluster_config() -> neuro_config_client.Cluster:
    orchestrator_config = OrchestratorConfig(
        job_hostname_template="{job_id}.jobs",
        job_fallback_hostname="defaults.jobs.apolo.us",
        job_schedule_timeout_s=300,
        job_schedule_scale_up_timeout_s=900,
        resource_pool_types=[ResourcePoolType(name="cpu")],
        resource_presets=[
            ResourcePreset(
                name="cpu-small",
                credits_per_hour=Decimal("10"),
                cpu=2,
                memory=2 * 10**6 * 1024,
            ),
        ],
    )
    return neuro_config_client.Cluster(
        name="test-cluster",
        created_at=datetime.now(UTC),
        orchestrator=orchestrator_config,
        energy=EnergyConfig(
            schedules=[
                EnergySchedule(
                    name="green",
                    periods=[
                        EnergySchedulePeriod(
                            weekday=1,
                            start_time=time(0, 0, tzinfo=UTC),
                            end_time=time(6, 0, tzinfo=UTC),
                        )
                    ],
                )
            ]
        ),
        storage=StorageConfig(url=URL("https://neu.ro/api/v1/storage"), volumes=()),
        registry=neuro_config_client.RegistryConfig(
            url=URL("https://registry.dev.neuromation.io")
        ),
        monitoring=MonitoringConfig(url=URL("https://neu.ro/api/v1/monitoring")),
        secrets=SecretsConfig(url=URL("https://neu.ro/api/v1/secrets")),
        metrics=MetricsConfig(url=URL("https://neu.ro/api/v1/metrics")),
        disks=DisksConfig(
            url=URL("https://neu.ro/api/v1/disk"),
            storage_limit_per_user=100 * 2**30,
        ),
        buckets=BucketsConfig(url=URL("https://neu.ro/api/v1/buckets")),
        apps=AppsConfig(
            apps_hostname_templates=["{app_name}.apps.dev.neu.ro"],
            app_proxy_url=URL("https://proxy.apps.dev.neu.ro"),
        ),
        dns=DNSConfig(name="neu.ro"),
        ingress=IngressConfig(acme_environment=ACMEEnvironment.PRODUCTION),
    )


@pytest.fixture
def mock_orchestrator(cluster_config: neuro_config_client.Cluster) -> MockOrchestrator:
    return MockOrchestrator(config=cluster_config)


@pytest.fixture
async def cluster_holder(
    cluster_config: neuro_config_client.Cluster, mock_orchestrator: MockOrchestrator
) -> AsyncIterator[ClusterHolder]:
    def _cluster_factory(config: neuro_config_client.Cluster) -> Cluster:
        return MockCluster(config, mock_orchestrator)

    async with ClusterHolder(factory=_cluster_factory) as holder:
        await holder.update(cluster_config)
        yield holder


@pytest.fixture
async def cluster_config_registry(
    cluster_config: neuro_config_client.Cluster,
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
def mock_admin_auth_client() -> MockAdminAuthClient:
    return MockAdminAuthClient()


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
    mock_auth_client: AuthClient,
    mock_admin_client: AdminClient,
    mock_admin_auth_client: AdminAuthClient,
    mock_api_base: URL,
) -> JobsService:
    return JobsService(
        cluster_config_registry=cluster_config_registry,
        jobs_storage=mock_jobs_storage,
        jobs_config=jobs_config,
        notifications_client=mock_notifications_client,
        auth_client=mock_auth_client,
        admin_auth_client=mock_admin_auth_client,
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
        scheduler=JobsScheduler(
            config=scheduler_config,
            admin_client=mock_admin_client,
            cluster_holder=cluster_holder,
        ),
        auth_client=mock_auth_client,
        api=mock_poller_api,
    )


UserFactory = Callable[
    [str, list[tuple[str, Balance, Quota] | tuple[str, str, Balance, Quota]]],
    Awaitable[AuthUser],
]


@pytest.fixture
def user_factory(
    mock_admin_client: MockAdminClient,
) -> UserFactory:
    async def _factory(
        name: str,
        clusters: list[tuple[str, Balance, Quota] | tuple[str, str, Balance, Quota]],
    ) -> AuthUser:
        mock_admin_client.users[name] = User(name=name, email=f"{name}@domain.com")
        for entry in clusters:
            org_name: str | None = None
            if len(entry) == 3:
                cluster, balance, quota = entry
            else:
                cluster, org_name, balance, quota = entry
            mock_admin_client.cluster_users[name].append(
                ClusterUser(
                    user_name=name,
                    cluster_name=cluster,
                    role=ClusterUserRoleType.USER,
                    balance=balance,
                    quota=quota,
                    org_name=org_name,
                )
            )
            if org_name:
                mock_admin_client.orgs[org_name].append(Org(name=org_name))
                mock_admin_client.org_users[name].append(
                    OrgUser(
                        org_name=org_name,
                        user_name=name,
                        role=OrgUserRoleType.USER,
                        balance=balance,
                    )
                )
        return AuthUser(name=name)

    return _factory


OrgFactory = Callable[[str, list[tuple[str, Balance, Quota]]], Awaitable[str]]


@pytest.fixture
def org_factory(
    mock_admin_client: MockAdminClient,
) -> OrgFactory:
    async def _factory(name: str, clusters: list[tuple[str, Balance, Quota]]) -> str:
        mock_admin_client.users[name] = User(name=name, email=f"{name}@domain.com")
        for cluster, balance, quota in clusters:
            mock_admin_client.orgs[name].append(
                Org(
                    name=name,
                    balance=balance,
                )
            )
            mock_admin_client.org_clusters[name].append(
                OrgCluster(
                    cluster_name=cluster,
                    org_name=name,
                    balance=balance,
                    quota=quota,
                )
            )
        return name

    return _factory


@pytest.fixture
def test_cluster() -> str:
    return "test-cluster"


@pytest.fixture
async def test_org(test_cluster: str, org_factory: OrgFactory) -> str:
    return await org_factory("test-org", [(test_cluster, Balance(), Quota())])


@pytest.fixture
async def test_user(user_factory: UserFactory, test_cluster: str) -> AuthUser:
    return await user_factory("test_user", [(test_cluster, Balance(), Quota())])


@pytest.fixture
async def test_user_with_org(
    user_factory: UserFactory, test_cluster: str, test_org: str
) -> AuthUser:
    return await user_factory(
        "test_user", [(test_cluster, test_org, Balance(), Quota())]
    )


@pytest.fixture
def test_project() -> str:
    return "proj"
