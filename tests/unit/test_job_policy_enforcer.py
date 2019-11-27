import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Optional,
    Set,
)

import pytest
from aiohttp import ClientResponseError, web
from async_generator import asynccontextmanager
from notifications_client import QuotaResourceType, QuotaWillBeReachedSoon
from notifications_client.client import Client
from yarl import URL

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job import AggregatedRunTime
from platform_api.orchestrator.job_policy_enforcer import (
    ClusterJobs,
    JobInfo,
    JobPolicyEnforcePoller,
    JobPolicyEnforcer,
    Jobs,
    PlatformApiClient,
    QuotaEnforcer,
    UserClusterStats,
    UserJobs,
    UserStats,
    _parse_job_info,
    _parse_jobs_runtime,
    _parse_quota_runtime,
    _parse_user_stats,
)
from platform_api.orchestrator.job_request import JobStatus
from tests.integration.api import ApiConfig
from tests.integration.conftest import ApiRunner


_EnforcePollingRunner = Callable[
    [JobPolicyEnforcer], AsyncContextManager[JobPolicyEnforcePoller]
]


@dataclass(frozen=True)
class PlatformApiEndpoints:
    url: URL

    @property
    def endpoint(self) -> str:
        return str(self.url)

    @property
    def platform_config_url(self) -> str:
        return f"{self.endpoint}/config"

    @property
    def jobs_base_url(self) -> str:
        return f"{self.endpoint}/jobs"

    def generate_job_url(self, job_id: str) -> str:
        return f"{self.jobs_base_url}/{job_id}"


@pytest.fixture
def job_policy_enforcer_config() -> JobPolicyEnforcerConfig:
    return JobPolicyEnforcerConfig(
        platform_api_url=URL("http://localhost:8080"),
        token="admin-token",
        interval_sec=0.1,
    )


class TestSerializers:
    def test_parse_jobs_runtime_regular(self) -> None:
        runtime = _parse_jobs_runtime(
            {"total_gpu_run_time_minutes": 10, "total_non_gpu_run_time_minutes": 15}
        )
        assert runtime.total_gpu_run_time_delta == timedelta(seconds=600)
        assert runtime.total_non_gpu_run_time_delta == timedelta(seconds=900)

    def test_parse_quota_runtime_regular(self) -> None:
        runtime = _parse_quota_runtime(
            {"total_gpu_run_time_minutes": 10, "total_non_gpu_run_time_minutes": 15}
        )
        assert runtime.total_gpu_run_time_delta == timedelta(seconds=600)
        assert runtime.total_non_gpu_run_time_delta == timedelta(seconds=900)

    def test_parse_quota_runtime_empty(self) -> None:
        runtime = _parse_quota_runtime({})
        assert runtime.total_gpu_run_time_delta == timedelta.max
        assert runtime.total_non_gpu_run_time_delta == timedelta.max

    def test_parse_quota_runtime_only_gpu(self) -> None:
        runtime = _parse_quota_runtime({"total_gpu_run_time_minutes": 10})
        assert runtime.total_gpu_run_time_delta == timedelta(seconds=600)
        assert runtime.total_non_gpu_run_time_delta == timedelta.max

    def test_parse_quota_runtime_only_non_gpu(self) -> None:
        runtime = _parse_quota_runtime({"total_non_gpu_run_time_minutes": 15})
        assert runtime.total_gpu_run_time_delta == timedelta.max
        assert runtime.total_non_gpu_run_time_delta == timedelta(seconds=900)

    def test_parse_job_info_gpu(self) -> None:
        payload = {
            "id": "job1",
            "owner": "user1",
            "status": "pending",
            "container": {"resources": {"gpu": 1}},
            "cluster_name": "cluster1",
        }
        job_info = _parse_job_info(payload)
        assert job_info.id == "job1"
        assert job_info.owner == "user1"
        assert job_info.status == JobStatus.PENDING
        assert job_info.is_gpu is True
        assert job_info.cluster_name == "cluster1"

    def test_parse_job_info_non_gpu(self) -> None:
        payload = {
            "id": "job123",
            "owner": "user2",
            "status": "running",
            "container": {"resources": {"cpu": 4}},
            "cluster_name": "cluster1",
        }
        job_info = _parse_job_info(payload)
        assert job_info.id == "job123"
        assert job_info.owner == "user2"
        assert job_info.status == JobStatus.RUNNING
        assert job_info.is_gpu is False
        assert job_info.cluster_name == "cluster1"

    def test_parse_user_stats(self) -> None:
        payload = {
            "name": "user1",
            "clusters": [
                {
                    "name": "cluster1",
                    "quota": {},
                    "jobs": {
                        "total_gpu_run_time_minutes": 0,
                        "total_non_gpu_run_time_minutes": 0,
                    },
                },
                {
                    "name": "cluster2",
                    "quota": {
                        "total_gpu_run_time_minutes": 2,
                        "total_non_gpu_run_time_minutes": 3,
                    },
                    "jobs": {
                        "total_gpu_run_time_minutes": 0,
                        "total_non_gpu_run_time_minutes": 1,
                    },
                },
            ],
        }
        user_stats = _parse_user_stats(payload)
        assert user_stats == UserStats(
            name="user1",
            clusters=[
                UserClusterStats(
                    name="cluster1",
                    quota=AggregatedRunTime(
                        total_gpu_run_time_delta=timedelta.max,
                        total_non_gpu_run_time_delta=timedelta.max,
                    ),
                    jobs=AggregatedRunTime(
                        total_gpu_run_time_delta=timedelta(0),
                        total_non_gpu_run_time_delta=timedelta(0),
                    ),
                ),
                UserClusterStats(
                    name="cluster2",
                    quota=AggregatedRunTime(
                        total_gpu_run_time_delta=timedelta(minutes=2),
                        total_non_gpu_run_time_delta=timedelta(minutes=3),
                    ),
                    jobs=AggregatedRunTime(
                        total_gpu_run_time_delta=timedelta(minutes=0),
                        total_non_gpu_run_time_delta=timedelta(minutes=1),
                    ),
                ),
            ],
        )


class TestUserStats:
    def test_get_cluster_fallback(self) -> None:
        user_stats = UserStats(name="u1", clusters=[])
        cluster_stats = user_stats.get_cluster("c1")
        assert cluster_stats == UserClusterStats.create_dummy(name="c1")
        assert cluster_stats.is_non_gpu_quota_exceeded
        assert cluster_stats.is_gpu_quota_exceeded

    def test_get_cluster(self) -> None:
        user_stats = UserStats(
            name="u1",
            clusters=[
                UserClusterStats(
                    name="c1",
                    quota=AggregatedRunTime(
                        total_non_gpu_run_time_delta=timedelta(2),
                        total_gpu_run_time_delta=timedelta(3),
                    ),
                    jobs=AggregatedRunTime(
                        total_non_gpu_run_time_delta=timedelta(1),
                        total_gpu_run_time_delta=timedelta(1),
                    ),
                )
            ],
        )
        cluster_stats = user_stats.get_cluster("c1")
        assert cluster_stats.name == "c1"
        assert not cluster_stats.is_non_gpu_quota_exceeded
        assert not cluster_stats.is_gpu_quota_exceeded


class TestJobs:
    def test_group_by_user_empty(self) -> None:
        jobs: List[JobInfo] = []
        groups = Jobs.group_by_user(jobs)
        assert groups == []

    def test_group_by_user(self) -> None:
        def _Job(**kwargs: Any) -> JobInfo:
            return JobInfo(status=JobStatus.RUNNING, **kwargs)

        job1 = _Job(id="j1", owner="u1", is_gpu=False, cluster_name="c1",)
        job2 = _Job(id="j2", owner="u1", is_gpu=True, cluster_name="c1",)
        job3 = _Job(id="j3", owner="u2", is_gpu=False, cluster_name="c1",)
        job4 = _Job(id="j4", owner="u2", is_gpu=True, cluster_name="c2",)
        job5 = _Job(id="j5", owner="u2", is_gpu=False, cluster_name="c2",)
        job6 = _Job(id="j6", owner="u2", is_gpu=True, cluster_name="c1",)
        job7 = _Job(id="j7", owner="u1", is_gpu=False, cluster_name="c1",)
        jobs: List[JobInfo] = [job1, job2, job3, job4, job5, job6, job7]
        groups = Jobs.group_by_user(jobs)
        assert groups == [
            UserJobs(
                name="u1",
                clusters={
                    "c1": ClusterJobs(name="c1", non_gpu=[job1, job7], gpu=[job2])
                },
            ),
            UserJobs(
                name="u2",
                clusters={
                    "c1": ClusterJobs(name="c1", non_gpu=[job3], gpu=[job6]),
                    "c2": ClusterJobs(name="c2", non_gpu=[job5], gpu=[job4]),
                },
            ),
        ]
        assert list(groups[0].clusters["c1"].non_gpu_ids) == [job1.id, job7.id]
        assert list(groups[0].clusters["c1"].gpu_ids) == [job2.id]
        assert list(groups[1].clusters["c1"].non_gpu_ids) == [job3.id]
        assert list(groups[1].clusters["c1"].gpu_ids) == [job6.id]
        assert list(groups[1].clusters["c2"].non_gpu_ids) == [job5.id]
        assert list(groups[1].clusters["c2"].gpu_ids) == [job4.id]


class TestPlatformApiClient:
    @pytest.mark.asyncio
    async def test_get_user_stats(self, mock_api: ApiConfig) -> None:
        job_policy_enforcer_config = JobPolicyEnforcerConfig(
            URL(mock_api.endpoint), "random_token"
        )
        client = PlatformApiClient(job_policy_enforcer_config)
        expected_quota = AggregatedRunTime(
            total_gpu_run_time_delta=timedelta(minutes=60 * 100),
            total_non_gpu_run_time_delta=timedelta(minutes=60 * 100),
        )
        expected_jobs = AggregatedRunTime(
            total_gpu_run_time_delta=timedelta(minutes=0),
            total_non_gpu_run_time_delta=timedelta(minutes=0),
        )
        expected_response = UserStats(
            name="user1",
            clusters=[
                UserClusterStats(
                    name="cluster1", quota=expected_quota, jobs=expected_jobs
                )
            ],
        )

        response = await client.get_user_stats("user1")
        assert response == expected_response

    @pytest.mark.asyncio
    async def test_get_non_terminated_jobs(self, mock_api: ApiConfig) -> None:
        job_policy_enforcer_config = JobPolicyEnforcerConfig(
            URL(mock_api.endpoint), "random_token"
        )
        client = PlatformApiClient(job_policy_enforcer_config)
        response = await client.get_non_terminated_jobs()
        assert len(response) == 5


class MockPlatformApiClient(PlatformApiClient):
    def __init__(
        self,
        gpu_quota_minutes: int = 10,
        cpu_quota_minutes: int = 10,
        kill_exception: Optional[Exception] = None,
        stat_exception: Optional[Exception] = None,
    ):
        self._gpu_quota = timedelta(minutes=gpu_quota_minutes)
        self._cpu_quota = timedelta(minutes=cpu_quota_minutes)
        self._killed_jobs: Set[str] = set()
        self._kill_exception = kill_exception
        self._stat_exception = stat_exception

    async def get_non_terminated_jobs(self) -> List[JobInfo]:
        return [
            JobInfo("job1", JobStatus.RUNNING, "user1", False, cluster_name="cluster1"),
            JobInfo("job2", JobStatus.PENDING, "user1", False, cluster_name="cluster1"),
            JobInfo("job3", JobStatus.RUNNING, "user2", False, cluster_name="cluster1"),
            JobInfo("job4", JobStatus.PENDING, "user2", False, cluster_name="cluster1"),
            JobInfo("job5", JobStatus.PENDING, "user2", True, cluster_name="cluster1"),
        ]

    async def get_user_stats(self, username: str) -> UserStats:
        quota = AggregatedRunTime(
            total_gpu_run_time_delta=self._gpu_quota,
            total_non_gpu_run_time_delta=self._cpu_quota,
        )
        jobs = AggregatedRunTime(
            total_gpu_run_time_delta=timedelta(minutes=9),
            total_non_gpu_run_time_delta=timedelta(minutes=9),
        )
        return UserStats(
            name=username,
            clusters=[UserClusterStats(name="cluster1", quota=quota, jobs=jobs)],
        )

    async def kill_job(self, job_id: str) -> None:
        if self._kill_exception is not None:
            raise self._kill_exception
        self._killed_jobs.add(job_id)

    @property
    def killed_jobs(self) -> Set[str]:
        return self._killed_jobs


class TestQuotaEnforcer:
    @pytest.mark.asyncio
    async def test_enforce_ok(self) -> None:
        client = MockPlatformApiClient()
        enforcer = QuotaEnforcer(client)
        await enforcer.enforce()
        assert len(client.killed_jobs) == 0

    @pytest.mark.asyncio
    async def test_enforce_user_quota_cpu_almost_reached_notification(
        self, mock_notifications_client: Client
    ) -> None:
        cpu_jobs = {"job3", "job4"}
        gpu_jobs = {"job5"}
        client = MockPlatformApiClient()
        enforcer = QuotaEnforcer(client, mock_notifications_client)
        await enforcer._enforce_user_quota(JobsByUser("user2", cpu_jobs, gpu_jobs))
        assert len(mock_notifications_client.sent_notifications) == 1
        notification = mock_notifications_client.sent_notifications[0]
        assert isinstance(notification, QuotaWillBeReachedSoon)
        assert notification.user_id == "user2"
        assert notification.resource == QuotaResourceType.NON_GPU

        # start another enforcement and verify no new notifications were sent
        await enforcer._enforce_user_quota(JobsByUser("user2", cpu_jobs, gpu_jobs))
        assert len(mock_notifications_client.sent_notifications) == 1

    @pytest.mark.asyncio
    async def test_enforce_user_quota_gpu_almost_reached_notification(
        self, mock_notifications_client: Client
    ) -> None:
        cpu_jobs = {"job3", "job4"}
        gpu_jobs = {"job5"}
        client = MockPlatformApiClient(cpu_quota_minutes=100)
        enforcer = QuotaEnforcer(client, mock_notifications_client)
        await enforcer._enforce_user_quota(JobsByUser("user1", cpu_jobs, gpu_jobs))
        assert len(mock_notifications_client.sent_notifications) == 1
        notification = mock_notifications_client.sent_notifications[0]
        assert isinstance(notification, QuotaWillBeReachedSoon)
        assert notification.user_id == "user1"
        assert notification.resource == QuotaResourceType.GPU
        # start another enforcement and verify no new notifications were sent
        await enforcer._enforce_user_quota(JobsByUser("user1", cpu_jobs, gpu_jobs))
        assert len(mock_notifications_client.sent_notifications) == 1

    @pytest.mark.asyncio
    async def test_enforcer_error_handling_in_api(self) -> None:
        client = MockPlatformApiClient(
            cpu_quota_minutes=1, kill_exception=Exception("Test exception")
        )
        enforcer = QuotaEnforcer(client)
        # If enforcer handles ApiClient exceptions correctly, this will not fail
        # (but will not yield any results either)
        await enforcer.enforce()

    @pytest.mark.asyncio
    async def test_enforcer_error_handling_in_check_user_quota(self) -> None:
        client = MockPlatformApiClient(
            cpu_quota_minutes=1, stat_exception=Exception("Test exception")
        )
        enforcer = QuotaEnforcer(client)
        # If enforcer handles exceptions correctly, this will not fail
        # (but will not yield any results either)
        await enforcer.enforce()

    @pytest.mark.asyncio
    async def test_enforce_gpu_exceeded(
        self, mock_notifications_client: Client
    ) -> None:
        gpu_jobs = {"job5"}
        client = MockPlatformApiClient(gpu_quota_minutes=1)
        enforcer = QuotaEnforcer(client, mock_notifications_client)
        await enforcer.enforce()
        assert client.killed_jobs == gpu_jobs

    @pytest.mark.asyncio
    async def test_enforce_cpu_exceeded(
        self, mock_notifications_client: Client
    ) -> None:
        cpu_jobs = {f"job{i}" for i in range(1, 5)}
        client = MockPlatformApiClient(cpu_quota_minutes=1)
        enforcer = QuotaEnforcer(client, mock_notifications_client)
        await enforcer.enforce()
        assert client.killed_jobs == cpu_jobs

    @pytest.mark.asyncio
    async def test_sent_notification_hash(self) -> None:
        # generate 5 hashes, where 1 element is different from any other hash and
        # verify they're all different
        quota_hashes = {
            QuotaEnforcer._compute_quota_notification_hash("user1", "cluster1", "CPU",
                                                           10),
            QuotaEnforcer._compute_quota_notification_hash("user2", "cluster1", "CPU",
                                                           10),
            QuotaEnforcer._compute_quota_notification_hash("user1", "cluster2", "CPU",
                                                           10),
            QuotaEnforcer._compute_quota_notification_hash("user1", "cluster1", "GPU",
                                                           10),
            QuotaEnforcer._compute_quota_notification_hash("user1", "cluster1", "CPU",
                                                           11),
        }
        assert len(quota_hashes) == 5


class MockedJobPolicyEnforcer(JobPolicyEnforcer):
    def __init__(
        self, *, raise_exception: bool = False, enforce_time_sec: float = 0
    ) -> None:
        self._raise_exception = raise_exception
        self._enforce_time_sec = enforce_time_sec
        self._called_times: int = 0

    @property
    def called_times(self) -> int:
        return self._called_times

    @property
    def enforce_time_sec(self) -> float:
        return self._enforce_time_sec

    async def enforce(self) -> None:
        self._called_times += 1
        await asyncio.sleep(self._enforce_time_sec)
        if self._raise_exception:
            raise RuntimeError("exception in job policy enforcer")


class TestJobPolicyEnforcePoller:
    @pytest.fixture
    async def run_enforce_polling(
        self, job_policy_enforcer_config: JobPolicyEnforcerConfig
    ) -> Callable[[JobPolicyEnforcer], AsyncIterator[JobPolicyEnforcePoller]]:
        @asynccontextmanager
        async def _factory(
            enforcer: JobPolicyEnforcer,
        ) -> AsyncIterator[JobPolicyEnforcePoller]:
            async with JobPolicyEnforcePoller(
                config=job_policy_enforcer_config, enforcers=[enforcer]
            ) as poller:
                yield poller

        return _factory

    @pytest.mark.asyncio
    async def test_basic_no_exception_short_response(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=False, enforce_time_sec=interval * 0.1
        )
        async with run_enforce_polling(enforcer):
            await asyncio.sleep(interval * 1.5)
            assert enforcer.called_times == 2

    @pytest.mark.asyncio
    async def test_basic_exception_thrown_short_response(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=True, enforce_time_sec=interval * 0.1
        )
        async with run_enforce_polling(enforcer):
            await asyncio.sleep(interval * 1.5)
            assert enforcer.called_times == 2

    @pytest.mark.asyncio
    async def test_basic_no_exception_long_enforce(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=False, enforce_time_sec=interval * 2
        )
        async with run_enforce_polling(enforcer):
            await asyncio.sleep(interval * 1.5)
            assert enforcer.called_times == 1

    @pytest.mark.asyncio
    async def test_basic_exception_thrown_long_enforce(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=True, enforce_time_sec=interval * 2
        )
        async with run_enforce_polling(enforcer):
            await asyncio.sleep(interval * 1.5)
            assert enforcer.called_times == 1

    @pytest.mark.asyncio
    async def test_concurrent_call_not_allowed(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=True, enforce_time_sec=interval
        )
        async with run_enforce_polling(enforcer) as poller:
            with pytest.raises(
                RuntimeError, match="Concurrent usage of enforce poller not allowed"
            ):
                async with poller:
                    pass


@pytest.fixture
async def mock_api() -> AsyncIterator[ApiConfig]:
    async def _get_jobs(request: web.Request) -> web.Response:
        statuses = request.query.getall("status")
        assert statuses == ["pending", "running"]
        payload: Dict[str, Any] = {
            "jobs": [
                {
                    "id": "job1",
                    "status": "running",
                    "owner": "user1",
                    "container": {"resources": {"cpu": 1.0}},
                    "cluster_name": "cluster1",
                },
                {
                    "id": "job2",
                    "status": "pending",
                    "owner": "user1",
                    "container": {"resources": {"cpu": 1.0}},
                    "cluster_name": "cluster1",
                },
                {
                    "id": "job3",
                    "status": "running",
                    "owner": "user2",
                    "container": {"resources": {"cpu": 1.0}},
                    "cluster_name": "cluster1",
                },
                {
                    "id": "job4",
                    "status": "pending",
                    "owner": "user2",
                    "container": {"resources": {"cpu": 1.0}},
                    "cluster_name": "cluster1",
                },
                {
                    "id": "job5",
                    "status": "pending",
                    "owner": "user2",
                    "container": {"resources": {"cpu": 1.0, "gpu": 0.5}},
                    "cluster_name": "cluster1",
                },
            ]
        }

        return web.json_response(payload)

    async def _kill_job(request: web.Request) -> web.Response:
        job_id = request.match_info.get("job_id")
        assert job_id is not None
        return web.Response()

    async def _user_stats(request: web.Request) -> web.Response:
        username = request.match_info["username"]
        payload: Dict[str, Any] = {
            "name": username,
            "clusters": [
                {
                    "name": "cluster1",
                    "jobs": {
                        "total_gpu_run_time_minutes": 0,
                        "total_non_gpu_run_time_minutes": 0,
                    },
                    "quota": {
                        "total_gpu_run_time_minutes": 60 * 100,
                        "total_non_gpu_run_time_minutes": 60 * 100,
                    },
                }
            ],
        }
        return web.json_response(payload)

    def _create_app() -> web.Application:
        app = web.Application()
        app.add_routes(
            [
                web.get("/api/v1/jobs", _get_jobs),
                web.delete("/api/v1/jobs/{job_id}", _kill_job),
                web.get("/api/v1/stats/users/{username}", _user_stats),
            ]
        )
        return app

    app = _create_app()
    runner = ApiRunner(app, port=8080)
    api_address = await runner.run()
    api_config = ApiConfig(host=api_address.host, port=api_address.port, runner=runner)
    yield api_config
    await runner.close()


class TestRealJobPolicyEnforcerClientWrapper:
    @pytest.fixture
    async def job_policy_enforcer_config(
        self, mock_api: ApiConfig
    ) -> JobPolicyEnforcerConfig:
        return JobPolicyEnforcerConfig(URL(mock_api.endpoint), "random_token")

    @pytest.fixture
    async def client(
        self, job_policy_enforcer_config: JobPolicyEnforcerConfig
    ) -> AsyncIterator[PlatformApiClient]:
        async with PlatformApiClient(job_policy_enforcer_config) as client:
            yield client

    @pytest.mark.asyncio
    async def test_get_non_terminated_jobs(self, client: PlatformApiClient) -> None:
        response = await client.get_non_terminated_jobs()
        assert len(response) == 5

    @pytest.mark.asyncio
    async def test_client_platform_unavailable(self, mock_api: ApiConfig) -> None:
        wrong_config = JobPolicyEnforcerConfig(
            URL(f"{mock_api.endpoint}/wrong/base/path"), "token"
        )
        async with PlatformApiClient(wrong_config) as client:
            with pytest.raises(ClientResponseError, match="404, message='Not Found'"):
                await client.get_non_terminated_jobs()

    @pytest.mark.asyncio
    async def test_enforcer_platform_unavailable(
        self, mock_api: ApiConfig, mock_notifications_client: Client
    ) -> None:
        wrong_config = JobPolicyEnforcerConfig(
            URL(f"{mock_api.endpoint}/wrong/base/path"), "token"
        )
        async with PlatformApiClient(wrong_config) as client:
            enforcer = QuotaEnforcer(client, mock_notifications_client)
            with pytest.raises(ClientResponseError, match="404, message='Not Found"):
                await enforcer.enforce()

    @pytest.mark.asyncio
    async def test_client_multiple_usages_unallowed(self, mock_api: ApiConfig) -> None:
        config = JobPolicyEnforcerConfig(URL(mock_api.endpoint), "token")
        client = PlatformApiClient(config)

        async with client:
            await client.get_non_terminated_jobs()

        async with client:
            with pytest.raises(RuntimeError, match="Session is closed"):
                await client.get_non_terminated_jobs()
