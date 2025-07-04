import asyncio
import logging
from collections.abc import Mapping
from datetime import timedelta
from pathlib import PurePath
from typing import Any

import aiohttp
from iso8601 import iso8601
from multidict import MultiDict
from neuro_logging import new_trace
from yarl import URL

from ..cluster import SingleClusterUpdater
from .job import (
    JobPriority,
    JobRecord,
    JobRestartPolicy,
    JobStatusHistory,
    JobStatusItem,
)
from .job_request import (
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
from .jobs_storage import JobStorageTransactionError
from .poller_service import JobsPollerApi, JobsPollerService

logger = logging.getLogger(__name__)


def job_response_to_job_record(payload: Mapping[str, Any]) -> JobRecord:
    def _parse_status_item(item: Mapping[str, Any]) -> JobStatusItem:
        status = JobStatus(item["status"])
        transition_time = iso8601.parse_date(item["transition_time"])
        return JobStatusItem(
            status=status,
            transition_time=transition_time,
            reason=item.get("reason"),
            description=item.get("description"),
            exit_code=item.get("exit_code"),
        )

    def _parse_container_volume(data: Mapping[str, Any]) -> ContainerVolume:
        return ContainerVolume.create(
            data["src_storage_uri"],
            dst_path=data["dst_path"],
            read_only=bool(data.get("read_only")),
        )

    def _parse_secret_volume(payload: dict[str, Any]) -> SecretContainerVolume:
        return SecretContainerVolume.create(
            uri=payload["src_secret_uri"], dst_path=PurePath(payload["dst_path"])
        )

    def _parse_disk_volume(payload: dict[str, Any]) -> DiskContainerVolume:
        return DiskContainerVolume.create(
            uri=payload["src_disk_uri"],
            dst_path=PurePath(payload["dst_path"]),
            read_only=payload["read_only"],
        )

    def _parse_resources(data: Mapping[str, Any]) -> ContainerResources:
        tpu = None
        if "tpu" in data:
            tpu = ContainerTPUResource.from_primitive(data["tpu"])

        return ContainerResources(
            cpu=data["cpu"],
            memory=data["memory"],
            nvidia_gpu=data.get("nvidia_gpu"),
            amd_gpu=data.get("amd_gpu"),
            intel_gpu=data.get("intel_gpu"),
            nvidia_gpu_model=data.get("nvidia_gpu_model") or data.get("gpu_model"),
            amd_gpu_model=data.get("amd_gpu_model"),
            intel_gpu_model=data.get("intel_gpu_model"),
            shm=data.get("shm"),
            tpu=tpu,
        )

    def _parse_container(data: Mapping[str, Any]) -> Container:
        http_server = None
        http = data.get("http", {})
        if "port" in http:
            http_server = ContainerHTTPServer(
                port=http["port"],
                health_check_path=http.get(
                    "health_check_path", ContainerHTTPServer.health_check_path
                ),
                requires_auth=http.get(
                    "requires_auth", ContainerHTTPServer.requires_auth
                ),
            )

        return Container(
            image=data["image"],
            resources=_parse_resources(data["resources"]),
            entrypoint=data.get("entrypoint"),
            command=data.get("command"),
            env=data["env"],
            volumes=[_parse_container_volume(item) for item in data["volumes"]],
            tty=data.get("tty", False),
            working_dir=data.get("working_dir"),
            secret_env={
                env_var: Secret.create(value)
                for env_var, value in data.get("secret_env", {}).items()
            },
            secret_volumes=[
                _parse_secret_volume(volume_payload)
                for volume_payload in data.get("secret_volumes", ())
            ],
            disk_volumes=[
                _parse_disk_volume(volume_payload)
                for volume_payload in data.get("disk_volumes", ())
            ],
            http_server=http_server,
        )

    project_name = payload["project_name"]
    return JobRecord(
        request=JobRequest(
            job_id=payload["id"],
            container=_parse_container(payload["container"]),
            description=payload.get("description"),
        ),
        owner=payload["owner"],
        status_history=JobStatusHistory(
            [_parse_status_item(item) for item in payload["statuses"]]
        ),
        cluster_name=payload["cluster_name"],
        org_name=payload.get("org_name"),
        project_name=project_name,
        org_project_hash=bytes.fromhex(payload["org_project_hash"]),
        namespace=payload["namespace"],
        name=payload.get("name"),
        preset_name=payload.get("preset_name"),
        tags=payload.get("tags", []),
        scheduler_enabled=payload["scheduler_enabled"],
        preemptible_node=payload["preemptible_node"],
        pass_config=payload["pass_config"],
        privileged=payload["privileged"],
        materialized=payload["materialized"],  # Missing
        max_run_time_minutes=payload.get("max_run_time_minutes"),
        schedule_timeout=payload.get("schedule_timeout"),
        restart_policy=JobRestartPolicy(payload["restart_policy"]),
        priority=JobPriority.from_name(payload["priority"]),
        energy_schedule_name=payload.get(
            "energy_schedule_name", JobRecord.energy_schedule_name
        ),
    )


class HttpJobsPollerApi(JobsPollerApi):
    _client: aiohttp.ClientSession | None = None

    def __init__(
        self,
        url: URL,
        token: str,
        cluster_name: str,
        trace_configs: list[aiohttp.TraceConfig] | None = None,
    ):
        self._base_url = url
        self._token = token
        self._cluster_name = cluster_name
        self._trace_configs = trace_configs

    async def init(self) -> None:
        if self._client:
            return
        headers: dict[str, str] = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        self._client = aiohttp.ClientSession(
            headers=headers, trace_configs=self._trace_configs
        )

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> "HttpJobsPollerApi":
        await self.init()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def get_unfinished_jobs(self) -> list[JobRecord]:
        assert self._client
        url = self._base_url / "jobs"
        params: MultiDict[Any] = MultiDict()
        for status in JobStatus.active_values():
            params.add("status", status)
        params["cluster_name"] = self._cluster_name
        async with self._client.get(url, params=params) as resp:
            payload = await resp.json()
        return [job_response_to_job_record(item) for item in payload["jobs"]]

    async def get_jobs_for_deletion(self, *, delay: timedelta) -> list[JobRecord]:
        assert self._client
        url = self._base_url / "jobs"
        params: MultiDict[Any] = MultiDict()
        for status in JobStatus.finished_values():
            params.add("status", status)
        params["materialized"] = "true"
        params["cluster_name"] = self._cluster_name
        async with self._client.get(url, params=params) as resp:
            payload = await resp.json()
        records = [job_response_to_job_record(item) for item in payload["jobs"]]
        return [record for record in records if record.should_be_deleted(delay=delay)]

    async def push_status(self, job_id: str, status: JobStatusItem) -> None:
        assert self._client
        url = self._base_url / "jobs" / job_id / "status"
        payload = {
            "status": str(status.status),
            "reason": status.reason,
            "description": status.description,
            "exit_code": status.exit_code,
        }
        async with self._client.put(url, json=payload) as resp:
            if resp.status == 400:
                raise JobStorageTransactionError(
                    f"Failed to update status of job {job_id} to {status.status}"
                )

    async def set_materialized(self, job_id: str, materialized: bool) -> None:
        assert self._client
        url = self._base_url / "jobs" / job_id / "materialized"
        payload = {"materialized": materialized}
        async with self._client.put(url, json=payload) as resp:
            if resp.status == 400:
                raise JobStorageTransactionError(
                    "Failed to update materialized field of job "
                    f"{job_id} to {materialized}"
                )


class JobsPoller:
    def __init__(
        self,
        *,
        jobs_poller_service: JobsPollerService,
        interval_s: float = 1,
        cluster_updater: SingleClusterUpdater,
    ) -> None:
        self._loop = asyncio.get_event_loop()

        self._jobs_poller_service = jobs_poller_service
        self._cluster_updater = cluster_updater
        self._interval_s = interval_s

        self._is_active: asyncio.Future[None] | None = None
        self._task: asyncio.Future[None] | None = None

    async def start(self) -> None:
        logger.info("Starting jobs polling")
        await self._init_task()

    async def __aenter__(self) -> "JobsPoller":
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    async def _init_task(self) -> None:
        assert not self._is_active
        assert not self._task

        self._is_active = self._loop.create_future()
        self._task = asyncio.ensure_future(self._run())
        # forcing execution of the newly created task
        await asyncio.sleep(0)

    async def stop(self) -> None:
        logger.info("Stopping jobs polling")
        assert self._is_active is not None
        self._is_active.set_result(None)

        assert self._task
        await self._task

        self._task = None
        self._is_active = None

    async def _run(self) -> None:
        assert self._is_active is not None
        while not self._is_active.done():
            await self._run_once()
            await self._wait()

    @new_trace
    async def _run_once(self) -> None:
        try:
            await self._cluster_updater.do_update()
        except Exception:
            logger.exception("exception when trying to update clusters")
        try:
            await self._jobs_poller_service.update_jobs_statuses()
        except Exception:
            logger.exception("exception when trying update jobs status")

    async def _wait(self) -> None:
        assert self._is_active is not None
        await asyncio.wait((self._is_active,), timeout=self._interval_s)
